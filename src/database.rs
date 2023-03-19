use std::{
    cell::{Cell, RefCell},
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Buf, Bytes, BytesMut};
use crc::{Crc, CRC_32_CKSUM};
use dashmap::{mapref::one::RefMut, DashMap};

use crate::{
    error::{BitcaskError, BitcaskResult},
    file_id::FileIdGenerator,
    file_manager::{self, create_file, open_stable_database_files, FileType},
};
use log::{error, info};

const CRC_SIZE: usize = 4;
const TSTAMP_SIZE: usize = 8;
const KEY_SIZE_SIZE: usize = 8;
const VALUE_SIZE_SIZE: usize = 8;
const ROW_OFFSET_SIZE: usize = 8;
const DATA_FILE_TSTAMP_OFFSET: usize = CRC_SIZE;
const DATA_FILE_KEY_SIZE_OFFSET: usize = CRC_SIZE + TSTAMP_SIZE;
const DATA_FILE_VALUE_SIZE_OFFSET: usize = DATA_FILE_KEY_SIZE_OFFSET + KEY_SIZE_SIZE;
const DATA_FILE_KEY_OFFSET: usize = CRC_SIZE + TSTAMP_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE;

#[derive(Debug)]
struct RowToWrite<'a> {
    crc: u32,
    tstamp: u64,
    key_size: u64,
    value_size: u64,
    key: &'a Vec<u8>,
    value: &'a [u8],
    size: usize,
}

impl<'a> RowToWrite<'a> {
    fn new(key: &'a Vec<u8>, value: &'a [u8]) -> RowToWrite<'a> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        RowToWrite::new_with_timestamp(key, value, now)
    }

    fn new_with_timestamp(key: &'a Vec<u8>, value: &'a [u8], timestamp: u64) -> RowToWrite<'a> {
        let key_size = key.len() as u64;
        let value_size = value.len() as u64;
        let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut ck = crc32.digest();
        ck.update(&timestamp.to_be_bytes());
        ck.update(&key_size.to_be_bytes());
        ck.update(&value_size.to_be_bytes());
        ck.update(&key);
        ck.update(value);
        RowToWrite {
            crc: ck.finalize(),
            tstamp: timestamp,
            key_size,
            value_size,
            key,
            value,
            size: DATA_FILE_KEY_OFFSET + key_size as usize + value_size as usize,
        }
    }

    fn to_bytes(&self) -> Bytes {
        let mut bs = BytesMut::with_capacity(self.size);
        bs.extend_from_slice(&self.crc.to_be_bytes());
        bs.extend_from_slice(&self.tstamp.to_be_bytes());
        bs.extend_from_slice(&self.key_size.to_be_bytes());
        bs.extend_from_slice(&self.value_size.to_be_bytes());
        bs.extend_from_slice(self.key);
        bs.extend_from_slice(self.value);
        bs.freeze()
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct RowPosition {
    pub file_id: u32,
    pub row_offset: u64,
    pub row_size: usize,
    pub tstmp: u64,
}

fn read_value_from_file(
    file_id: u32,
    data_file: &mut File,
    value_offset: u64,
    size: usize,
) -> BitcaskResult<Vec<u8>> {
    data_file.seek(SeekFrom::Start(value_offset))?;
    let mut buf = vec![0; size];
    data_file.read_exact(&mut buf)?;

    let bs = Bytes::from(buf);
    let expected_crc = bs.slice(0..4).get_u32();

    let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
    let mut ck = crc32.digest();
    ck.update(&bs.slice(4..));
    let actual_crc = ck.finalize();
    if expected_crc != actual_crc {
        return Err(BitcaskError::CrcCheckFailed(
            file_id,
            value_offset,
            expected_crc,
            actual_crc,
        ));
    }

    let key_size = bs
        .slice(DATA_FILE_KEY_SIZE_OFFSET..(DATA_FILE_KEY_SIZE_OFFSET + KEY_SIZE_SIZE))
        .get_u64() as usize;
    let val_size = bs
        .slice(DATA_FILE_VALUE_SIZE_OFFSET..(DATA_FILE_VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE))
        .get_u64() as usize;
    let val_offset = DATA_FILE_KEY_OFFSET + key_size;
    let ret = bs.slice(val_offset..val_offset + val_size).into();
    Ok(ret)
}

trait BitcaskDataFile {
    fn read_value(&mut self, value_offset: u64, size: usize) -> BitcaskResult<Vec<u8>>;
}

#[derive(Debug)]
struct WritingFile {
    file_id: u32,
    data_file: File,
    file_size: usize,
}

impl WritingFile {
    fn new(database_dir: &Path, file_id: u32) -> BitcaskResult<WritingFile> {
        let data_file = create_file(&database_dir, file_id, FileType::DataFile)?;
        Ok(WritingFile {
            file_id,
            data_file,
            file_size: 0,
        })
    }

    fn write_row(&mut self, row: RowToWrite) -> BitcaskResult<RowPosition> {
        let value_offset = self.data_file.seek(SeekFrom::End(0))?;
        let data_to_write = row.to_bytes();
        self.data_file.write_all(&*data_to_write)?;
        self.file_size += data_to_write.len();
        Ok(RowPosition {
            file_id: self.file_id,
            row_offset: value_offset,
            row_size: row.size,
            tstmp: row.tstamp,
        })
    }

    fn read_value(&mut self, value_offset: u64, size: usize) -> BitcaskResult<Vec<u8>> {
        read_value_from_file(self.file_id, &mut self.data_file, value_offset, size)
    }

    fn transit_to_readonly(mut self) -> BitcaskResult<(u32, File)> {
        self.data_file.flush()?;
        let file_id = self.file_id;
        let mut perms = self.data_file.metadata()?.permissions();
        perms.set_readonly(true);
        self.data_file.set_permissions(perms)?;
        Ok((file_id, self.data_file))
    }

    fn flush(&mut self) -> BitcaskResult<()> {
        Ok(self.data_file.flush()?)
    }
}

pub struct RowToRead {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub row_position: RowPosition,
}

#[derive(Debug)]
struct StableFile {
    database_dir: PathBuf,
    file_id: u32,
    file: File,
}

impl StableFile {
    fn new(database_dir: &PathBuf, file_id: u32, file: File) -> StableFile {
        StableFile {
            database_dir: database_dir.clone(),
            file_id,
            file,
        }
    }

    fn read_value(&mut self, value_offset: u64, size: usize) -> BitcaskResult<Vec<u8>> {
        read_value_from_file(self.file_id, &mut self.file, value_offset, size)
    }

    fn read_next_row(&mut self) -> BitcaskResult<RowToRead> {
        let value_offset = self.file.seek(SeekFrom::Current(0))?;
        let mut header_buf = vec![0; DATA_FILE_KEY_OFFSET];
        self.file.read_exact(&mut header_buf)?;

        let header_bs = Bytes::from(header_buf);
        let expected_crc = header_bs.slice(0..DATA_FILE_TSTAMP_OFFSET).get_u32();

        self.file.metadata().unwrap();

        let tstmp = header_bs
            .slice(DATA_FILE_TSTAMP_OFFSET..DATA_FILE_KEY_SIZE_OFFSET)
            .get_u64();
        let key_size = header_bs
            .slice(DATA_FILE_KEY_SIZE_OFFSET..(DATA_FILE_KEY_SIZE_OFFSET + KEY_SIZE_SIZE))
            .get_u64() as usize;
        let value_size = header_bs
            .slice(DATA_FILE_VALUE_SIZE_OFFSET..(DATA_FILE_VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE))
            .get_u64() as usize;

        let mut kv_buf = vec![0; key_size + value_size];
        self.file.read_exact(&mut kv_buf)?;
        let kv_bs = Bytes::from(kv_buf);
        let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut ck = crc32.digest();
        ck.update(&header_bs[DATA_FILE_TSTAMP_OFFSET..]);
        ck.update(&kv_bs);
        let actual_crc = ck.finalize();
        if expected_crc != actual_crc {
            return Err(BitcaskError::CrcCheckFailed(
                self.file_id,
                value_offset,
                expected_crc,
                actual_crc,
            ));
        }

        Ok(RowToRead {
            key: kv_bs.slice(0..key_size).into(),
            value: kv_bs.slice(key_size..).into(),
            row_position: RowPosition {
                file_id: self.file_id,
                row_offset: value_offset,
                row_size: DATA_FILE_KEY_OFFSET + key_size + value_size,
                tstmp,
            },
        })
    }

    fn iter(&self) -> BitcaskResult<StableFileIter> {
        let file = file_manager::open_file(&self.database_dir, self.file_id, FileType::DataFile)?;
        Ok(StableFileIter {
            stable_file: StableFile::new(&self.database_dir, self.file_id, file.file),
        })
    }
}

#[derive(Debug)]
pub struct StableFileIter {
    stable_file: StableFile,
}

impl Iterator for StableFileIter {
    type Item = BitcaskResult<RowToRead>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.stable_file.read_next_row() {
            Err(BitcaskError::IoError(e)) => match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    return None;
                }
                _ => return Some(Err(BitcaskError::IoError(e))),
            },
            r => return Some(r),
        }
    }
}

pub struct RowHint {
    pub timestamp: u64,
    pub key_size: usize,
    pub value_size: usize,
    pub row_offset: u64,
    pub key: Vec<u8>,
}

const HINT_FILE_KEY_SIZE_OFFSET: usize = TSTAMP_SIZE;
const HINT_FILE_VALUE_SIZE_OFFSET: usize = HINT_FILE_KEY_SIZE_OFFSET + KEY_SIZE_SIZE;
const HINT_FILE_ROW_OFFSET_OFFSET: usize = HINT_FILE_VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE;
const HINT_FILE_KEY_OFFSET: usize = HINT_FILE_ROW_OFFSET_OFFSET + ROW_OFFSET_SIZE;
const HINT_FILE_HEADER_SIZE: usize =
    TSTAMP_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE + ROW_OFFSET_SIZE;

impl RowHint {
    fn to_bytes(&self) -> Bytes {
        let mut bs = BytesMut::with_capacity(HINT_FILE_HEADER_SIZE + self.key.len());
        bs.extend_from_slice(&self.timestamp.to_be_bytes());
        bs.extend_from_slice(&self.key_size.to_be_bytes());
        bs.extend_from_slice(&self.value_size.to_be_bytes());
        bs.extend_from_slice(&self.row_offset.to_be_bytes());
        bs.extend_from_slice(&self.key);
        bs.freeze()
    }
}

struct HintFile {
    database_dir: PathBuf,
    file_id: u32,
    file: File,
}

impl HintFile {
    fn new(database_dir: &PathBuf, file_id: u32, file: File) -> HintFile {
        HintFile {
            database_dir: database_dir.clone(),
            file_id,
            file,
        }
    }

    fn write_file(
        &mut self,
        iter: Box<dyn Iterator<Item = BitcaskResult<RowHint>>>,
    ) -> BitcaskResult<()> {
        let hints: BitcaskResult<Vec<RowHint>> = iter.collect();
        for hint in hints? {
            let data_to_write = hint.to_bytes();
            self.file.write_all(&*data_to_write)?;
        }
        Ok(())
    }

    fn iter(&self) -> BitcaskResult<HintFileIterator> {
        let file = file_manager::open_file(&self.database_dir, self.file_id, FileType::HintFile)?;
        Ok(HintFileIterator {
            file: HintFile::new(&self.database_dir, self.file_id, file.file),
        })
    }

    fn read_next_hint(&mut self) -> BitcaskResult<RowHint> {
        let mut header_buf = vec![0; HINT_FILE_HEADER_SIZE];
        self.file.read_exact(&mut header_buf)?;

        let header_bs = Bytes::from(header_buf);
        let timestamp = header_bs.slice(0..HINT_FILE_KEY_SIZE_OFFSET).get_u64();
        let key_size = header_bs
            .slice(HINT_FILE_KEY_SIZE_OFFSET..HINT_FILE_VALUE_SIZE_OFFSET)
            .get_u64() as usize;
        let value_size = header_bs.slice(HINT_FILE_VALUE_SIZE_OFFSET..24).get_u64() as usize;
        let row_offset = header_bs
            .slice(HINT_FILE_ROW_OFFSET_OFFSET..HINT_FILE_KEY_OFFSET)
            .get_u64();

        let mut k_buf = vec![0; key_size];
        self.file.read_exact(&mut k_buf)?;
        let kv_bs = Bytes::from(k_buf);

        Ok(RowHint {
            timestamp,
            key_size,
            value_size,
            row_offset,
            key: kv_bs.into(),
        })
    }
}

struct HintFileIterator {
    file: HintFile,
}

impl Iterator for HintFileIterator {
    type Item = BitcaskResult<RowHint>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.file.read_next_hint() {
            Err(BitcaskError::IoError(e)) => match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    return None;
                }
                _ => return Some(Err(BitcaskError::IoError(e))),
            },
            r => return Some(r),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DataBaseOptions {
    pub max_file_size: Option<usize>,
}

#[derive(Debug)]
pub struct Database {
    database_dir: PathBuf,
    file_id_generator: Arc<FileIdGenerator>,
    writing_file: Mutex<RefCell<WritingFile>>,
    stable_files: DashMap<u32, Mutex<StableFile>>,
    options: DataBaseOptions,
}

const DEFAULT_MAX_DATABASE_FILE_SIZE: usize = 100 * 1024;
const DATABASE_FILE_DIRECTORY: &str = "database";

impl Database {
    pub fn open(
        directory: &Path,
        file_id_generator: Arc<FileIdGenerator>,
        options: DataBaseOptions,
    ) -> BitcaskResult<Database> {
        info!(target: "database", "open db at {:?}", directory);
        let database_dir = directory.join(DATABASE_FILE_DIRECTORY);
        std::fs::create_dir_all(database_dir.clone())?;
        let opened_stable_files = open_stable_database_files(&database_dir)?;
        if !opened_stable_files.is_empty() {
            let writing_file_id = opened_stable_files.keys().max().unwrap_or(&0) + 1;
            file_id_generator.update_file_id(writing_file_id);
        }
        let writing_file = Mutex::new(RefCell::new(WritingFile::new(
            &database_dir,
            file_id_generator.generate_next_file_id(),
        )?));
        let stable_files = opened_stable_files
            .into_iter()
            .map(|(k, v)| (k, Mutex::new(StableFile::new(&database_dir, k, v))))
            .collect::<DashMap<u32, Mutex<StableFile>>>();

        Ok(Database {
            writing_file,
            file_id_generator,
            database_dir,
            stable_files,
            options,
        })
    }

    pub fn get_database_dir(&self) -> &Path {
        &self.database_dir
    }

    pub fn get_max_file_id(&self) -> u32 {
        let writing_file_ref = self.writing_file.lock().unwrap();
        let writing_file = writing_file_ref.borrow();
        writing_file.file_id
    }

    pub fn write(&self, key: &Vec<u8>, value: &[u8]) -> BitcaskResult<RowPosition> {
        let row = RowToWrite::new(&key, value);
        self.do_write(row)
    }

    pub fn write_with_timestamp(
        &self,
        key: &Vec<u8>,
        value: &[u8],
        timestamp: u64,
    ) -> BitcaskResult<RowPosition> {
        let row = RowToWrite::new_with_timestamp(&key, value, timestamp);
        self.do_write(row)
    }

    pub fn flush_writing_file(&self) -> BitcaskResult<()> {
        let writing_file_ref = self.writing_file.lock().unwrap();
        self.do_flush_writing_file(&writing_file_ref)?;
        Ok(())
    }

    pub fn iter(&self) -> BitcaskResult<DatabaseIter> {
        let mut opened_stable_files: Vec<StableFile> =
            open_stable_database_files(&self.database_dir)?
                .into_iter()
                .map(|(file_id, file)| StableFile::new(&self.database_dir, file_id, file))
                .collect();
        opened_stable_files.sort_by_key(|e| e.file_id);
        let iters: BitcaskResult<Vec<StableFileIter>> =
            opened_stable_files.iter().rev().map(|f| f.iter()).collect();
        Ok(DatabaseIter::new(iters?))
    }

    pub fn read_value(&self, row_position: &RowPosition) -> BitcaskResult<Vec<u8>> {
        {
            let writing_file_ref = self.writing_file.lock().unwrap();
            let mut writing_file = writing_file_ref.borrow_mut();
            if row_position.file_id == writing_file.file_id {
                return writing_file.read_value(row_position.row_offset, row_position.row_size);
            }
        }

        let l = self.get_file_to_read(row_position.file_id)?;
        let mut f = l.lock().unwrap();
        f.read_value(row_position.row_offset, row_position.row_size)
    }

    pub fn load_files(&self, file_ids: Vec<u32>) -> BitcaskResult<()> {
        self.flush_writing_file()?;

        for file_id in file_ids {
            let data_file =
                file_manager::open_file(&self.database_dir, file_id, FileType::DataFile)?;
            self.stable_files.insert(
                file_id,
                Mutex::new(StableFile::new(&self.database_dir, file_id, data_file.file)),
            );
        }

        Ok(())
    }

    pub fn get_file_ids(&self) -> Vec<u32> {
        let writing_file_ref = self.writing_file.lock().unwrap();
        let writing_file_id = writing_file_ref.borrow().file_id;
        let mut ids: Vec<u32> = self
            .stable_files
            .iter()
            .map(|f| f.value().lock().unwrap().file_id)
            .collect();
        ids.push(writing_file_id);
        ids
    }

    pub fn purge_outdated_files(&self, max_file_id: u32) -> BitcaskResult<()> {
        self.stable_files.retain(|_, v| {
            let f = v.lock().unwrap();
            f.file_id >= max_file_id
        });
        Ok(())
    }

    pub fn write_hint_file(&self, file_id: u32) -> BitcaskResult<()> {
        let row_hint_file =
            file_manager::create_file(&self.database_dir, file_id, FileType::HintFile)?;
        let mut hint_file = HintFile::new(&self.database_dir, file_id, row_hint_file);

        let data_file = file_manager::open_file(&self.database_dir, file_id, FileType::DataFile)?;
        let stable_file_iter =
            StableFile::new(&self.database_dir, file_id, data_file.file).iter()?;

        let boxed_iter = Box::new(stable_file_iter.map(|ret| {
            ret.and_then(|row| {
                Ok(RowHint {
                    timestamp: row.row_position.tstmp,
                    key_size: row.key.len(),
                    value_size: row.value.len(),
                    row_offset: row.row_position.row_offset,
                    key: row.key,
                })
            })
        }));
        hint_file.write_file(boxed_iter)
    }

    pub fn close(&self) -> BitcaskResult<()> {
        self.flush_writing_file()?;
        Ok(())
    }

    fn do_write(&self, row: RowToWrite) -> BitcaskResult<RowPosition> {
        let writing_file_ref = self.writing_file.lock().unwrap();
        if self.check_file_overflow(&writing_file_ref, &row) {
            self.do_flush_writing_file(&writing_file_ref)?;
        }
        let mut writing_file = writing_file_ref.borrow_mut();
        writing_file.write_row(row)
    }

    fn check_file_overflow(
        &self,
        writing_file_ref: &RefCell<WritingFile>,
        row: &RowToWrite,
    ) -> bool {
        let writing_file = writing_file_ref.borrow();
        row.size + writing_file.file_size
            > self
                .options
                .max_file_size
                .unwrap_or(DEFAULT_MAX_DATABASE_FILE_SIZE)
    }

    fn do_flush_writing_file(&self, writing_file_ref: &RefCell<WritingFile>) -> BitcaskResult<()> {
        let next_file_id = self.file_id_generator.generate_next_file_id();
        let next_writing_file = WritingFile::new(&self.database_dir, next_file_id)?;
        let mut old_file = writing_file_ref.replace(next_writing_file);
        old_file.flush()?;
        let (file_id, file) = old_file.transit_to_readonly()?;
        self.stable_files.insert(
            file_id,
            Mutex::new(StableFile::new(&self.database_dir, file_id, file)),
        );
        Ok(())
    }

    fn get_file_to_read(&self, file_id: u32) -> BitcaskResult<RefMut<u32, Mutex<StableFile>>> {
        self.stable_files
            .get_mut(&file_id)
            .ok_or(BitcaskError::TargetFileIdNotFound(file_id))
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let ret = self.close();
        if ret.is_err() {
            error!(target: "Database", "Close database failed: {}", ret.err().unwrap())
        }
    }
}

pub struct DatabaseIter {
    current_iter: Cell<Option<StableFileIter>>,
    remain_iters: Vec<StableFileIter>,
}

impl DatabaseIter {
    fn new(mut iters: Vec<StableFileIter>) -> DatabaseIter {
        if iters.is_empty() {
            return DatabaseIter {
                remain_iters: iters,
                current_iter: Cell::new(None),
            };
        } else {
            let current_iter = iters.pop();
            return DatabaseIter {
                remain_iters: iters,
                current_iter: Cell::new(current_iter),
            };
        }
    }
}

impl Iterator for DatabaseIter {
    type Item = BitcaskResult<RowToRead>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.current_iter.get_mut() {
                None => break,
                Some(iter) => match iter.next() {
                    None => {
                        self.current_iter.replace(self.remain_iters.pop());
                    }
                    other => return other,
                },
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;
    const DEFAULT_OPTIONS: DataBaseOptions = DataBaseOptions {
        max_file_size: Some(1024),
    };

    #[test]
    fn test_read_write_writing_file() {
        let dir = tempfile::tempdir().unwrap();
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let db = Database::open(&dir.path(), file_id_generator, DEFAULT_OPTIONS).unwrap();
        let kvs = [
            ("k1", "value1奥森"),
            ("k2", "value2"),
            ("k3", "value3"),
            ("k1", "value4"),
        ];
        let offset_values = kvs
            .into_iter()
            .map(|(k, v)| (db.write(&k.into(), v.as_bytes()).unwrap(), v))
            .collect::<Vec<(RowPosition, &str)>>();

        offset_values.iter().for_each(|(ret, value)| {
            assert_eq!(db.read_value(&ret).unwrap(), *value.as_bytes());
        });
        assert_eq!(
            offset_values
                .iter()
                .map(|v| v.0)
                .collect::<Vec<RowPosition>>(),
            db.iter()
                .unwrap()
                .map(|r| r.unwrap().row_position)
                .collect::<Vec<RowPosition>>()
        );
        assert_eq!(
            kvs.iter()
                .map(|kv| kv.0.to_string())
                .map(|k| k.as_bytes().clone().to_vec())
                .collect::<Vec<Vec<u8>>>(),
            db.iter()
                .unwrap()
                .map(|r| r.unwrap().key)
                .collect::<Vec<Vec<u8>>>()
        )
    }

    #[test]
    fn test_read_write_with_stable_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut offset_values: Vec<(RowPosition, &str)> = vec![];
        {
            let file_id_generator = Arc::new(FileIdGenerator::new());
            let db = Database::open(&dir.path(), file_id_generator, DEFAULT_OPTIONS).unwrap();
            let kvs = [("k1", "value1"), ("k2", "value2")];
            offset_values.append(
                &mut kvs
                    .into_iter()
                    .map(|(k, v)| (db.write(&k.into(), v.as_bytes()).unwrap(), v))
                    .collect::<Vec<(RowPosition, &str)>>(),
            );
        }
        {
            let file_id_generator = Arc::new(FileIdGenerator::new());
            let db = Database::open(&dir.path(), file_id_generator, DEFAULT_OPTIONS).unwrap();
            let kvs = [("k3", "hello world"), ("k1", "value4")];
            offset_values.append(
                &mut kvs
                    .into_iter()
                    .map(|(k, v)| (db.write(&k.into(), v.as_bytes()).unwrap(), v))
                    .collect::<Vec<(RowPosition, &str)>>(),
            );
        }

        let file_id_generator = Arc::new(FileIdGenerator::new());
        let db = Database::open(&dir.path(), file_id_generator, DEFAULT_OPTIONS).unwrap();
        offset_values.iter().for_each(|(ret, value)| {
            assert_eq!(db.read_value(&ret).unwrap(), *value.as_bytes());
        });
        assert_eq!(
            offset_values
                .iter()
                .map(|v| v.0)
                .collect::<Vec<RowPosition>>(),
            db.iter()
                .unwrap()
                .map(|r| r.unwrap().row_position)
                .collect::<Vec<RowPosition>>()
        );
        assert_eq!(
            vec!["k1", "k2", "k3", "k1"]
                .iter()
                .map(|kv| kv.to_string())
                .map(|k| k.as_bytes().clone().to_vec())
                .collect::<Vec<Vec<u8>>>(),
            db.iter()
                .unwrap()
                .map(|r| r.unwrap().key)
                .collect::<Vec<Vec<u8>>>()
        )
    }
    #[test]
    fn test_wrap_file() {
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(
            &dir.path(),
            file_id_generator,
            DataBaseOptions {
                max_file_size: Some(100),
            },
        )
        .unwrap();
        let kvs = [
            ("k1", "value1_value1_value1"),
            ("k2", "value2_value2_value2"),
            ("k3", "value3_value3_value3"),
            ("k1", "value4_value4_value4"),
        ];
        assert_eq!(0, db.stable_files.len());
        let offset_values = kvs
            .into_iter()
            .map(|(k, v)| (db.write(&k.into(), v.as_bytes()).unwrap(), v))
            .collect::<Vec<(RowPosition, &str)>>();

        offset_values.iter().for_each(|(ret, value)| {
            assert_eq!(db.read_value(&ret).unwrap(), *value.as_bytes());
        });
        assert_eq!(1, db.stable_files.len());
        assert_eq!(
            offset_values
                .iter()
                .map(|v| v.0)
                .collect::<Vec<RowPosition>>(),
            db.iter()
                .unwrap()
                .map(|r| r.unwrap().row_position)
                .collect::<Vec<RowPosition>>()
        );
        assert_eq!(
            vec!["k1", "k2", "k3", "k1"]
                .iter()
                .map(|kv| kv.to_string())
                .map(|k| k.as_bytes().clone().to_vec())
                .collect::<Vec<Vec<u8>>>(),
            db.iter()
                .unwrap()
                .map(|r| r.unwrap().key)
                .collect::<Vec<Vec<u8>>>()
        )
    }
    #[test]
    fn test_hint_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut offset_values: Vec<(RowPosition, &str)> = vec![];
        {
            let file_id_generator = Arc::new(FileIdGenerator::new());
            let db = Database::open(&dir.path(), file_id_generator, DEFAULT_OPTIONS).unwrap();
            let kvs = [("k1", "value1"), ("k2", "value2")];
            offset_values.append(
                &mut kvs
                    .into_iter()
                    .map(|(k, v)| (db.write(&k.into(), v.as_bytes()).unwrap(), v))
                    .collect::<Vec<(RowPosition, &str)>>(),
            );
        }
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let db = Database::open(&dir.path(), file_id_generator, DEFAULT_OPTIONS).unwrap();
        db.write_hint_file(1);
    }
}
