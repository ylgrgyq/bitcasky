use std::{
    cell::RefCell,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::{Buf, Bytes, BytesMut};
use crc::{Crc, CRC_32_CKSUM};
use dashmap::{mapref::one::RefMut, DashMap};

use crate::{
    error::{BitcaskError, BitcaskResult},
    file_manager::{create_database_file, open_stable_database_files},
};

const CRC_SIZE: usize = 4;
const TSTAMP_SIZE: usize = 8;
const KEY_SIZE_SIZE: usize = 8;
const VALUE_SIZE_SIZE: usize = 8;
const TSTAMP_OFFSET: usize = CRC_SIZE;
const KEY_SIZE_OFFSET: usize = CRC_SIZE + TSTAMP_SIZE;
const VALUE_SIZE_OFFSET: usize = KEY_SIZE_OFFSET + KEY_SIZE_SIZE;
const KEY_OFFSET: usize = CRC_SIZE + TSTAMP_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE;

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
        let key_size = key.len() as u64;
        let value_size = value.len() as u64;
        let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
        let mut ck = crc32.digest();
        ck.update(&now.to_be_bytes());
        ck.update(&key_size.to_be_bytes());
        ck.update(&value_size.to_be_bytes());
        ck.update(&key);
        ck.update(value);
        RowToWrite {
            crc: ck.finalize(),
            tstamp: now,
            key_size,
            value_size,
            key,
            value,
            size: KEY_OFFSET + key_size as usize + value_size as usize,
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

#[derive(Debug)]
struct WritingFile {
    file_id: u32,
    data_file: File,
    file_size: usize,
}

impl WritingFile {
    fn new(database_dir: &Path, file_id: u32) -> BitcaskResult<WritingFile> {
        let data_file = create_database_file(&database_dir, file_id)?;
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

#[derive(Debug, Clone, Copy)]
pub struct DataBaseOptions {
    pub max_file_size: Option<usize>,
}

#[derive(Debug)]
pub struct Database {
    database_dir: PathBuf,
    writing_file: Mutex<RefCell<WritingFile>>,
    stable_files: DashMap<u32, Mutex<File>>,
    options: DataBaseOptions,
}

const DEFAULT_MAX_DATABASE_FILE_SIZE: usize = 100 * 1024;
const DATABASE_FILE_DIRECTORY: &str = "database";

impl Database {
    pub fn open(directory: &Path, options: DataBaseOptions) -> BitcaskResult<Database> {
        let database_dir = directory.join(DATABASE_FILE_DIRECTORY);
        std::fs::create_dir_all(database_dir.clone())?;
        let opened_stable_files = open_stable_database_files(&database_dir)?;
        let writing_file_id = opened_stable_files.keys().max().unwrap_or(&0) + 1;
        let writing_file = Mutex::new(RefCell::new(WritingFile::new(
            &database_dir,
            writing_file_id,
        )?));
        let stable_files = opened_stable_files
            .into_iter()
            .map(|(k, v)| (k, Mutex::new(v)))
            .collect::<DashMap<u32, Mutex<File>>>();
        Ok(Database {
            writing_file,
            database_dir,
            stable_files,
            options,
        })
    }

    pub fn write(&self, key: &Vec<u8>, value: &[u8]) -> BitcaskResult<RowPosition> {
        let row = RowToWrite::new(&key, value);
        let writing_file_ref = self.writing_file.lock().unwrap();
        if self.check_file_overflow(&writing_file_ref, &row) {
            let next_writing_file =
                WritingFile::new(&self.database_dir, writing_file_ref.borrow().file_id + 1)?;
            let old_file = writing_file_ref.replace(next_writing_file);
            let (file_id, file) = old_file.transit_to_readonly()?;
            self.stable_files.insert(file_id, Mutex::new(file));
        }
        let mut writing_file = writing_file_ref.borrow_mut();
        writing_file.write_row(row)
    }

    pub fn iter(&self) -> BitcaskResult<Iter> {
        let mut opened_stable_files = open_stable_database_files(&self.database_dir)?
            .into_iter()
            .collect::<Vec<(u32, File)>>();
        opened_stable_files.sort_by_key(|e| e.0);
        Ok(Iter {
            files: opened_stable_files,
            current: 0,
        })
    }

    pub fn read_value(&self, row_position: &RowPosition) -> BitcaskResult<Vec<u8>> {
        {
            let writing_file_ref = self.writing_file.lock().unwrap();
            let mut writing_file = writing_file_ref.borrow_mut();
            if row_position.file_id == writing_file.file_id {
                return read_value_from_file(
                    row_position.file_id,
                    &mut writing_file.data_file,
                    row_position.row_offset,
                    row_position.row_size,
                );
            }
        }

        let l = self.get_file_to_read(row_position.file_id)?;
        let mut f = l.lock().unwrap();
        read_value_from_file(
            row_position.file_id,
            &mut f,
            row_position.row_offset,
            row_position.row_size,
        )
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

    fn get_file_to_read(&self, file_id: u32) -> BitcaskResult<RefMut<u32, Mutex<File>>> {
        self.stable_files
            .get_mut(&file_id)
            .ok_or(BitcaskError::TargetFileIdNotFound(file_id))
    }
}

pub struct IterItem {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub row_position: RowPosition,
}

pub struct Iter {
    files: Vec<(u32, File)>,
    current: usize,
}

impl Iterator for Iter {
    type Item = BitcaskResult<IterItem>;

    fn next(&mut self) -> Option<Self::Item> {
        let files_len = self.files.len();
        while self.current < files_len {
            let (file_id, file) = self.files.get_mut(self.current).unwrap();
            match read_next_item_from_file(file_id.clone(), file) {
                Err(BitcaskError::IoError(e)) => match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => {
                        self.current += 1;
                    }
                    _ => return Some(Err(BitcaskError::IoError(e))),
                },
                r => return Some(r),
            }
        }
        None
    }
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
        .slice(KEY_SIZE_OFFSET..(KEY_SIZE_OFFSET + KEY_SIZE_SIZE))
        .get_u64() as usize;
    let val_size = bs
        .slice(VALUE_SIZE_OFFSET..(VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE))
        .get_u64() as usize;
    let val_offset = KEY_OFFSET + key_size;
    let ret = bs.slice(val_offset..val_offset + val_size).into();
    Ok(ret)
}

fn read_next_item_from_file(file_id: u32, data_file: &mut File) -> BitcaskResult<IterItem> {
    let value_offset = data_file.seek(SeekFrom::Current(0))?;
    let mut header_buf = vec![0; KEY_OFFSET];
    data_file.read_exact(&mut header_buf)?;

    let header_bs = Bytes::from(header_buf);
    let expected_crc = header_bs.slice(0..4).get_u32();

    data_file.metadata().unwrap();

    let tstmp = header_bs.slice(TSTAMP_OFFSET..KEY_SIZE_OFFSET).get_u64();
    let key_size = header_bs
        .slice(KEY_SIZE_OFFSET..(KEY_SIZE_OFFSET + KEY_SIZE_SIZE))
        .get_u64() as usize;
    let value_size = header_bs
        .slice(VALUE_SIZE_OFFSET..(VALUE_SIZE_OFFSET + VALUE_SIZE_SIZE))
        .get_u64() as usize;

    let mut kv_buf = vec![0; key_size + value_size];
    data_file.read_exact(&mut kv_buf)?;
    let kv_bs = Bytes::from(kv_buf);
    let crc32 = Crc::<u32>::new(&CRC_32_CKSUM);
    let mut ck = crc32.digest();
    ck.update(&header_bs[4..]);
    ck.update(&kv_bs);
    let actual_crc = ck.finalize();
    if expected_crc != actual_crc {
        return Err(BitcaskError::CrcCheckFailed(
            file_id,
            value_offset,
            expected_crc,
            actual_crc,
        ));
    }

    Ok(IterItem {
        key: kv_bs.slice(0..key_size).into(),
        value: kv_bs.slice(key_size..).into(),
        row_position: RowPosition {
            file_id,
            row_offset: value_offset,
            row_size: KEY_OFFSET + key_size + value_size,
            tstmp,
        },
    })
}

#[cfg(test)]
mod tests {

    use super::*;
    const DEFAULT_OPTIONS: DataBaseOptions = DataBaseOptions {
        max_file_size: Some(1024),
    };

    #[test]
    fn test_read_write_writing_file() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
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
            let db = Database::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
            let kvs = [("k1", "value1"), ("k2", "value2")];
            offset_values.append(
                &mut kvs
                    .into_iter()
                    .map(|(k, v)| (db.write(&k.into(), v.as_bytes()).unwrap(), v))
                    .collect::<Vec<(RowPosition, &str)>>(),
            );
        }
        {
            let db = Database::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
            let kvs = [("k3", "hello world"), ("k1", "value4")];
            offset_values.append(
                &mut kvs
                    .into_iter()
                    .map(|(k, v)| (db.write(&k.into(), v.as_bytes()).unwrap(), v))
                    .collect::<Vec<(RowPosition, &str)>>(),
            );
        }

        let db = Database::open(&dir.path(), DEFAULT_OPTIONS).unwrap();
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
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(
            &dir.path(),
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
}
