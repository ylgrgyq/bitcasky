use core::panic;
use std::{
    cell::{Cell, RefCell},
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
    vec,
};

use bytes::{Buf, Bytes, BytesMut};
use crc::{Crc, CRC_32_CKSUM};
use dashmap::{mapref::one::RefMut, DashMap};

use crate::{
    error::{BitcaskError, BitcaskResult},
    file_id::FileIdGenerator,
    file_manager::{
        self, create_file, get_valid_data_file_ids, open_data_files_under_path, open_file, FileType,
    },
};
use log::{error, info, warn};

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
        let data_file = create_file(&database_dir, FileType::DataFile(file_id))?;
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

#[derive(Debug)]
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
        let file = file_manager::open_file(&self.database_dir, FileType::DataFile(self.file_id))?;
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
        let file = file_manager::open_file(&self.database_dir, FileType::HintFile(self.file_id))?;
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

/**
 * Statistics of a Database.
 * Some of the metrics may not accurate due to concurrent access.
 */
pub struct DatabaseStats {
    /**
     * Number of data files in Database
     */
    pub number_of_data_files: usize,
    /**
     * Number of data files in Database
     */
    pub number_of_hint_files: usize,
    /**
     * Data size in bytes of this Database
     */
    pub total_data_size_in_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct DataBaseOptions {
    pub max_file_size: usize,
}

#[derive(Debug)]
pub struct Database {
    pub database_dir: PathBuf,
    file_id_generator: Arc<FileIdGenerator>,
    writing_file: Mutex<RefCell<WritingFile>>,
    stable_files: DashMap<u32, Mutex<StableFile>>,
    options: DataBaseOptions,
}

fn validate_database_directory(dir: &Path) -> BitcaskResult<()> {
    fs::create_dir_all(dir)?;
    if !file_manager::check_directory_is_writable(dir) {
        return Err(BitcaskError::PermissionDenied(format!(
            "do not have writable permission for path: {}",
            dir.display()
        )));
    }
    Ok(())
}

fn shift_data_files(
    database_dir: &Path,
    known_max_file_id: u32,
    file_id_generator: &Arc<FileIdGenerator>,
) -> BitcaskResult<Vec<u32>> {
    let mut data_file_ids = get_valid_data_file_ids(database_dir)
        .into_iter()
        .filter(|id| *id >= known_max_file_id)
        .collect::<Vec<u32>>();
    // must change name in descending order to keep data file's order even when any change name operation failed
    data_file_ids.sort_by(|a, b| b.cmp(a));

    // rename files which file id >= knwon_max_file_id to files which file id greater than all merged files
    // because values in these files is written after merged files
    let mut new_file_ids = vec![];
    for from_id in data_file_ids {
        let new_file_id = file_id_generator.generate_next_file_id();
        file_manager::change_data_file_id(database_dir, from_id, new_file_id)?;
        new_file_ids.push(new_file_id);
    }
    Ok(new_file_ids)
}

fn recover_merge(
    database_dir: &Path,
    file_id_generator: &Arc<FileIdGenerator>,
) -> BitcaskResult<()> {
    let merge_file_dir = file_manager::merge_file_dir(database_dir);

    if !merge_file_dir.exists() {
        return Ok(());
    }

    let mut merge_data_file_ids = file_manager::get_valid_data_file_ids(&merge_file_dir);
    if merge_data_file_ids.is_empty() {
        return Ok(());
    }

    merge_data_file_ids.sort();
    let merge_meta = file_manager::read_merge_meta(&merge_file_dir)?;
    if *merge_data_file_ids.first().unwrap() <= merge_meta.known_max_file_id {
        return Err(BitcaskError::InvalidMergeDataFile(
            merge_meta.known_max_file_id,
            *merge_data_file_ids.first().unwrap(),
        ));
    }

    file_id_generator.update_file_id(*merge_data_file_ids.last().unwrap());

    shift_data_files(
        database_dir,
        merge_meta.known_max_file_id,
        file_id_generator,
    )?;

    file_manager::commit_merge_files(database_dir, &merge_data_file_ids)?;

    file_manager::purge_outdated_data_files(database_dir, merge_meta.known_max_file_id)?;

    let clear_ret = file_manager::clear_dir(&merge_file_dir);
    if clear_ret.is_err() {
        warn!(
            "clear merge directory failed after merge recovered. {}",
            clear_ret.unwrap_err()
        );
    }

    Ok(())
}

impl Database {
    pub fn open(
        directory: &Path,
        file_id_generator: Arc<FileIdGenerator>,
        options: DataBaseOptions,
    ) -> BitcaskResult<Database> {
        let database_dir: PathBuf = directory.into();
        validate_database_directory(&database_dir)?;

        let recover_ret = recover_merge(&database_dir, &file_id_generator);
        if recover_ret.is_err() {
            let merge_dir = file_manager::merge_file_dir(&database_dir);
            warn!(
                "recover merge under path: {} failed with error: \"{}\"",
                merge_dir.display(),
                recover_ret.as_ref().unwrap_err()
            );
            match recover_ret.as_ref().unwrap_err() {
                BitcaskError::InvalidMergeDataFile(_, _) => {
                    // clear Merge directory when recover merge failed
                    file_manager::clear_dir(&file_manager::merge_file_dir(&database_dir))?;
                }
                _ => return Err(recover_ret.unwrap_err()),
            }
        }

        let opened_stable_files = open_data_files_under_path(&database_dir)?;
        if !opened_stable_files.is_empty() {
            let writing_file_id = opened_stable_files.keys().max().unwrap_or(&0);
            file_id_generator.update_file_id(*writing_file_id);
        }
        let writing_file = Mutex::new(RefCell::new(WritingFile::new(
            &database_dir,
            file_id_generator.generate_next_file_id(),
        )?));
        let stable_files = opened_stable_files
            .into_iter()
            .map(|(k, v)| (k, Mutex::new(StableFile::new(&database_dir, k, v))))
            .collect::<DashMap<u32, Mutex<StableFile>>>();

        info!(target: "Database", "database opened at directory: {:?}, with {} file recovered", directory, stable_files.len());
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
        // flush file only when we actually wrote something
        if writing_file_ref.borrow().file_size > 0 {
            self.do_flush_writing_file(&writing_file_ref)?;
        }
        Ok(())
    }

    pub fn iter(&self) -> BitcaskResult<DatabaseIter> {
        let mut file_ids: Vec<u32>;
        {
            let writing_file = self.writing_file.lock().unwrap();
            let writing_file_id = writing_file.borrow().file_id;

            file_ids = self
                .stable_files
                .iter()
                .map(|f| f.lock().unwrap().file_id)
                .collect::<Vec<u32>>();
            file_ids.push(writing_file_id);
        }

        let files: BitcaskResult<Vec<StableFile>> = file_ids
            .iter()
            .map(|id| file_manager::open_file(&self.database_dir, FileType::DataFile(*id)))
            .map(|f| {
                f.and_then(|f| match f.file_type {
                    FileType::DataFile(id) => Ok(StableFile::new(&self.database_dir, id, f.file)),
                    _ => unreachable!(),
                })
            })
            .collect();
        let mut opened_stable_files = files?;
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

    pub fn load_merged_files(
        &self,
        merged_file_ids: &Vec<u32>,
        known_max_file_id: u32,
    ) -> BitcaskResult<()> {
        if merged_file_ids.is_empty() {
            return Ok(());
        }
        self.flush_writing_file()?;

        let data_file_ids = shift_data_files(
            &self.database_dir,
            known_max_file_id,
            &self.file_id_generator,
        )?;

        // rebuild stable files with file id >= known_max_file_id files and merged files
        self.stable_files.clear();

        for file_id in data_file_ids {
            self.open_stable_file(file_id)?;
        }

        file_manager::commit_merge_files(&self.database_dir, &merged_file_ids)?;

        for file_id in merged_file_ids {
            if self.stable_files.contains_key(&file_id) {
                panic!("merged file id: {} already loaded in database", file_id);
            }
            self.open_stable_file(*file_id)?;
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

    pub fn write_hint_file(&self, file_id: u32) -> BitcaskResult<()> {
        let row_hint_file =
            file_manager::create_file(&self.database_dir, FileType::HintFile(file_id))?;
        let mut hint_file = HintFile::new(&self.database_dir, file_id, row_hint_file);

        let data_file = file_manager::open_file(&self.database_dir, FileType::DataFile(file_id))?;
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

    pub fn stats(&self) -> BitcaskResult<DatabaseStats> {
        let mut writing_file_size: u64 = 0;
        {
            writing_file_size = self.writing_file.lock().unwrap().borrow().file_size as u64;
        }
        let mut total_data_size_in_bytes: u64 = self
            .stable_files
            .iter()
            .map(|f| {
                let file = f.value().lock().unwrap();
                file.file.metadata().and_then(|m| Ok(m.len()))
            })
            .collect::<Result<Vec<u64>, std::io::Error>>()?
            .iter()
            .sum();
        total_data_size_in_bytes += writing_file_size;

        Ok(DatabaseStats {
            number_of_data_files: self.stable_files.len() + 1,
            number_of_hint_files: 0,
            total_data_size_in_bytes,
        })
    }

    pub fn close(&self) -> BitcaskResult<()> {
        let writing_file_ref = self.writing_file.lock().unwrap();
        writing_file_ref.borrow_mut().flush()?;
        Ok(())
    }

    fn open_stable_file(&self, file_id: u32) -> BitcaskResult<()> {
        let data_file = open_file(&self.database_dir, FileType::DataFile(file_id))?;
        let meta = data_file.file.metadata()?;
        if meta.len() <= 0 {
            info!(target: "Database", "skip load empty data file with id: {}", &file_id);
            return Ok(());
        }
        self.stable_files.insert(
            file_id,
            Mutex::new(StableFile::new(&self.database_dir, file_id, data_file.file)),
        );
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
        row.size + writing_file.file_size > self.options.max_file_size
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
    use crate::file_manager::MergeMeta;

    use super::*;
    use bitcask_tests::common::{get_temporary_directory_path, TestingKV};
    use test_log::test;

    const DEFAULT_OPTIONS: DataBaseOptions = DataBaseOptions {
        max_file_size: 1024,
    };

    struct TestingRow {
        kv: TestingKV,
        pos: RowPosition,
    }

    impl TestingRow {
        fn new(kv: TestingKV, pos: RowPosition) -> TestingRow {
            TestingRow { kv, pos }
        }
    }

    fn assert_rows_value(db: &Database, expect: &Vec<TestingRow>) {
        for row in expect {
            assert_row_value(db, row);
        }
    }

    fn assert_row_value(db: &Database, expect: &TestingRow) {
        let actual = db.read_value(&expect.pos).unwrap();
        assert_eq!(*expect.kv.value(), actual);
    }

    fn assert_database_rows(db: &Database, expect_rows: &Vec<TestingRow>) {
        let mut i = 0;
        for actual_row in db.iter().unwrap().map(|r| r.unwrap()) {
            let expect_row = expect_rows.get(i).unwrap();
            assert_eq!(expect_row.kv.key(), actual_row.key);
            assert_eq!(expect_row.kv.value(), actual_row.value);
            assert_eq!(expect_row.pos, actual_row.row_position);
            i += 1;
        }
        assert_eq!(expect_rows.len(), i);
    }

    fn write_kvs_to_db(db: &Database, kvs: Vec<TestingKV>) -> Vec<TestingRow> {
        kvs.into_iter()
            .map(|kv| {
                let pos = db.write(&kv.key(), &kv.value()).unwrap();
                TestingRow::new(kv, pos)
            })
            .collect::<Vec<TestingRow>>()
    }

    #[test]
    fn test_read_write_writing_file() {
        let dir = get_temporary_directory_path();
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let db = Database::open(&dir, file_id_generator, DEFAULT_OPTIONS).unwrap();
        let kvs = vec![
            TestingKV::new("k1", "value1奥森"),
            TestingKV::new("k2", "value2"),
            TestingKV::new("k3", "value3"),
            TestingKV::new("k1", "value4"),
        ];
        let rows = write_kvs_to_db(&db, kvs);
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_read_write_with_stable_files() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        let kvs = vec![
            TestingKV::new("k1", "value1"),
            TestingKV::new("k2", "value2"),
        ];
        rows.append(&mut write_kvs_to_db(&db, kvs));
        db.flush_writing_file().unwrap();

        let kvs = vec![
            TestingKV::new("k3", "hello world"),
            TestingKV::new("k1", "value4"),
        ];
        rows.append(&mut write_kvs_to_db(&db, kvs));
        db.flush_writing_file().unwrap();

        assert_eq!(3, file_id_generator.get_file_id());
        assert_eq!(2, db.stable_files.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_recovery() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let file_id_generator = Arc::new(FileIdGenerator::new());
        {
            let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }
        {
            let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k3", "hello world"),
                TestingKV::new("k1", "value4"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }

        let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        assert_eq!(3, file_id_generator.get_file_id());
        assert_eq!(2, db.stable_files.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_recover_merge_with_only_merge_meta() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let file_id_generator = Arc::new(FileIdGenerator::new());
        {
            let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }
        let merge_file_dir = file_manager::create_merge_file_dir(&dir).unwrap();
        let merge_meta = MergeMeta {
            known_max_file_id: 101,
        };
        file_manager::write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        assert_eq!(2, file_id_generator.get_file_id());
        assert_eq!(1, db.stable_files.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_recover_merge_with_invalid_merge_meta() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let file_id_generator = Arc::new(FileIdGenerator::new());
        {
            let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }
        let merge_file_dir = file_manager::create_merge_file_dir(&dir).unwrap();
        {
            // write something to data file in merge dir
            let db = Database::open(&merge_file_dir, file_id_generator.clone(), DEFAULT_OPTIONS)
                .unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            write_kvs_to_db(&db, kvs);
        }

        let merge_meta = MergeMeta {
            known_max_file_id: file_id_generator.generate_next_file_id(),
        };
        file_manager::write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        assert_eq!(4, file_id_generator.get_file_id());
        assert_eq!(1, db.stable_files.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
        assert!(!merge_file_dir.exists());
    }

    #[test]
    fn test_recover_merge() {
        let dir = get_temporary_directory_path();
        let file_id_generator = Arc::new(FileIdGenerator::new());
        {
            let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            write_kvs_to_db(&db, kvs);
        }
        let merge_meta = MergeMeta {
            known_max_file_id: file_id_generator.generate_next_file_id(),
        };
        let merge_file_dir = file_manager::create_merge_file_dir(&dir).unwrap();
        file_manager::write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        let mut rows: Vec<TestingRow> = vec![];
        {
            // write something to data file in merge dir
            let db = Database::open(&merge_file_dir, file_id_generator.clone(), DEFAULT_OPTIONS)
                .unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value3"),
                TestingKV::new("k2", "value4"),
                TestingKV::new("k3", "value5"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }

        let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        assert_eq!(4, file_id_generator.get_file_id());
        assert_eq!(1, db.stable_files.len());
        assert_rows_value(&db, &rows);
        assert_database_rows(&db, &rows);
        assert!(!merge_file_dir.exists());
    }

    #[test]
    fn test_recover_merge_failed_with_unexpeded_error() {
        let dir = get_temporary_directory_path();
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let mut rows: Vec<TestingRow> = vec![];
        {
            let db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value1"),
                TestingKV::new("k2", "value2"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
        }
        let merge_meta = MergeMeta {
            known_max_file_id: file_id_generator.generate_next_file_id(),
        };
        let merge_file_dir = file_manager::create_merge_file_dir(&dir).unwrap();
        file_manager::write_merge_meta(&merge_file_dir, merge_meta).unwrap();
        {
            // write something to data file in merge dir
            let db = Database::open(&merge_file_dir, file_id_generator.clone(), DEFAULT_OPTIONS)
                .unwrap();
            let kvs = vec![
                TestingKV::new("k1", "value3"),
                TestingKV::new("k2", "value4"),
            ];
            write_kvs_to_db(&db, kvs);
        }

        // change one data file under merge directory to readonly
        // so this file cannot recover and move to base directory
        let meta = fs::metadata(&merge_file_dir).unwrap();
        let mut perms = meta.permissions();
        perms.set_readonly(true);
        fs::set_permissions(&merge_file_dir, perms).unwrap();

        let ret = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS);
        assert!(ret.is_err());
    }

    #[test]
    fn test_wrap_file() {
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let dir = get_temporary_directory_path();
        let db = Database::open(
            &dir,
            file_id_generator,
            DataBaseOptions { max_file_size: 100 },
        )
        .unwrap();
        let kvs = vec![
            TestingKV::new("k1", "value1_value1_value1"),
            TestingKV::new("k2", "value2_value2_value2"),
            TestingKV::new("k3", "value3_value3_value3"),
            TestingKV::new("k1", "value4_value4_value4"),
        ];
        assert_eq!(0, db.stable_files.len());
        let rows = write_kvs_to_db(&db, kvs);
        assert_rows_value(&db, &rows);
        assert_eq!(1, db.stable_files.len());
        assert_database_rows(&db, &rows);
    }

    #[test]
    fn test_load_merged_files() {
        let dir = get_temporary_directory_path();
        let mut rows: Vec<TestingRow> = vec![];
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let old_db = Database::open(&dir, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
        let kvs = vec![
            TestingKV::new("k1", "value1"),
            TestingKV::new("k2", "value2"),
        ];
        rows.append(&mut write_kvs_to_db(&old_db, kvs));
        {
            let merge_path = file_manager::create_merge_file_dir(&dir).unwrap();
            let db =
                Database::open(&merge_path, file_id_generator.clone(), DEFAULT_OPTIONS).unwrap();
            let kvs = vec![
                TestingKV::new("k3", "hello world"),
                TestingKV::new("k1", "value4"),
            ];
            rows.append(&mut write_kvs_to_db(&db, kvs));
            old_db
                .load_merged_files(&db.get_file_ids(), old_db.get_max_file_id())
                .unwrap();
        }

        assert_eq!(5, file_id_generator.get_file_id());
        assert_eq!(2, old_db.stable_files.len());
    }

    #[test]
    fn test_hint_file() {
        let dir = get_temporary_directory_path();
        let mut offset_values: Vec<(RowPosition, &str)> = vec![];
        {
            let file_id_generator = Arc::new(FileIdGenerator::new());
            let db = Database::open(&dir, file_id_generator, DEFAULT_OPTIONS).unwrap();
            let kvs = [("k1", "value1"), ("k2", "value2")];
            offset_values.append(
                &mut kvs
                    .into_iter()
                    .map(|(k, v)| (db.write(&k.into(), v.as_bytes()).unwrap(), v))
                    .collect::<Vec<(RowPosition, &str)>>(),
            );
        }
        let file_id_generator = Arc::new(FileIdGenerator::new());
        let db = Database::open(&dir, file_id_generator, DEFAULT_OPTIONS).unwrap();
        db.write_hint_file(1);
    }
}
