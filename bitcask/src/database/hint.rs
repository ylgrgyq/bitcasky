use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    mem::ManuallyDrop,
    path::{Path, PathBuf},
    thread::{self, JoinHandle},
    vec,
};

use bytes::{Buf, Bytes, BytesMut};
use log::{debug, error, info, warn};

use crate::{
    database::data_storage::DataStorage,
    error::{BitcaskError, BitcaskResult},
    fs::{self, FileType},
    storage_id::StorageId,
    utils,
};
use crossbeam_channel::{unbounded, Sender};

use super::{common::RecoveredRow, data_storage::DataStorageOptions};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct HintRow {
    pub timestamp: u64,
    pub key_size: u64,
    pub row_offset: u64,
    pub key: Vec<u8>,
}

const TSTAMP_SIZE: usize = 8;
const KEY_SIZE_SIZE: usize = 8;
const ROW_OFFSET_SIZE: usize = 8;
const HINT_FILE_KEY_SIZE_OFFSET: usize = TSTAMP_SIZE;
const HINT_FILE_ROW_OFFSET_OFFSET: usize = HINT_FILE_KEY_SIZE_OFFSET + KEY_SIZE_SIZE;
const HINT_FILE_KEY_OFFSET: usize = HINT_FILE_ROW_OFFSET_OFFSET + ROW_OFFSET_SIZE;
const HINT_FILE_HEADER_SIZE: usize = TSTAMP_SIZE + KEY_SIZE_SIZE + ROW_OFFSET_SIZE;
const DEFAULT_LOG_TARGET: &str = "Hint";
const HINT_FILES_TMP_DIRECTORY: &str = "TmpHint";

pub struct HintFile {
    storage_id: StorageId,
    file: File,
}

impl HintFile {
    pub fn create(database_dir: &Path, storage_id: StorageId) -> BitcaskResult<Self> {
        let file = fs::create_file(database_dir, FileType::HintFile, Some(storage_id))?;
        debug!(
            target: DEFAULT_LOG_TARGET,
            "create hint file with id: {}", storage_id
        );
        Ok(HintFile { storage_id, file })
    }

    pub fn open_iterator(
        database_dir: &Path,
        storage_id: StorageId,
    ) -> BitcaskResult<HintFileIterator> {
        let file = Self::open(database_dir, storage_id)?;
        debug!(
            target: DEFAULT_LOG_TARGET,
            "open hint file iterator with id: {}", storage_id
        );
        Ok(HintFileIterator { file })
    }

    pub fn write_hint_row(&mut self, hint: &HintRow) -> BitcaskResult<()> {
        let data_to_write = self.to_bytes(hint);
        self.file.write_all(&data_to_write)?;
        debug!(target: DEFAULT_LOG_TARGET, "write hint row success. key: {:?}, key_size: {}, row_offset: {}, timestamp: {}", 
            hint.key, hint.key_size, hint.row_offset, hint.timestamp);
        Ok(())
    }

    pub fn read_hint_row(&mut self) -> BitcaskResult<HintRow> {
        let mut header_buf = vec![0; HINT_FILE_HEADER_SIZE];
        self.file.read_exact(&mut header_buf)?;

        let header_bs = Bytes::from(header_buf);
        let timestamp = header_bs.slice(0..TSTAMP_SIZE).get_u64();
        let key_size = header_bs
            .slice(HINT_FILE_KEY_SIZE_OFFSET..HINT_FILE_ROW_OFFSET_OFFSET)
            .get_u64();
        let row_offset = header_bs
            .slice(HINT_FILE_ROW_OFFSET_OFFSET..HINT_FILE_KEY_OFFSET)
            .get_u64();

        let mut k_buf = vec![0; key_size as usize];
        self.file.read_exact(&mut k_buf)?;
        let key: Vec<u8> = Bytes::from(k_buf).into();

        debug!(target: DEFAULT_LOG_TARGET, "read hint row success. key: {:?}, key_size: {}, row_offset: {}, timestamp: {}", 
            key, key_size, row_offset, timestamp);

        Ok(HintRow {
            timestamp,
            key_size,
            row_offset,
            key,
        })
    }

    fn to_bytes(&self, row: &HintRow) -> Bytes {
        let mut bs = BytesMut::with_capacity(HINT_FILE_HEADER_SIZE + row.key.len());
        bs.extend_from_slice(&row.timestamp.to_be_bytes());
        bs.extend_from_slice(&row.key_size.to_be_bytes());
        bs.extend_from_slice(&row.row_offset.to_be_bytes());
        bs.extend_from_slice(&row.key);
        bs.freeze()
    }

    fn open(database_dir: &Path, storage_id: StorageId) -> BitcaskResult<Self> {
        let file = fs::open_file(database_dir, FileType::HintFile, Some(storage_id))?;
        Ok(HintFile {
            storage_id,
            file: file.file,
        })
    }
}

impl Drop for HintFile {
    fn drop(&mut self) {
        if let Err(e) = self.file.flush() {
            warn!(target: DEFAULT_LOG_TARGET, "flush hint file failed. {}", e)
        }
    }
}

pub struct HintFileIterator {
    file: HintFile,
}

impl Iterator for HintFileIterator {
    type Item = BitcaskResult<RecoveredRow>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.file.read_hint_row() {
            Err(BitcaskError::IoError(e)) => match e.kind() {
                std::io::ErrorKind::UnexpectedEof => None,
                _ => Some(Err(BitcaskError::IoError(e))),
            },
            r => Some(r.map(|h| RecoveredRow {
                row_location: super::RowLocation {
                    storage_id: self.file.storage_id,
                    row_offset: h.row_offset,
                },
                timestamp: h.timestamp,
                key: h.key,
                is_tombstone: false,
            })),
        }
    }
}

#[derive(Debug)]
pub struct HintWriter {
    sender: ManuallyDrop<Sender<StorageId>>,
    worker_join_handle: Option<JoinHandle<()>>,
}

impl HintWriter {
    pub fn start(database_dir: &Path, storage_options: DataStorageOptions) -> HintWriter {
        let (sender, receiver) = unbounded();

        let moved_dir = database_dir.to_path_buf();
        let worker_join_handle = Some(thread::spawn(move || {
            while let Ok(storage_id) = receiver.recv() {
                if let Err(e) = Self::write_hint_file(&moved_dir, storage_id, storage_options) {
                    warn!(
                        target: DEFAULT_LOG_TARGET,
                        "write hint file with id: {} under path: {} failed {}",
                        storage_id,
                        moved_dir.display(),
                        e
                    );
                }
            }
        }));

        HintWriter {
            sender: ManuallyDrop::new(sender),
            worker_join_handle,
        }
    }

    pub fn async_write_hint_file(&self, data_storage_id: StorageId) {
        if let Err(e) = self.sender.send(data_storage_id) {
            error!(
                target: DEFAULT_LOG_TARGET,
                "send file id: {} to hint file writer failed with error {}", data_storage_id, e
            );
        }
    }

    pub fn len(&self) -> usize {
        self.sender.len()
    }

    fn write_hint_file(
        database_dir: &Path,
        data_storage_id: StorageId,
        storage_options: DataStorageOptions,
    ) -> BitcaskResult<()> {
        let stable_file_opt = DataStorage::open(database_dir, data_storage_id, storage_options)?;
        if stable_file_opt.is_empty() {
            info!(
                target: DEFAULT_LOG_TARGET,
                "skip write hint for empty data file with id: {}", &data_storage_id
            );
            return Ok(());
        }

        let data_itr = stable_file_opt.iter()?;

        let mut m = HashMap::new();
        for row in data_itr {
            match row {
                Ok(r) => {
                    if utils::is_tombstone(&r.value) {
                        m.remove(&r.key);
                    } else {
                        m.insert(
                            r.key.clone(),
                            HintRow {
                                timestamp: r.timestamp,
                                key_size: r.key.len() as u64,
                                row_offset: r.row_location.row_offset,
                                key: r.key,
                            },
                        );
                    }
                }
                Err(e) => return Err(BitcaskError::StorageError(e)),
            }
        }

        let hint_file_tmp_dir = create_hint_file_tmp_dir(database_dir)?;
        let mut hint_file = HintFile::create(&hint_file_tmp_dir, data_storage_id)?;
        m.values()
            .map(|r| hint_file.write_hint_row(r))
            .collect::<BitcaskResult<Vec<_>>>()?;
        fs::move_file(
            FileType::HintFile,
            Some(data_storage_id),
            &hint_file_tmp_dir,
            database_dir,
        )?;
        Ok(())
    }
}

impl Drop for HintWriter {
    fn drop(&mut self) {
        unsafe { ManuallyDrop::drop(&mut self.sender) }
        if let Some(join_handle) = self.worker_join_handle.take() {
            if join_handle.join().is_err() {
                error!(
                    target: DEFAULT_LOG_TARGET,
                    "wait worker thread finish failed"
                );
            }
        }
    }
}

pub fn clear_temp_hint_file_directory(database_dir: &Path) {
    if let Err(e) = create_hint_file_tmp_dir(database_dir).and_then(|hint_file_tmp_dir| {
        let paths = std::fs::read_dir(hint_file_tmp_dir)?;
        for path in paths {
            let file_path = path?;
            if !file_path.path().is_file() {
                continue;
            }
            std::fs::remove_file(file_path.path())?;
        }
        Ok(())
    }) {
        error!(
            target: DEFAULT_LOG_TARGET,
            "clear temp hint file directory failed. {}", e
        )
    }
}

fn create_hint_file_tmp_dir(base_dir: &Path) -> BitcaskResult<PathBuf> {
    let p = hint_file_tmp_dir(base_dir);
    fs::create_dir(p.as_path())?;
    Ok(p)
}

fn hint_file_tmp_dir(base_dir: &Path) -> PathBuf {
    base_dir.join(HINT_FILES_TMP_DIRECTORY)
}

#[cfg(test)]
mod tests {
    use crate::database::{common::RowToWrite, data_storage::DataStorageWriter};

    use super::*;
    use test_log::test;

    use bitcask_tests::common::get_temporary_directory_path;

    #[test]
    fn test_read_write_hint_file() {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let key = vec![1, 2, 3];
        let expect_row = HintRow {
            timestamp: 12345,
            key_size: key.len() as u64,
            row_offset: 789,
            key,
        };
        {
            let mut hint_file = HintFile::create(&dir, storage_id).unwrap();
            hint_file.write_hint_row(&expect_row).unwrap();
        }
        let mut hint_file = HintFile::open(&dir, storage_id).unwrap();
        let actual_row = hint_file.read_hint_row().unwrap();
        assert_eq!(expect_row, actual_row);
    }

    #[test]
    fn test_read_write_stable_data_file() {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let mut writing_file = DataStorage::new(
            &dir,
            storage_id,
            DataStorageOptions {
                max_file_size: 1024,
            },
        )
        .unwrap();
        let key = vec![1, 2, 3];
        let val: [u8; 3] = [5, 6, 7];
        let pos = writing_file
            .write_row(&RowToWrite::new(&key, val.to_vec()))
            .unwrap();
        writing_file.transit_to_readonly().unwrap();

        {
            let writer = HintWriter::start(
                &dir,
                DataStorageOptions {
                    max_file_size: 1024,
                },
            );
            writer.async_write_hint_file(storage_id);
        }

        let mut hint_file = HintFile::open(&dir, storage_id).unwrap();
        let hint_row = hint_file.read_hint_row().unwrap();

        assert_eq!(key, hint_row.key);
        assert_eq!(key.len() as u64, hint_row.key_size);
        assert_eq!(pos.row_offset, hint_row.row_offset);
    }
}
