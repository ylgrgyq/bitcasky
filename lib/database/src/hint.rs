use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    mem::ManuallyDrop,
    path::{Path, PathBuf},
    thread::{self, JoinHandle},
    vec,
};

use bytes::Bytes;
use log::{debug, error, info, warn};

use common::{
    formatter::{
        get_formatter_from_file, initialize_new_file, BitcaskFormatter, Formatter, RowHint,
        RowHintHeader,
    },
    fs::{self, FileType},
    storage_id::StorageId,
    tombstone,
};

use crate::{
    common::{DatabaseError, DatabaseResult},
    data_storage::DataStorage,
};
use crossbeam_channel::{unbounded, Sender};

use super::{common::RecoveredRow, data_storage::DataStorageOptions};

const DEFAULT_LOG_TARGET: &str = "Hint";
const HINT_FILES_TMP_DIRECTORY: &str = "TmpHint";

pub struct HintFile {
    storage_id: StorageId,
    file: File,
    formatter: BitcaskFormatter,
}

impl HintFile {
    pub fn create(database_dir: &Path, storage_id: StorageId) -> DatabaseResult<Self> {
        let mut file = fs::create_file(database_dir, FileType::HintFile, Some(storage_id))?;
        let formatter = initialize_new_file(&mut file)?;
        debug!(
            target: DEFAULT_LOG_TARGET,
            "create hint file with id: {}", storage_id
        );
        Ok(HintFile {
            storage_id,
            file,
            formatter,
        })
    }

    pub fn open_iterator(
        database_dir: &Path,
        storage_id: StorageId,
    ) -> DatabaseResult<HintFileIterator> {
        let file = Self::open(database_dir, storage_id)?;
        debug!(
            target: DEFAULT_LOG_TARGET,
            "open hint file iterator with id: {}", storage_id
        );
        Ok(HintFileIterator { file })
    }

    pub fn write_hint_row(&mut self, hint: &RowHint) -> DatabaseResult<()> {
        let data_to_write = self.formatter.encode_row_hint(hint);
        self.file.write_all(&data_to_write)?;
        debug!(target: DEFAULT_LOG_TARGET, "write hint row success. key: {:?}, header: {:?}", 
            hint.key, hint.header);
        Ok(())
    }

    pub fn read_hint_row(&mut self) -> DatabaseResult<RowHint> {
        let mut header_buf = vec![0; self.formatter.row_hint_header_size()];
        self.file.read_exact(&mut header_buf)?;

        let header_bs = Bytes::from(header_buf);
        let header = self.formatter.decode_row_hint_header(header_bs);

        let mut k_buf = vec![0; header.key_size as usize];
        self.file.read_exact(&mut k_buf)?;
        let key: Vec<u8> = Bytes::from(k_buf).into();

        debug!(target: DEFAULT_LOG_TARGET, "read hint row success. key: {:?}, header: {:?}", 
            key, header);

        Ok(RowHint { header, key })
    }

    fn open(database_dir: &Path, storage_id: StorageId) -> DatabaseResult<Self> {
        let mut file = fs::open_file(database_dir, FileType::HintFile, Some(storage_id))?;
        let formatter = get_formatter_from_file(&mut file.file).map_err(|e| {
            DatabaseError::HintFileCorrupted(e, storage_id, database_dir.display().to_string())
        })?;
        Ok(HintFile {
            storage_id,
            file: file.file,
            formatter,
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
    type Item = DatabaseResult<RecoveredRow>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.file.read_hint_row() {
            Err(DatabaseError::IoError(e)) => match e.kind() {
                std::io::ErrorKind::UnexpectedEof => None,
                _ => Some(Err(DatabaseError::IoError(e))),
            },
            r => Some(r.map(|h| RecoveredRow {
                row_location: super::RowLocation {
                    storage_id: self.file.storage_id,
                    row_offset: h.header.row_offset,
                },
                timestamp: h.header.timestamp,
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
    ) -> DatabaseResult<()> {
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
                    if tombstone::is_tombstone(&r.value) {
                        m.remove(&r.key);
                    } else {
                        m.insert(
                            r.key.clone(),
                            RowHint {
                                header: RowHintHeader {
                                    timestamp: r.timestamp,
                                    key_size: r.key.len() as u64,
                                    row_offset: r.row_location.row_offset,
                                },
                                key: r.key,
                            },
                        );
                    }
                }
                Err(e) => return Err(DatabaseError::StorageError(e)),
            }
        }

        let hint_file_tmp_dir = create_hint_file_tmp_dir(database_dir)?;
        let mut hint_file = HintFile::create(&hint_file_tmp_dir, data_storage_id)?;
        m.values()
            .map(|r| hint_file.write_hint_row(r))
            .collect::<DatabaseResult<Vec<_>>>()?;
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
            info!("asdfasdfsdf");
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

fn create_hint_file_tmp_dir(base_dir: &Path) -> DatabaseResult<PathBuf> {
    let p = hint_file_tmp_dir(base_dir);
    fs::create_dir(p.as_path())?;
    Ok(p)
}

fn hint_file_tmp_dir(base_dir: &Path) -> PathBuf {
    base_dir.join(HINT_FILES_TMP_DIRECTORY)
}

#[cfg(test)]
mod tests {
    use crate::data_storage::DataStorageWriter;
    use common::formatter::RowToWrite;

    use super::*;
    use test_log::test;

    use bitcask_tests::common::get_temporary_directory_path;

    #[test]
    fn test_read_write_hint_file() {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let key = vec![1, 2, 3];
        let expect_row = RowHint {
            header: RowHintHeader {
                timestamp: 12345,
                key_size: key.len() as u64,
                row_offset: 789,
            },
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
        assert_eq!(key.len() as u64, hint_row.header.key_size);
        assert_eq!(pos.row_offset, hint_row.header.row_offset);
    }
}
