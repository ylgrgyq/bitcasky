use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    mem::ManuallyDrop,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
};

use log::{debug, error, warn};

use crate::{
    clock::Clock,
    formatter::{
        get_formatter_from_file, padding, BitcaskyFormatter, Formatter, RowHint, RowHintHeader,
        FILE_HEADER_SIZE,
    },
    fs::{self, FileType},
    storage_id::StorageId,
};
use crate::{database::create_data_file, options::BitcaskyOptions};
use memmap2::{MmapMut, MmapOptions};

use crate::database::{
    common::{DatabaseError, DatabaseResult},
    data_storage::DataStorage,
    RowLocation,
};
use crossbeam_channel::{unbounded, Sender};

use super::common::RecoveredRow;

const DEFAULT_LOG_TARGET: &str = "Hint";
const HINT_FILES_TMP_DIRECTORY: &str = "TmpHint";

pub struct HintFile {
    storage_id: StorageId,
    file: File,
    formatter: BitcaskyFormatter,
    map_view: MmapMut,
    offset: usize,
    capacity: usize,
}

impl HintFile {
    pub fn create(
        database_dir: &Path,
        storage_id: StorageId,
        init_hint_file_capacity: usize,
    ) -> DatabaseResult<Self> {
        let formatter = BitcaskyFormatter::default();
        let file = create_data_file(
            database_dir,
            FileType::HintFile,
            Some(storage_id),
            &formatter,
            false,
            init_hint_file_capacity,
        )?;
        debug!(
            target: DEFAULT_LOG_TARGET,
            "create hint file with id: {}", storage_id
        );
        Self::new(file, storage_id, formatter)
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
        let value_offset = self.offset;

        let formatter = self.formatter;
        let net_size = formatter.encode_row_hint(hint, &mut self.as_mut_slice()[value_offset..]);

        self.offset += net_size + padding(net_size);

        debug!(target: DEFAULT_LOG_TARGET, "write hint row success. key: {:?}, header: {:?}, offset: {}", 
            hint.key, hint.header, value_offset);
        Ok(())
    }

    pub fn read_hint_row(&mut self) -> DatabaseResult<Option<RowHint>> {
        if self.offset + self.formatter.row_hint_header_size() >= self.capacity {
            return Ok(None);
        }

        let mut offset = self.offset;
        let header_bs = &self.as_slice()[offset..offset + self.formatter.row_hint_header_size()];
        let header = self.formatter.decode_row_hint_header(header_bs);

        offset += self.formatter.row_hint_header_size();

        let key: Vec<u8> = self.as_slice()[offset..(offset + header.key_size)].into();

        debug!(target: DEFAULT_LOG_TARGET, "read hint row success. key: {:?}, header: {:?}, offset: {}", 
            key, header, self.offset);

        let net_size = self.formatter.row_hint_header_size() + header.key_size;
        self.offset += net_size + padding(net_size);

        Ok(Some(RowHint { header, key }))
    }

    pub fn finish_write(&mut self) -> DatabaseResult<()> {
        fs::truncate_file(&mut self.file, self.offset)?;
        Ok(())
    }

    fn open(database_dir: &Path, storage_id: StorageId) -> DatabaseResult<Self> {
        let mut file = fs::open_file(database_dir, FileType::HintFile, Some(storage_id))?;
        let formatter = get_formatter_from_file(&mut file.file).map_err(|e| {
            DatabaseError::HintFileCorrupted(e, storage_id, database_dir.display().to_string())
        })?;
        Self::new(file.file, storage_id, formatter)
    }

    fn new(
        file: File,
        storage_id: StorageId,
        formatter: BitcaskyFormatter,
    ) -> DatabaseResult<HintFile> {
        let capacity = file.metadata()?.len() as usize;
        let mmap = unsafe { MmapOptions::new().offset(0).len(capacity).map_mut(&file)? };
        Ok(HintFile {
            storage_id,
            file,
            formatter,
            offset: FILE_HEADER_SIZE,
            map_view: mmap,
            capacity,
        })
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.map_view[0..self.capacity]
    }

    fn as_slice(&self) -> &[u8] {
        &self.map_view[0..self.capacity]
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
            Err(e) => Some(Err(e)),
            Ok(Some(r)) => Some(Ok(RecoveredRow {
                row_location: RowLocation {
                    storage_id: self.file.storage_id,
                    row_offset: r.header.row_offset,
                    row_size: r.header.row_size,
                },
                invalid: false,
                key: r.key,
            })),
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
pub struct HintWriterTelemetry {
    pub number_of_pending_hint_files: usize,
    pub write_times: u64,
}

#[derive(Debug)]
pub struct HintWriter {
    sender: ManuallyDrop<Sender<StorageId>>,
    worker_join_handle: Option<JoinHandle<()>>,
    write_counter: Arc<AtomicU64>,
}

impl HintWriter {
    pub fn start(database_dir: &Path, options: Arc<BitcaskyOptions>) -> HintWriter {
        let (sender, receiver) = unbounded();

        let write_counter = Arc::new(AtomicU64::new(0));
        let moved_counter = write_counter.clone();
        let moved_dir = database_dir.to_path_buf();
        let worker_join_handle = Some(thread::spawn(move || {
            while let Ok(storage_id) = receiver.recv() {
                if let Err(e) = Self::write_hint_file(&moved_dir, storage_id, options.clone()) {
                    warn!(
                        target: DEFAULT_LOG_TARGET,
                        "write hint file with id: {} under path: {} failed {}",
                        storage_id,
                        moved_dir.display(),
                        e
                    );
                } else {
                    moved_counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));

        HintWriter {
            sender: ManuallyDrop::new(sender),
            worker_join_handle,
            write_counter,
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

    pub fn get_telemetry_data(&self) -> HintWriterTelemetry {
        HintWriterTelemetry {
            number_of_pending_hint_files: self.sender.len(),
            write_times: self.write_counter.load(Ordering::Acquire),
        }
    }

    fn write_hint_file(
        database_dir: &Path,
        data_storage_id: StorageId,
        options: Arc<BitcaskyOptions>,
    ) -> DatabaseResult<()> {
        let m = HintWriter::build_row_hint(database_dir, data_storage_id, options.clone())?;

        let hint_file_tmp_dir = create_hint_file_tmp_dir(database_dir)?;
        let mut hint_file = HintFile::create(
            &hint_file_tmp_dir,
            data_storage_id,
            options.database.init_hint_file_capacity,
        )?;
        m.values()
            .map(|r| hint_file.write_hint_row(r))
            .collect::<DatabaseResult<Vec<_>>>()?;

        hint_file.finish_write()?;

        fs::move_file(
            FileType::HintFile,
            Some(data_storage_id),
            &hint_file_tmp_dir,
            database_dir,
        )?;
        Ok(())
    }

    fn build_row_hint(
        database_dir: &Path,
        data_storage_id: StorageId,
        options: Arc<BitcaskyOptions>,
    ) -> DatabaseResult<HashMap<Vec<u8>, RowHint>> {
        let stable_file_opt = DataStorage::open(database_dir, data_storage_id, options.clone())?;

        let data_itr = stable_file_opt.iter()?;

        let mut m = HashMap::new();
        for row in data_itr {
            match row {
                Ok(r) => {
                    if !r.value.is_valid(options.clock.now()) {
                        m.remove(&r.key);
                    } else {
                        m.insert(
                            r.key.clone(),
                            RowHint {
                                header: RowHintHeader {
                                    expire_timestamp: r.value.expire_timestamp,
                                    key_size: r.key.len(),
                                    row_offset: r.row_location.row_offset,
                                    row_size: r.row_location.row_size,
                                },
                                key: r.key,
                            },
                        );
                    }
                }
                Err(e) => return Err(DatabaseError::StorageError(e)),
            }
        }
        Ok(m)
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
    use crate::database::data_storage::DataStorageWriter;
    use crate::formatter::RowToWrite;

    use super::*;
    use test_log::test;

    use crate::test_utils::get_temporary_directory_path;

    #[test]
    fn test_read_write_hint_file() {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let key = vec![1, 2, 3];
        let expect_row = RowHint {
            header: RowHintHeader {
                expire_timestamp: 12345,
                key_size: key.len(),
                row_offset: 789,
                row_size: 123,
            },
            key,
        };
        {
            let mut hint_file = HintFile::create(&dir, storage_id, 1024).unwrap();
            hint_file.write_hint_row(&expect_row).unwrap();
        }
        let mut hint_file = HintFile::open(&dir, storage_id).unwrap();
        if let Some(actual_row) = hint_file.read_hint_row().unwrap() {
            assert_eq!(expect_row, actual_row);
        } else {
            unreachable!();
        }
    }

    #[test]
    fn test_read_write_stable_data_file() {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let mut writing_file = DataStorage::new(
            &dir,
            storage_id,
            Arc::new(BitcaskyFormatter::default()),
            Arc::new(
                BitcaskyOptions::default()
                    .max_data_file_size(1024)
                    .init_data_file_capacity(100),
            ),
        )
        .unwrap();
        let key = vec![1, 2, 3];
        let val: [u8; 3] = [5, 6, 7];
        let pos = writing_file
            .write_row(&RowToWrite::new(&key, val.to_vec()))
            .unwrap();
        writing_file.flush().unwrap();

        {
            let writer = HintWriter::start(
                &dir,
                Arc::new(
                    BitcaskyOptions::default()
                        .max_data_file_size(1024)
                        .init_data_file_capacity(100),
                ),
            );
            writer.async_write_hint_file(storage_id);
        }

        let mut hint_file = HintFile::open(&dir, storage_id).unwrap();
        if let Some(hint_row) = hint_file.read_hint_row().unwrap() {
            assert_eq!(key, hint_row.key);
            assert_eq!(key.len(), hint_row.header.key_size);
            assert_eq!(pos.row_offset, hint_row.header.row_offset);
        } else {
            unreachable!();
        }
    }
}
