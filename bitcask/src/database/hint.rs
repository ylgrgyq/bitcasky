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
use log::{error, warn};

use crate::{
    error::{BitcaskError, BitcaskResult},
    fs::{self, FileType},
    utils,
};
use crossbeam_channel::{unbounded, Sender};

use super::{
    common::RecoveredRow,
    constants::{KEY_SIZE_SIZE, ROW_OFFSET_SIZE, TSTAMP_SIZE, VALUE_SIZE_SIZE},
    stable_file::StableFile,
};

pub struct HintRow {
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

impl HintRow {
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

pub struct HintFile {
    database_dir: PathBuf,
    file_id: u32,
    file: File,
}

impl HintFile {
    pub fn create(database_dir: &Path, file_id: u32) -> BitcaskResult<Self> {
        let file = fs::create_file(database_dir, FileType::HintFile, Some(file_id))?;
        Ok(HintFile {
            database_dir: database_dir.to_path_buf(),
            file_id,
            file,
        })
    }

    pub fn open(database_dir: &Path, file_id: u32) -> BitcaskResult<Self> {
        let file = fs::open_file(database_dir, FileType::HintFile, Some(file_id))?;
        Ok(HintFile {
            database_dir: database_dir.to_path_buf(),
            file_id,
            file: file.file,
        })
    }

    pub fn write_file<I>(&mut self, iter: I) -> BitcaskResult<()>
    where
        I: Iterator<Item = HintRow>,
    {
        for hint in iter {
            let data_to_write = hint.to_bytes();
            self.file.write_all(&data_to_write)?;
        }
        Ok(())
    }

    pub fn iter(&self) -> BitcaskResult<HintFileIterator> {
        Ok(HintFileIterator {
            file: HintFile::open(&self.database_dir, self.file_id)?,
        })
    }

    fn read_next_hint(&mut self) -> BitcaskResult<HintRow> {
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

        Ok(HintRow {
            timestamp,
            key_size,
            value_size,
            row_offset,
            key: kv_bs.into(),
        })
    }
}

pub struct HintFileIterator {
    file: HintFile,
}

impl Iterator for HintFileIterator {
    type Item = BitcaskResult<RecoveredRow>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.file.read_next_hint() {
            Err(BitcaskError::IoError(e)) => match e.kind() {
                std::io::ErrorKind::UnexpectedEof => None,
                _ => Some(Err(BitcaskError::IoError(e))),
            },
            r => Some(r.map(|h| RecoveredRow {
                file_id: self.file.file_id,
                timestamp: h.timestamp,
                row_offset: h.row_offset,
                row_size: h.key_size + h.value_size,
                key: h.key,
                is_tombstone: false,
            })),
        }
    }
}

#[derive(Debug)]
pub struct HintFileWriter {
    sender: ManuallyDrop<Sender<u32>>,
    worker_join_handle: Option<JoinHandle<()>>,
}

impl HintFileWriter {
    pub fn start(database_dir: PathBuf) -> HintFileWriter {
        let (sender, receiver) = unbounded();

        let moved_dir = database_dir;
        let worker_join_handle = Some(thread::spawn(move || {
            while let Ok(file_id) = receiver.recv() {
                if let Err(e) = Self::write_hint_file(&moved_dir, file_id) {
                    error!(target: "Hint", "write hint file failed {}", e);
                }
            }
        }));

        HintFileWriter {
            sender: ManuallyDrop::new(sender),
            worker_join_handle,
        }
    }

    pub fn async_write_hint_file(&self, data_file_id: u32) {
        if let Err(e) = self.sender.send(data_file_id) {
            error!(target: "Database", "send file id: {} to hint file writer failed with error {}", data_file_id, e);
        }
    }

    pub fn len(&self) -> usize {
        self.sender.len()
    }

    fn write_hint_file(database_dir: &Path, data_file_id: u32) -> BitcaskResult<()> {
        let data_file = fs::open_file(database_dir, FileType::DataFile, Some(data_file_id))?;
        let stable_file = StableFile::new(database_dir, data_file_id, data_file.file, false)?;
        let data_itr = stable_file.iter()?;

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
                                timestamp: r.row_position.timestamp,
                                key_size: r.key.len(),
                                value_size: r.value.len(),
                                row_offset: r.row_position.row_offset,
                                key: r.key,
                            },
                        );
                    }
                }
                Err(e) => return Err(e),
            }
        }
        let hint_file_tmp_dir = fs::create_hint_file_tmp_dir(database_dir)?;
        let mut hint_file = HintFile::create(&hint_file_tmp_dir, data_file_id)?;
        hint_file.write_file(m.into_values().collect::<Vec<HintRow>>().into_iter())?;

        fs::commit_hint_files(database_dir, data_file_id)
    }
}

impl Drop for HintFileWriter {
    fn drop(&mut self) {
        unsafe { ManuallyDrop::drop(&mut self.sender) }
        if let Some(join_handle) = self.worker_join_handle.take() {
            if join_handle.join().is_err() {
                error!(target: "hint", "wait worker thread finish failed");
            }
        }
    }
}

pub fn clear_temp_hint_file_directory(database_dir: &Path) {
    if let Err(e) = fs::create_hint_file_tmp_dir(database_dir).and_then(|hint_file_tmp_dir| {
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
        error!(target: "Hint", "clear temp hint file directory failed. {}", e)
    }
}
