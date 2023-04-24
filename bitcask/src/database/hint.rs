use std::{
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    thread, vec,
};

use bytes::{Buf, Bytes, BytesMut};
use log::error;

use crate::{
    error::{BitcaskError, BitcaskResult},
    fs::{self, FileType},
};
use crossbeam_channel::{unbounded, Sender};

use super::{
    constants::{KEY_SIZE_SIZE, ROW_OFFSET_SIZE, TSTAMP_SIZE, VALUE_SIZE_SIZE},
    stable_file::StableFile,
};

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

pub struct HintFile {
    database_dir: PathBuf,
    file_id: u32,
    file: File,
}

impl HintFile {
    pub fn new(database_dir: &Path, file_id: u32, file: File) -> Self {
        HintFile {
            database_dir: database_dir.to_path_buf(),
            file_id,
            file,
        }
    }

    pub fn write_file(
        &mut self,
        iter: Box<dyn Iterator<Item = BitcaskResult<RowHint>>>,
    ) -> BitcaskResult<()> {
        let hints: BitcaskResult<Vec<RowHint>> = iter.collect();
        for hint in hints? {
            let data_to_write = hint.to_bytes();
            self.file.write_all(&data_to_write)?;
        }
        Ok(())
    }

    fn iter(&self) -> BitcaskResult<HintFileIterator> {
        let file = fs::open_file(&self.database_dir, FileType::HintFile, Some(self.file_id))?;
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
                std::io::ErrorKind::UnexpectedEof => None,
                _ => Some(Err(BitcaskError::IoError(e))),
            },
            r => Some(r),
        }
    }
}

pub struct HintFileWriter {
    database_dir: PathBuf,
}

impl HintFileWriter {
    pub fn start(base_dir: PathBuf) -> Sender<u32> {
        let (sender, receiver) = unbounded();

        let hint_writer = HintFileWriter {
            database_dir: base_dir,
        };

        thread::spawn(move || loop {
            match receiver.recv() {
                Ok(file_id) => {
                    if let Err(e) = hint_writer.write_hint_file(file_id) {
                        error!(target: "Hint", "write hint file failed {}", e);
                    }
                }
                Err(_) => {
                    return;
                }
            }
        });

        sender
    }

    pub fn write_hint_file(&self, data_file_id: u32) -> BitcaskResult<()> {
        let data_file = fs::open_file(&self.database_dir, FileType::DataFile, Some(data_file_id))?;
        let stable_file = StableFile::new(&self.database_dir, data_file_id, data_file.file, false)?;
        let data_itr = stable_file.iter()?;
        let hint_itr = data_itr.map(|ret| {
            ret.map(|r| RowHint {
                timestamp: r.row_position.tstmp,
                key_size: r.key.len(),
                value_size: r.value.len(),
                row_offset: r.row_position.row_offset,
                key: r.key,
            })
        });

        let hint_file_tmp_dir = fs::create_hint_file_tmp_dir(&self.database_dir)?;
        let f = fs::create_file(&hint_file_tmp_dir, FileType::HintFile, Some(data_file_id))?;
        let mut hint_file = HintFile::new(&self.database_dir, data_file_id, f);
        hint_file.write_file(Box::new(hint_itr))?;

        fs::commit_hint_files(&self.database_dir, data_file_id)
    }
}
