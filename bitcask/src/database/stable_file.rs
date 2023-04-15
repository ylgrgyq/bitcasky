use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    vec,
};

use bytes::{Buf, Bytes};
use crc::{Crc, CRC_32_CKSUM};

use crate::{
    error::{BitcaskError, BitcaskResult},
    file_manager::{self, FileType},
};
use log::error;

use super::{
    common::{io_error_to_bitcask_error, read_value_from_file, RowPosition, RowToRead},
    constants::{
        DATA_FILE_KEY_OFFSET, DATA_FILE_KEY_SIZE_OFFSET, DATA_FILE_TSTAMP_OFFSET,
        DATA_FILE_VALUE_SIZE_OFFSET, KEY_SIZE_SIZE, VALUE_SIZE_SIZE,
    },
};

#[derive(Debug)]
pub struct StableFile {
    database_dir: PathBuf,
    pub file_id: u32,
    file: File,
    pub file_size: u64,
    tolerate_data_file_corrption: bool,
}

impl StableFile {
    pub fn new(
        database_dir: &Path,
        file_id: u32,
        file: File,
        tolerate_data_file_corrption: bool,
    ) -> BitcaskResult<Self> {
        let meta = file.metadata()?;
        Ok(StableFile {
            database_dir: database_dir.to_path_buf(),
            file_id,
            file,
            file_size: meta.len(),
            tolerate_data_file_corrption,
        })
    }

    pub fn read_value(&mut self, value_offset: u64, size: usize) -> BitcaskResult<Vec<u8>> {
        read_value_from_file(self.file_id, &mut self.file, value_offset, size)
    }

    fn read_next_row(&mut self) -> BitcaskResult<Option<RowToRead>> {
        let value_offset = self.file.seek(SeekFrom::Current(0))?;
        if value_offset >= self.file_size {
            return Ok(None);
        }

        let mut header_buf = vec![0; DATA_FILE_KEY_OFFSET];
        self.file.read_exact(&mut header_buf).map_err(|e| {
            io_error_to_bitcask_error(
                &self.database_dir,
                self.file_id,
                e,
                "Read data header got unexpected EOF",
            )
        })?;

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
        self.file.read_exact(&mut kv_buf).map_err(|e| {
            io_error_to_bitcask_error(
                &self.database_dir,
                self.file_id,
                e,
                "Read data got unexpected EOF",
            )
        })?;

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

        Ok(Some(RowToRead {
            key: kv_bs.slice(0..key_size).into(),
            value: kv_bs.slice(key_size..).into(),
            row_position: RowPosition {
                file_id: self.file_id,
                row_offset: value_offset,
                row_size: DATA_FILE_KEY_OFFSET + key_size + value_size,
                tstmp,
            },
        }))
    }

    pub fn iter(&self) -> BitcaskResult<StableFileIter> {
        let file =
            file_manager::open_file(&self.database_dir, FileType::DataFile, Some(self.file_id))?;
        let stable_file = StableFile::new(
            &self.database_dir,
            self.file_id,
            file.file,
            self.tolerate_data_file_corrption,
        )?;
        Ok(StableFileIter { stable_file })
    }
}

#[derive(Debug)]
pub struct StableFileIter {
    stable_file: StableFile,
}

impl Iterator for StableFileIter {
    type Item = BitcaskResult<RowToRead>;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.stable_file.read_next_row();
        match ret {
            Ok(o) => o.map(Ok),
            Err(e) => {
                if let BitcaskError::DataFileCorrupted(dir, file_id, hint) = &e {
                    if self.stable_file.tolerate_data_file_corrption {
                        error!(target: "Database", "Tolerate corrupted data file with file id {} under path {} corrupted. Corrupted hint: {}", file_id, dir, hint);
                        return None;
                    }
                }
                Some(Err(e))
            }
        }
    }
}
