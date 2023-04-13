use std::{
    fs::File,
    io::{Seek, SeekFrom, Write},
    path::PathBuf,
};

use crate::{
    error::BitcaskResult,
    file_manager::{create_file, FileType},
};

use super::common::{io_error_to_bitcask_error, read_value_from_file, RowPosition, RowToWrite};

#[derive(Debug)]
pub struct WritingFile {
    database_dir: PathBuf,
    file_id: u32,
    data_file: File,
    file_size: usize,
}

impl WritingFile {
    pub fn new(database_dir: &PathBuf, file_id: u32) -> BitcaskResult<Self> {
        let data_file = create_file(&database_dir, FileType::DataFile, Some(file_id))?;
        Ok(WritingFile {
            database_dir: database_dir.clone(),
            file_id,
            data_file,
            file_size: 0,
        })
    }

    pub fn file_id(&self) -> u32 {
        self.file_id
    }

    pub fn file_size(&self) -> usize {
        self.file_size
    }

    pub fn write_row<'a>(&mut self, row: RowToWrite<'a>) -> BitcaskResult<RowPosition> {
        let value_offset = self.data_file.seek(SeekFrom::End(0))?;
        let data_to_write = row.to_bytes();
        self.data_file.write_all(&*data_to_write).map_err(|e| {
            io_error_to_bitcask_error(
                &self.database_dir,
                self.file_id,
                e,
                "Write data file may partially failed",
            )
        })?;

        self.file_size += data_to_write.len();
        Ok(RowPosition {
            file_id: self.file_id,
            row_offset: value_offset,
            row_size: row.size,
            tstmp: row.tstamp,
        })
    }

    pub fn read_value(&mut self, value_offset: u64, size: usize) -> BitcaskResult<Vec<u8>> {
        read_value_from_file(self.file_id, &mut self.data_file, value_offset, size)
    }

    pub fn transit_to_readonly(mut self) -> BitcaskResult<(u32, File)> {
        self.data_file.flush()?;
        let file_id = self.file_id;
        let mut perms = self.data_file.metadata()?.permissions();
        perms.set_readonly(true);
        self.data_file.set_permissions(perms)?;
        Ok((file_id, self.data_file))
    }

    pub fn flush(&mut self) -> BitcaskResult<()> {
        Ok(self.data_file.flush()?)
    }
}
