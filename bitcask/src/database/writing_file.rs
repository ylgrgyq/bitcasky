use std::{
    fs::File,
    io::{Seek, SeekFrom, Write},
    ops::Deref,
    path::{Path, PathBuf},
};

use crate::{
    error::BitcaskResult,
    file_id::FileId,
    fs::{create_file, FileType},
};

use super::common::{
    io_error_to_bitcask_error, read_value_from_file, RowLocation, RowToWrite, TimedValue,
};

#[derive(Debug)]
pub struct WritingFile {
    database_dir: PathBuf,
    file_id: FileId,
    data_file: File,
    file_size: usize,
}

impl WritingFile {
    pub fn new(database_dir: &Path, file_id: FileId) -> BitcaskResult<Self> {
        let data_file = create_file(database_dir, FileType::DataFile, Some(file_id))?;
        Ok(WritingFile {
            database_dir: database_dir.to_path_buf(),
            file_id,
            data_file,
            file_size: 0,
        })
    }

    pub fn file_id(&self) -> FileId {
        self.file_id
    }

    pub fn file_size(&self) -> usize {
        self.file_size
    }

    pub fn write_row<V: Deref<Target = [u8]>>(
        &mut self,
        row: RowToWrite<V>,
    ) -> BitcaskResult<RowLocation> {
        let value_offset = self.data_file.seek(SeekFrom::End(0))?;
        let data_to_write = row.to_bytes();
        self.data_file.write_all(&data_to_write).map_err(|e| {
            io_error_to_bitcask_error(
                &self.database_dir,
                self.file_id,
                e,
                "Write data file may partially failed",
            )
        })?;

        self.file_size += data_to_write.len();
        Ok(RowLocation {
            file_id: self.file_id,
            row_offset: value_offset,
            row_size: row.size,
        })
    }

    pub fn read_value(&mut self, value_offset: u64, size: u64) -> BitcaskResult<TimedValue> {
        read_value_from_file(self.file_id, &mut self.data_file, value_offset, size)
    }

    pub fn transit_to_readonly(mut self) -> BitcaskResult<(FileId, File)> {
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
