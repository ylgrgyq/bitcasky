use mockall::mock;

use std::{fs::File, path::PathBuf};

use crate::error::BitcaskResult;

use super::common::{RowPosition, RowToWrite};

mock! {
    #[derive(Debug)]
    pub WritingFile {
        pub fn new(database_dir: &PathBuf, file_id: u32) -> BitcaskResult<Self>;
        pub fn file_id(&self) -> u32;

        pub fn file_size(&self) -> usize;

        pub fn write_row<'a>(&mut self, row: RowToWrite<'a>) -> BitcaskResult<RowPosition>;

        pub fn read_value(&mut self, value_offset: u64, size: usize) -> BitcaskResult<Vec<u8>>;
        pub fn transit_to_readonly(mut self) -> BitcaskResult<(u32, File)> ;
        pub fn flush(&mut self) -> BitcaskResult<()> ;
    }
}
