use thiserror::Error;

#[derive(Error, Debug)]
pub enum BitcaskError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Failed to parse database file name: {0}")]
    InvalidDatabaseFileName(String),
    #[error("Crc check failed on reading value with file id: {0}, offset: {1}. expect crc is: {2}, actual crc is: {3}")]
    CrcCheckFailed(u32, u64, u32, u32),
    #[error("Read non-existent file with id {0}")]
    TargetFileIdNotFound(u32),
}

pub type BitcaskResult<T> = Result<T, BitcaskError>;
