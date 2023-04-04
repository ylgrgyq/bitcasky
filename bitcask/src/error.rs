use thiserror::Error;

#[derive(Error, Debug)]
pub enum BitcaskError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Permission Denied: \"{0}\"")]
    PermissionDenied(String),
    #[error("The parameter: \"{0}\" is invalid for reason: {1}")]
    InvalidParameter(String, String),
    #[error("Failed to parse database file name: {0}")]
    InvalidDatabaseFileName(String),
    #[error("Crc check failed on reading value with file id: {0}, offset: {1}. expect crc is: {2}, actual crc is: {3}")]
    CrcCheckFailed(u32, u64, u32, u32),
    #[error("Read non-existent file with id {0}")]
    TargetFileIdNotFound(u32),
    #[error("Merge file directory: {0} is not empty. Maybe last merge is failed. Please remove files in this directory manually")]
    MergeFileDirectoryNotEmpty(String),
    #[error("Another merge is in progress")]
    MergeInProgress(),
    #[error("Lock directory: {0} failed. Maybe there's another process is using this directory")]
    LockDirectoryFailed(String),
    #[error("Recover merge operation failed with reason: {0}")]
    RecoverMergeFailed(String),
}

pub type BitcaskResult<T> = Result<T, BitcaskError>;
