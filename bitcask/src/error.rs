use thiserror::Error;

#[derive(Error, Debug)]
pub enum BitcaskError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Permission Denied: \"{0}\"")]
    PermissionDenied(String),
    #[error("Database is broken due to previos unrecoverable error: {0}.")]
    DatabaseBroken(String),
    #[error("The parameter: \"{0}\" is invalid for reason: {1}")]
    InvalidParameter(String, String),
    #[error("Invalid bitcask file name: {0}")]
    InvalidFileName(String),
    #[error("Crc check failed on reading value with file id: {0}, offset: {1}. expect crc is: {2}, actual crc is: {3}")]
    CrcCheckFailed(u32, u64, u32, u32),
    #[error("Data file with file id {1} under path {0} corrupted. Hint: {2}")]
    DataFileCorrupted(String, u32, String),
    #[error("Read non-existent file with id {0}")]
    TargetFileIdNotFound(u32),
    #[error("Merge file directory: {0} is not empty. Maybe last merge is failed. Please remove files in this directory manually")]
    MergeFileDirectoryNotEmpty(String),
    #[error("Another merge is in progress")]
    MergeInProgress(),
    #[error("Lock directory: {0} failed. Maybe there's another process is using this directory")]
    LockDirectoryFailed(String),
    #[error("Invalid file id {0} in MergeMeta file. Min file ids in Merge directory is {1}")]
    InvalidMergeDataFile(u32, u32),
    #[error("Unknown server error: {0}")]
    UnknownServerError(String),
    #[error(transparent)]
    StorageError(#[from] crate::database::DataStorageError),
}

pub type BitcaskResult<T> = Result<T, BitcaskError>;
