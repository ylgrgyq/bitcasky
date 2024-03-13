use crate::database::DatabaseError;
use thiserror::Error;

use crate::formatter::FormatterError;

#[derive(Error, Debug)]
pub enum BitcaskyError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Permission Denied: \"{0}\"")]
    PermissionDenied(String),
    #[error("The parameter: \"{0}\" is invalid for reason: {1}")]
    InvalidParameter(String, String),
    #[error("Found corrupted merge meta file under file directory: {1}")]
    MergeMetaFileCorrupted(#[source] FormatterError, String),
    #[error("Merge file directory: {0} is not empty. Maybe last merge is failed. Please remove files in this directory manually")]
    MergeFileDirectoryNotEmpty(String),
    #[error("Another merge is in progress")]
    MergeInProgress(),
    #[error("Invalid file id {0} in MergeMeta file. Min file ids in Merge directory is {1}")]
    InvalidMergeDataFile(u32, u32),
    #[error("Lock directory: {0} failed. Maybe there's another process is using this directory")]
    LockDirectoryFailed(String),
    #[error(transparent)]
    DatabaseError(#[from] DatabaseError),
}

pub type BitcaskyResult<T> = Result<T, BitcaskyError>;
