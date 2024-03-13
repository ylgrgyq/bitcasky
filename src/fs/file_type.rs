use core::panic;
use std::path::{Path, PathBuf};

use crate::storage_id::StorageId;

const LOCK_FILE_EXTENSION: &str = "lock";
const MERGE_META_FILE_EXTENSION: &str = "meta";
const DATA_FILE_EXTENSION: &str = "data";
const HINT_FILE_EXTENSION: &str = "hint";

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub enum FileType {
    Unknown,
    LockFile,
    MergeMeta,
    DataFile,
    HintFile,
}

impl FileType {
    pub fn get_path<P: AsRef<Path>>(&self, base_dir: P, storage_id: Option<StorageId>) -> PathBuf {
        base_dir.as_ref().join(match self {
            Self::LockFile => format!("bitcask.{}", LOCK_FILE_EXTENSION),
            Self::MergeMeta => format!("merge.{}", MERGE_META_FILE_EXTENSION),
            Self::DataFile => format!("{}.{}", storage_id.unwrap(), DATA_FILE_EXTENSION),
            Self::HintFile => format!("{}.{}", storage_id.unwrap(), HINT_FILE_EXTENSION),
            Self::Unknown => panic!("get path for unknown data type"),
        })
    }

    pub fn check_file_belongs_to_type(&self, file_path: &Path) -> bool {
        let ft = match file_path.extension() {
            None => FileType::Unknown,
            Some(os_str) => match os_str.to_str() {
                Some(LOCK_FILE_EXTENSION) => FileType::LockFile,
                Some(MERGE_META_FILE_EXTENSION) => FileType::MergeMeta,
                Some(DATA_FILE_EXTENSION) => FileType::DataFile,
                Some(HINT_FILE_EXTENSION) => FileType::HintFile,
                _ => FileType::Unknown,
            },
        };
        *self == ft
    }

    pub fn parse_storage_id_from_file_name(&self, file_path: &Path) -> Option<StorageId> {
        let binding = file_path.file_name().unwrap().to_string_lossy();
        let (storage_id_str, _) = binding.split_at(binding.len() - self.extension().len() - 1);
        match self {
            Self::LockFile => None,
            Self::MergeMeta => None,
            Self::DataFile => Some(storage_id_str),
            Self::HintFile => Some(storage_id_str),
            Self::Unknown => panic!("get path for unknown data type"),
        }
        .map(|storage_id_str| storage_id_str.parse::<StorageId>())
        .transpose()
        .unwrap_or(None)
    }

    fn extension(&self) -> &str {
        match self {
            Self::LockFile => LOCK_FILE_EXTENSION,
            Self::MergeMeta => MERGE_META_FILE_EXTENSION,
            Self::DataFile => DATA_FILE_EXTENSION,
            Self::HintFile => HINT_FILE_EXTENSION,
            Self::Unknown => panic!("get path for unknown data type"),
        }
    }
}

impl std::fmt::Display for FileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileType::Unknown => f.write_str("Unknown"),
            FileType::LockFile => f.write_str("LockFile"),
            FileType::MergeMeta => f.write_str("MergeMetaFile"),
            FileType::DataFile => f.write_str("DataFile"),
            FileType::HintFile => f.write_str("HintFile"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utilities::common::get_temporary_directory_path;

    #[test]
    fn test_file_type() {
        let dir = get_temporary_directory_path();

        let p = FileType::LockFile.get_path(&dir, None);
        assert!(FileType::LockFile.check_file_belongs_to_type(&p));
        let p = FileType::HintFile.get_path(&dir, Some(123));
        assert!(FileType::HintFile.check_file_belongs_to_type(&p));
        let p = FileType::DataFile.get_path(&dir, Some(100));
        assert!(FileType::DataFile.check_file_belongs_to_type(&p));
        let p = FileType::MergeMeta.get_path(&dir, Some(100));
        assert!(FileType::MergeMeta.check_file_belongs_to_type(&p));

        assert!(!FileType::LockFile.check_file_belongs_to_type(&dir.join("")));
        assert!(!FileType::DataFile.check_file_belongs_to_type(&dir.join("")));
        assert!(!FileType::HintFile.check_file_belongs_to_type(&dir.join("")));
        assert!(!FileType::MergeMeta.check_file_belongs_to_type(&dir.join("")));

        assert!(!FileType::LockFile.check_file_belongs_to_type(&dir.join(".abc")));
        assert!(!FileType::DataFile.check_file_belongs_to_type(&dir.join(".abc")));
        assert!(!FileType::HintFile.check_file_belongs_to_type(&dir.join(".abc")));
        assert!(!FileType::MergeMeta.check_file_belongs_to_type(&dir.join(".abc")));
    }
}
