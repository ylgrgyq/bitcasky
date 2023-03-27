use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
};

use log::{info, warn};
use walkdir::WalkDir;

use crate::error::{BitcaskError, BitcaskResult};

const TESTING_DIRECTORY: &str = "Testing";
const MERGE_FILES_DIRECTORY: &str = "Merge";
const DATA_FILE_POSTFIX: &str = ".data";
const HINT_FILE_POSTFIX: &str = ".hint";

pub enum FileType {
    DataFile,
    HintFile,
}

impl FileType {
    fn generate_name(&self, base_dir: &Path, file_id: u32) -> PathBuf {
        base_dir.join(format!(
            "{}{}",
            file_id,
            match self {
                Self::DataFile => DATA_FILE_POSTFIX,
                Self::HintFile => HINT_FILE_POSTFIX,
            },
        ))
    }
}

pub struct IdentifiedFile {
    pub file_id: u32,
    pub file: File,
}

pub fn check_directory_is_writable(base_dir: &Path) -> bool {
    if fs::metadata(base_dir)
        .and_then(|meta| Ok(meta.permissions().readonly()))
        .unwrap_or(false)
    {
        return false;
    }

    // create a directory deliberately to check we have writable permission for target path
    let testing_path = base_dir.join(TESTING_DIRECTORY);
    if testing_path.exists() {
        if !fs::remove_dir(&testing_path)
            .and_then(|_| Ok(true))
            .unwrap_or(false)
        {
            return false;
        }
    }

    if !fs::create_dir(&testing_path)
        .and_then(|_| fs::remove_dir(testing_path).and_then(|_| Ok(true)))
        .unwrap_or(false)
    {
        return false;
    }
    true
}

pub fn create_file(base_dir: &Path, file_id: u32, file_type: FileType) -> BitcaskResult<File> {
    let path = file_type.generate_name(base_dir, file_id);
    Ok(File::options()
        .write(true)
        .create(true)
        .read(true)
        .open(path)?)
}

pub fn create_merge_file_dir(base_dir: &Path) -> BitcaskResult<PathBuf> {
    let merge_dir_path = base_dir.join(MERGE_FILES_DIRECTORY);

    if !merge_dir_path.exists() {
        fs::create_dir(merge_dir_path.clone())?;
    }

    let paths = fs::read_dir(merge_dir_path.clone())?;
    for path in paths {
        let file_path = path?;
        if file_path.path().is_dir() {
            continue;
        }
        warn!(target: "FileManager", "Merge file directory:{} is not empty", merge_dir_path.display().to_string());
        return Err(BitcaskError::MergeFileDirectoryNotEmpty(
            file_path.path().display().to_string(),
        ));
    }
    Ok(merge_dir_path)
}

pub fn commit_merge_files(base_dir: &Path) -> BitcaskResult<()> {
    let merge_dir_path = base_dir.join(MERGE_FILES_DIRECTORY);
    let paths = fs::read_dir(merge_dir_path.clone())?;
    for path in paths {
        let file_path = path?;
        if file_path.path().is_dir() {
            continue;
        }
        let target_path = base_dir.join(file_path.file_name());
        fs::rename(file_path.path(), target_path)?;
    }

    Ok(())
}

pub fn open_file(
    base_dir: &Path,
    file_id: u32,
    file_type: FileType,
) -> BitcaskResult<IdentifiedFile> {
    let path = file_type.generate_name(base_dir, file_id);
    let file = File::options().read(true).open(path)?;
    Ok(IdentifiedFile { file_id, file })
}

pub fn open_data_files_under_path(base_dir: &Path) -> BitcaskResult<HashMap<u32, File>> {
    let file_entries = get_valid_data_file_paths(base_dir);
    let db_files = file_entries
        .iter()
        .map(|f| open_file_by_path(f))
        .collect::<BitcaskResult<Vec<IdentifiedFile>>>()?;
    Ok(db_files.into_iter().map(|f| (f.file_id, f.file)).collect())
}

fn open_file_by_path(file_path: &Path) -> BitcaskResult<IdentifiedFile> {
    let file_id = parse_file_id_from_data_file(file_path)?;
    let file = File::options().read(true).open(file_path)?;
    Ok(IdentifiedFile { file_id, file })
}

fn get_valid_data_file_paths(base_dir: &Path) -> Vec<PathBuf> {
    WalkDir::new(base_dir)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let file_name = e.file_name().to_string_lossy();
            if file_name.ends_with(DATA_FILE_POSTFIX)
                && parse_file_id_from_data_file(e.path()).is_ok()
            {
                Some(e.into_path())
            } else {
                None
            }
        })
        .collect()
}

fn parse_file_id_from_data_file(file_path: &Path) -> BitcaskResult<u32> {
    let binding = file_path.file_name().unwrap().to_string_lossy();
    let (file_id_str, _) = binding.split_at(binding.len() - DATA_FILE_POSTFIX.len());
    file_id_str
        .parse::<u32>()
        .map_err(|_| BitcaskError::InvalidDatabaseFileName(binding.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    fn is_empty_dir(dir: &Path) -> BitcaskResult<bool> {
        let paths = fs::read_dir(dir.clone())?;

        for path in paths {
            let file_path = path?;
            if file_path.path() == dir {
                continue;
            }
            return Ok(false);
        }
        Ok(true)
    }
    fn get_data_file_ids_in_dir(dir_path: &Path) -> Vec<u32> {
        let mut actual_file_ids = vec![];
        for path in fs::read_dir(dir_path).unwrap() {
            let file_path = path.unwrap();
            if file_path.path().is_dir() {
                continue;
            }

            let id = parse_file_id_from_data_file(&file_path.path()).unwrap();
            actual_file_ids.push(id);
        }
        actual_file_ids.sort();
        actual_file_ids
    }

    #[test]
    fn test_create_merge_file_dir() {
        let dir = tempfile::tempdir().unwrap();
        let merge_file_path = create_merge_file_dir(&dir.path()).unwrap();
        create_file(&merge_file_path, 0, FileType::DataFile).unwrap();

        let failed_to_create = create_merge_file_dir(&dir.path());
        assert!(match failed_to_create.err().unwrap() {
            BitcaskError::MergeFileDirectoryNotEmpty(_) => true,
            _ => false,
        });
    }

    #[test]
    fn test_commit_merge_files() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();

        let merge_file_path = create_merge_file_dir(&dir_path).unwrap();
        create_file(&merge_file_path, 0, FileType::DataFile).unwrap();
        create_file(&merge_file_path, 1, FileType::DataFile).unwrap();
        create_file(&merge_file_path, 2, FileType::DataFile).unwrap();

        assert_eq!(vec![0, 1, 2,], get_data_file_ids_in_dir(&merge_file_path));
        assert!(get_data_file_ids_in_dir(dir_path).is_empty());

        commit_merge_files(&dir_path).unwrap();

        assert!(is_empty_dir(&merge_file_path).unwrap());

        assert_eq!(vec![0, 1, 2,], get_data_file_ids_in_dir(dir_path));
    }
}
