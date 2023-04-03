use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use bytes::{Buf, Bytes};
use fs2::FileExt;
use log::{error, warn};
use walkdir::WalkDir;

use crate::error::{BitcaskError, BitcaskResult};

const TESTING_DIRECTORY: &str = "Testing";
const MERGE_FILES_DIRECTORY: &str = "Merge";
const LOCK_FILE_POSTFIX: &str = ".lock";
const MERGE_META_FILE_POSTFIX: &str = ".meta";
const DATA_FILE_POSTFIX: &str = ".data";
const HINT_FILE_POSTFIX: &str = ".hint";

pub enum FileType {
    LockFile,
    MergeMeta,
    DataFile(u32),
    HintFile(u32),
}

impl FileType {
    fn get_path(&self, base_dir: &Path) -> PathBuf {
        base_dir.join(match self {
            Self::LockFile => format!("bitcask{}", LOCK_FILE_POSTFIX),
            Self::MergeMeta => format!("merge{}", MERGE_META_FILE_POSTFIX),
            Self::DataFile(id) => format!("{}{}", id, DATA_FILE_POSTFIX),
            Self::HintFile(id) => format!("{}{}", id, HINT_FILE_POSTFIX),
        })
    }
    fn check_file_belongs_to_type(&self, file_path: &Path) -> bool {
        false
    }
}

pub struct IdentifiedFile {
    pub file_type: FileType,
    pub file: File,
}

pub fn lock_directory(base_dir: &Path) -> BitcaskResult<Option<File>> {
    fs::create_dir_all(base_dir)?;
    let p = FileType::LockFile.get_path(base_dir);
    let file = File::options()
        .write(true)
        .create(true)
        .read(true)
        .open(&p)?;
    match file.try_lock_exclusive() {
        Ok(_) => return Ok(Some(file)),
        _ => return Ok(None),
    }
}

pub fn unlock_directory(file: &File) {
    match file.unlock() {
        Ok(_) => return,
        Err(e) => error!(target: "FileManager", "Unlock directory failed with reason: {}", e),
    }
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

pub fn create_file(base_dir: &Path, file_type: FileType) -> BitcaskResult<File> {
    let path = file_type.get_path(base_dir);
    Ok(File::options()
        .write(true)
        .create(true)
        .read(true)
        .open(path)?)
}

pub fn merge_file_dir(base_dir: &Path) -> PathBuf {
    base_dir.join(MERGE_FILES_DIRECTORY)
}

pub fn create_merge_file_dir(base_dir: &Path) -> BitcaskResult<PathBuf> {
    let merge_dir_path = merge_file_dir(base_dir);

    if !merge_dir_path.exists() {
        fs::create_dir(merge_dir_path.clone())?;
    }

    let paths = fs::read_dir(merge_dir_path.clone())?;
    for path in paths {
        let file_path = path?;
        if file_path.path().is_dir() {
            continue;
        }
        if FileType::MergeMeta.check_file_belongs_to_type(&file_path.path()) {
            continue;
        }
        warn!(target: "FileManager", "Merge file directory:{} is not empty", merge_dir_path.display().to_string());
        return Err(BitcaskError::MergeFileDirectoryNotEmpty(
            file_path.path().display().to_string(),
        ));
    }
    Ok(merge_dir_path)
}

pub fn commit_merge_files(base_dir: &Path, file_ids: &Vec<u32>) -> BitcaskResult<()> {
    let merge_dir_path = merge_file_dir(base_dir);
    for file_id in file_ids {
        let from_p = FileType::DataFile(*file_id).get_path(&merge_dir_path);
        let to_p = FileType::DataFile(*file_id).get_path(&base_dir);
        fs::rename(from_p, to_p)?;
    }
    Ok(())
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub struct MergeMeta {
    pub known_max_file_id: u32,
}

pub fn read_merge_meta(merge_file_dir: &Path) -> BitcaskResult<MergeMeta> {
    let mut merge_meta_file = open_file(&merge_file_dir, FileType::MergeMeta)?;
    let mut buf = vec![0; 4];
    merge_meta_file.file.read_exact(&mut buf)?;
    let mut bs = Bytes::from(buf);
    let known_max_file_id = bs.get_u32();
    Ok(MergeMeta { known_max_file_id })
}

pub fn write_merge_meta(merge_file_dir: &Path, merge_meta: MergeMeta) -> BitcaskResult<()> {
    let mut merge_meta_file = create_file(&merge_file_dir, FileType::MergeMeta)?;
    merge_meta_file.write(&merge_meta.known_max_file_id.to_be_bytes())?;
    Ok(())
}

pub fn change_file_id(base_dir: &Path, from_file_id: u32, to_file_id: u32) -> BitcaskResult<()> {
    let from_p = FileType::DataFile(from_file_id).get_path(&base_dir);
    let to_p = FileType::DataFile(to_file_id).get_path(&base_dir);
    fs::rename(from_p, to_p)?;
    Ok(())
}

pub fn open_file(base_dir: &Path, file_type: FileType) -> BitcaskResult<IdentifiedFile> {
    let path = file_type.get_path(base_dir);
    let file = File::options().read(true).open(path)?;
    Ok(IdentifiedFile { file_type, file })
}

pub fn open_data_files_under_path(base_dir: &Path) -> BitcaskResult<HashMap<u32, File>> {
    let file_entries = get_valid_data_file_paths(base_dir);
    let db_files = file_entries
        .iter()
        .map(|f| open_data_file_by_path(f))
        .collect::<BitcaskResult<Vec<IdentifiedFile>>>()?;
    Ok(db_files
        .into_iter()
        .map(|f| match f.file_type {
            FileType::DataFile(id) => (id, f.file),
            _ => unreachable!(),
        })
        .collect())
}

pub fn get_valid_data_file_ids(base_dir: &Path) -> Vec<u32> {
    get_valid_data_file_paths(base_dir)
        .iter()
        .map(|p| parse_file_id_from_data_file(p).unwrap())
        .collect()
}

pub fn delete_file(base_dir: &Path, file_id: u32, file_type: FileType) -> BitcaskResult<()> {
    let path = file_type.get_path(base_dir);
    fs::remove_file(path)?;
    Ok(())
}

fn open_data_file_by_path(file_path: &Path) -> BitcaskResult<IdentifiedFile> {
    let file_id = parse_file_id_from_data_file(file_path)?;
    let file = File::options().read(true).open(file_path)?;
    Ok(IdentifiedFile {
        file_type: FileType::DataFile(file_id),
        file,
    })
}

fn parse_file_id_from_data_file(file_path: &Path) -> BitcaskResult<u32> {
    let binding = file_path.file_name().unwrap().to_string_lossy();
    let (file_id_str, _) = binding.split_at(binding.len() - DATA_FILE_POSTFIX.len());
    file_id_str
        .parse::<u32>()
        .map_err(|_| BitcaskError::InvalidDatabaseFileName(binding.to_string()))
}

fn get_valid_data_file_paths(base_dir: &Path) -> Vec<PathBuf> {
    WalkDir::new(base_dir)
        .follow_links(false)
        .max_depth(1)
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

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use bitcask_tests::common::get_temporary_directory_path;
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
        let dir = get_temporary_directory_path();
        let merge_file_path = create_merge_file_dir(&dir).unwrap();
        create_file(&merge_file_path, FileType::DataFile(0)).unwrap();

        let failed_to_create = create_merge_file_dir(&dir);
        assert!(match failed_to_create.err().unwrap() {
            BitcaskError::MergeFileDirectoryNotEmpty(_) => true,
            _ => false,
        });
    }

    #[test]
    fn test_commit_merge_files() {
        let dir_path = get_temporary_directory_path();

        let merge_file_path = create_merge_file_dir(&dir_path).unwrap();
        create_file(&merge_file_path, FileType::DataFile(0)).unwrap();
        create_file(&merge_file_path, FileType::DataFile(1)).unwrap();
        create_file(&merge_file_path, FileType::DataFile(2)).unwrap();

        assert_eq!(vec![0, 1, 2,], get_data_file_ids_in_dir(&merge_file_path));
        assert!(get_data_file_ids_in_dir(&dir_path).is_empty());

        commit_merge_files(&dir_path, &vec![0, 1, 2]).unwrap();

        assert!(is_empty_dir(&merge_file_path).unwrap());

        assert_eq!(vec![0, 1, 2,], get_data_file_ids_in_dir(&dir_path));
    }

    #[test]
    fn test_read_write_merge_meta() {
        let dir_path = get_temporary_directory_path();
        let merge_file_path = create_merge_file_dir(&dir_path).unwrap();
        let expect_meta = MergeMeta {
            known_max_file_id: 10101,
        };
        write_merge_meta(&merge_file_path, expect_meta).unwrap();
        let actual_meta = read_merge_meta(&merge_file_path).unwrap();
        assert_eq!(expect_meta, actual_meta);
    }
}
