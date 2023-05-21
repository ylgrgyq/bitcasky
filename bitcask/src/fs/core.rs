use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use bytes::{Buf, Bytes};
use log::warn;
use walkdir::WalkDir;

use crate::{
    error::{BitcaskError, BitcaskResult},
    fs::FileType,
};

const TESTING_DIRECTORY: &str = "Testing";
const MERGE_FILES_DIRECTORY: &str = "Merge";

pub struct IdentifiedFile {
    pub file_type: FileType,
    pub file: File,
    pub file_id: Option<u32>,
}

pub fn check_directory_is_writable(base_dir: &Path) -> bool {
    if fs::metadata(base_dir)
        .map(|meta| meta.permissions().readonly())
        .unwrap_or(false)
    {
        return false;
    }

    // create a directory deliberately to check we have writable permission for target path
    let testing_path = base_dir.join(TESTING_DIRECTORY);
    if testing_path.exists() && !fs::remove_dir(&testing_path).map(|_| true).unwrap_or(false) {
        return false;
    }

    if !fs::create_dir(&testing_path)
        .and_then(|_| fs::remove_dir(testing_path).map(|_| true))
        .unwrap_or(false)
    {
        return false;
    }
    true
}

pub fn create_file(
    base_dir: &Path,
    file_type: FileType,
    file_id: Option<u32>,
) -> BitcaskResult<File> {
    let path = file_type.get_path(base_dir, file_id);
    Ok(File::options()
        .write(true)
        .create(true)
        .read(true)
        .open(path)?)
}

pub fn open_file(
    base_dir: &Path,
    file_type: FileType,
    file_id: Option<u32>,
) -> BitcaskResult<IdentifiedFile> {
    let path = file_type.get_path(base_dir, file_id);
    let file = File::options().read(true).open(path)?;
    Ok(IdentifiedFile {
        file_type,
        file,
        file_id,
    })
}

pub fn delete_file(
    base_dir: &Path,
    file_type: FileType,
    file_id: Option<u32>,
) -> BitcaskResult<()> {
    let path = file_type.get_path(base_dir, file_id);
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

pub fn commit_file(
    file_type: FileType,
    file_id: Option<u32>,
    from_dir: &Path,
    to_dir: &Path,
) -> Result<(), std::io::Error> {
    let from_p = file_type.get_path(from_dir, file_id);
    if from_p.exists() {
        let to_p = file_type.get_path(to_dir, file_id);
        return fs::rename(from_p, to_p);
    }
    Ok(())
}

pub fn create_dir(base_dir: &Path) -> BitcaskResult<()> {
    if !base_dir.exists() {
        std::fs::create_dir(base_dir)?;
    }

    Ok(())
}

pub fn clear_dir(base_dir: &Path) -> BitcaskResult<()> {
    fs::remove_dir_all(base_dir)?;
    Ok(())
}

pub fn merge_file_dir(base_dir: &Path) -> PathBuf {
    base_dir.join(MERGE_FILES_DIRECTORY)
}

pub fn create_merge_file_dir(base_dir: &Path) -> BitcaskResult<PathBuf> {
    let merge_dir_path = merge_file_dir(base_dir);

    if !merge_dir_path.exists() {
        fs::create_dir(merge_dir_path.clone())?;
    }

    let mut merge_dir_empty = true;
    let paths = fs::read_dir(merge_dir_path.clone())?;
    for path in paths {
        let file_path = path?;
        if file_path.path().is_dir() {
            continue;
        }
        if FileType::MergeMeta.check_file_belongs_to_type(&file_path.path()) {
            continue;
        }
        warn!(target: "File", "Merge file directory:{} is not empty, it at least has file: {}", merge_dir_path.display().to_string(), file_path.path().display());

        merge_dir_empty = false;
        break;
    }
    if !merge_dir_empty {
        let clear_ret = clear_dir(&merge_dir_path).and_then(|_| {
            fs::create_dir(merge_dir_path.clone())?;
            Ok(())
        });
        if clear_ret.is_err() {
            warn!(target: "File", "clear merge directory failed. {}", clear_ret.unwrap_err());
            return Err(BitcaskError::MergeFileDirectoryNotEmpty(
                merge_dir_path.display().to_string(),
            ));
        }
    }

    Ok(merge_dir_path)
}

pub fn commit_merge_files(base_dir: &Path, file_ids: &Vec<u32>) -> BitcaskResult<()> {
    let merge_dir_path = merge_file_dir(base_dir);
    for file_id in file_ids {
        commit_file(
            FileType::DataFile,
            Some(*file_id),
            &merge_dir_path,
            base_dir,
        )?;
        commit_file(
            FileType::HintFile,
            Some(*file_id),
            &merge_dir_path,
            base_dir,
        )?;
    }
    Ok(())
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct MergeMeta {
    pub known_max_file_id: u32,
}

pub fn read_merge_meta(merge_file_dir: &Path) -> BitcaskResult<MergeMeta> {
    let mut merge_meta_file = open_file(merge_file_dir, FileType::MergeMeta, None)?;
    let mut buf = vec![0; 4];
    merge_meta_file.file.read_exact(&mut buf)?;
    let mut bs = Bytes::from(buf);
    let known_max_file_id = bs.get_u32();
    Ok(MergeMeta { known_max_file_id })
}

pub fn write_merge_meta(merge_file_dir: &Path, merge_meta: MergeMeta) -> BitcaskResult<()> {
    let mut merge_meta_file = create_file(merge_file_dir, FileType::MergeMeta, None)?;
    merge_meta_file.write_all(&merge_meta.known_max_file_id.to_be_bytes())?;
    Ok(())
}

pub fn change_data_file_id(
    base_dir: &Path,
    from_file_id: u32,
    to_file_id: u32,
) -> BitcaskResult<()> {
    let from_p = FileType::DataFile.get_path(base_dir, Some(from_file_id));
    let to_p = FileType::DataFile.get_path(base_dir, Some(to_file_id));
    fs::rename(from_p, to_p)?;
    Ok(())
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
            FileType::DataFile => (f.file_id.unwrap(), f.file),
            _ => unreachable!(),
        })
        .collect())
}

pub fn get_valid_data_file_ids(base_dir: &Path) -> Vec<u32> {
    get_valid_data_file_paths(base_dir)
        .iter()
        .map(|p| FileType::DataFile.parse_file_id_from_file_name(p).unwrap())
        .collect()
}

pub fn purge_outdated_data_files(base_dir: &Path, max_file_id: u32) -> BitcaskResult<()> {
    get_valid_data_file_ids(base_dir)
        .iter()
        .filter(|id| **id <= max_file_id)
        .for_each(|id| {
            delete_file(base_dir, FileType::DataFile, Some(*id)).unwrap_or_default();
            delete_file(base_dir, FileType::HintFile, Some(*id)).unwrap_or_default();
        });
    Ok(())
}

fn open_data_file_by_path(file_path: &Path) -> BitcaskResult<IdentifiedFile> {
    if let Some(file_id) = FileType::DataFile.parse_file_id_from_file_name(file_path) {
        let file = File::options().read(true).open(file_path)?;
        return Ok(IdentifiedFile {
            file_type: FileType::DataFile,
            file,
            file_id: Some(file_id),
        });
    }
    let file_name = file_path
        .file_name()
        .map(|s| s.to_str().unwrap_or(""))
        .unwrap_or("");
    Err(BitcaskError::InvalidFileName(file_name.into()))
}

fn get_valid_data_file_paths(base_dir: &Path) -> Vec<PathBuf> {
    WalkDir::new(base_dir)
        .follow_links(false)
        .max_depth(1)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            if FileType::DataFile.check_file_belongs_to_type(e.path())
                && FileType::DataFile
                    .parse_file_id_from_file_name(e.path())
                    .map(|_| true)
                    .unwrap_or(false)
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
    use log::info;
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

            let id = FileType::DataFile
                .parse_file_id_from_file_name(&file_path.path())
                .unwrap();
            actual_file_ids.push(id);
        }
        actual_file_ids.sort();
        actual_file_ids
    }

    #[test]
    fn test_clear_dir() {
        let dir = get_temporary_directory_path();
        let merge_file_path = create_merge_file_dir(&dir).unwrap();
        create_file(&merge_file_path, FileType::DataFile, Some(0)).unwrap();

        clear_dir(&merge_file_path).unwrap();
        assert!(!merge_file_path.exists());
    }

    #[test]
    fn test_create_merge_file_dir() {
        let dir = get_temporary_directory_path();
        let merge_file_path = create_merge_file_dir(&dir).unwrap();
        create_file(&merge_file_path, FileType::DataFile, Some(0)).unwrap();

        create_merge_file_dir(&dir).unwrap();

        let paths = fs::read_dir(merge_file_path.clone()).unwrap();
        assert!(!paths.into_iter().any(|p| {
            let file_path = p.unwrap();
            if file_path.path().is_dir() {
                return false;
            }
            if FileType::MergeMeta.check_file_belongs_to_type(&file_path.path()) {
                return false;
            }
            return true;
        }));
    }

    #[test]
    fn test_commit_merge_files() {
        let dir_path = get_temporary_directory_path();

        let merge_file_path = create_merge_file_dir(&dir_path).unwrap();
        create_file(&merge_file_path, FileType::DataFile, Some(0)).unwrap();
        create_file(&merge_file_path, FileType::DataFile, Some(1)).unwrap();
        create_file(&merge_file_path, FileType::DataFile, Some(2)).unwrap();

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
