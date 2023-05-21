use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
};

use walkdir::WalkDir;

use crate::{
    error::{BitcaskError, BitcaskResult},
    fs::FileType,
};

const TESTING_DIRECTORY: &str = "Testing";

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

pub fn is_empty_dir(dir: &Path) -> BitcaskResult<bool> {
    let paths = fs::read_dir(dir)?;

    for path in paths {
        let file_path = path?;
        if file_path.path() == dir {
            continue;
        }
        return Ok(false);
    }
    Ok(true)
}

pub fn get_data_file_ids_in_dir(dir_path: &Path) -> Vec<u32> {
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

    // #[test]
    // fn test_clear_dir() {
    //     let dir = get_temporary_directory_path();
    //     let merge_file_path = create_merge_file_dir(&dir).unwrap();
    //     create_file(&merge_file_path, FileType::DataFile, Some(0)).unwrap();

    //     clear_dir(&merge_file_path).unwrap();
    //     assert!(!merge_file_path.exists());
    // }
}
