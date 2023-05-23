use std::{
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

fn open_file_by_path(file_type: FileType, file_path: &Path) -> BitcaskResult<IdentifiedFile> {
    if file_type.check_file_belongs_to_type(file_path) {
        let file_id = file_type.parse_file_id_from_file_name(file_path);
        let file = File::options().read(true).open(file_path)?;
        return Ok(IdentifiedFile {
            file_type,
            file,
            file_id,
        });
    }
    let file_name = file_path
        .file_name()
        .map(|s| s.to_str().unwrap_or(""))
        .unwrap_or("");
    Err(BitcaskError::InvalidFileName(file_name.into()))
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

pub fn change_file_id(
    base_dir: &Path,
    file_type: FileType,
    from_file_id: u32,
    to_file_id: u32,
) -> BitcaskResult<()> {
    let from_p = file_type.get_path(base_dir, Some(from_file_id));
    let to_p = file_type.get_path(base_dir, Some(to_file_id));
    fs::rename(from_p, to_p)?;
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

pub fn get_file_ids_in_dir(dir_path: &Path, file_type: FileType) -> Vec<u32> {
    let mut actual_file_ids = vec![];
    for path in fs::read_dir(dir_path).unwrap() {
        let file_dir_entry = path.unwrap();
        let file_path = file_dir_entry.path();
        if file_path.is_dir() {
            continue;
        }
        if !file_type.check_file_belongs_to_type(&file_path) {
            continue;
        }

        let id = file_type.parse_file_id_from_file_name(&file_path).unwrap();
        actual_file_ids.push(id);
    }
    actual_file_ids.sort();
    actual_file_ids
}

pub fn get_file_paths_in_dir(base_dir: &Path, file_type: &FileType) -> Vec<PathBuf> {
    WalkDir::new(base_dir)
        .follow_links(false)
        .max_depth(1)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            if file_type.check_file_belongs_to_type(e.path()) {
                Some(e.into_path())
            } else {
                None
            }
        })
        .collect()
}

pub fn open_files_in_dir(
    base_dir: &Path,
    file_type: FileType,
) -> BitcaskResult<Vec<IdentifiedFile>> {
    let file_entries = get_file_paths_in_dir(base_dir, &file_type);
    file_entries
        .iter()
        .map(|f| open_file_by_path(file_type, f))
        .collect::<BitcaskResult<Vec<IdentifiedFile>>>()
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        vec,
    };

    use super::*;
    use bitcask_tests::common::get_temporary_directory_path;
    use bytes::{Buf, Bytes, BytesMut};
    use log::info;
    use test_log::test;

    #[test]
    fn test_create_file() {
        let dir = get_temporary_directory_path();
        let file_id = Some(123);
        let file_path = FileType::DataFile.get_path(&dir, file_id);
        assert!(!file_path.exists());
        create_file(&dir, FileType::DataFile, file_id).unwrap();
        assert!(file_path.exists());
    }

    #[test]
    fn test_delete_file() {
        let dir = get_temporary_directory_path();
        let file_id = Some(123);
        let file_path = FileType::DataFile.get_path(&dir, file_id);
        assert!(!file_path.exists());
        create_file(&dir, FileType::DataFile, file_id).unwrap();
        assert!(file_path.exists());
        delete_file(&dir, FileType::DataFile, file_id).unwrap();
        assert!(!file_path.exists());
    }

    #[test]
    fn test_open_file() {
        let dir = get_temporary_directory_path();
        let file_id = Some(123);
        let mut file = create_file(&dir, FileType::DataFile, file_id).unwrap();
        let mut bs = BytesMut::with_capacity(8);
        let expect_val: u64 = 12345;
        bs.extend_from_slice(&expect_val.to_be_bytes());
        let bs = bs.freeze();
        file.write_all(&bs).unwrap();

        {
            let mut f = open_file(&dir, FileType::DataFile, file_id).unwrap();
            assert_eq!(file_id, f.file_id);
            assert_eq!(FileType::DataFile, f.file_type);

            let mut header_buf = vec![0; 8];
            f.file.read_exact(&mut header_buf).unwrap();

            let mut actual = Bytes::from(header_buf);
            assert_eq!(expect_val, actual.get_u64());
        }

        {
            let p = FileType::DataFile.get_path(&dir, file_id);
            let mut f = open_file_by_path(FileType::DataFile, &p).unwrap();
            assert_eq!(file_id, f.file_id);
            assert_eq!(FileType::DataFile, f.file_type);

            let mut header_buf = vec![0; 8];
            f.file.read_exact(&mut header_buf).unwrap();

            let mut actual = Bytes::from(header_buf);
            assert_eq!(expect_val, actual.get_u64());
        }
    }
}
