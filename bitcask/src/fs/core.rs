use std::{
    fs::{self, File},
    io::Result,
    path::Path,
};

use crate::{file_id::FileId, fs::FileType};

const TESTING_DIRECTORY: &str = "Testing";

pub struct IdentifiedFile {
    pub file_type: FileType,
    pub file: File,
    pub file_id: Option<FileId>,
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
    file_id: Option<FileId>,
) -> std::io::Result<File> {
    let path = file_type.get_path(base_dir, file_id);
    File::options()
        .write(true)
        .create(true)
        .read(true)
        .open(path)
}

pub fn open_file(
    base_dir: &Path,
    file_type: FileType,
    file_id: Option<FileId>,
) -> std::io::Result<IdentifiedFile> {
    let path = file_type.get_path(base_dir, file_id);
    let file = File::options().read(true).open(path)?;
    Ok(IdentifiedFile {
        file_type,
        file,
        file_id,
    })
}

pub fn delete_file(base_dir: &Path, file_type: FileType, file_id: Option<FileId>) -> Result<()> {
    let path = file_type.get_path(base_dir, file_id);
    if path.exists() {
        fs::remove_file(path)?;
    }
    Ok(())
}

pub fn commit_file(
    file_type: FileType,
    file_id: Option<FileId>,
    from_dir: &Path,
    to_dir: &Path,
) -> Result<()> {
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
    from_file_id: FileId,
    to_file_id: FileId,
) -> Result<()> {
    let from_p = file_type.get_path(base_dir, Some(from_file_id));
    let to_p = file_type.get_path(base_dir, Some(to_file_id));
    fs::rename(from_p, to_p)?;
    Ok(())
}

pub fn create_dir(base_dir: &Path) -> Result<()> {
    if !base_dir.exists() {
        std::fs::create_dir(base_dir)?;
    }

    Ok(())
}

pub fn delete_dir(base_dir: &Path) -> Result<()> {
    fs::remove_dir_all(base_dir)?;
    Ok(())
}

pub fn get_file_ids_in_dir(dir_path: &Path, file_type: FileType) -> Vec<FileId> {
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

// used by some tests
#[allow(dead_code)]
pub fn is_empty_dir(dir: &Path) -> Result<bool> {
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

#[cfg(test)]
mod tests {
    use std::{
        io::{ErrorKind, Read, Result, Write},
        vec,
    };

    use super::*;
    use bitcask_tests::common::get_temporary_directory_path;
    use bytes::{Buf, Bytes, BytesMut};
    use test_log::test;

    fn open_file_by_path(file_type: FileType, file_path: &Path) -> Result<IdentifiedFile> {
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
        Err(std::io::Error::new(
            ErrorKind::InvalidInput,
            format!("invalid file name: {}", file_name),
        ))
    }

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

    #[test]
    fn test_commit_file() {
        let from_dir = get_temporary_directory_path();
        let to_dir = get_temporary_directory_path();
        let file_id = Some(123);
        create_file(&from_dir, FileType::DataFile, file_id).unwrap();
        assert!(FileType::DataFile.get_path(&from_dir, file_id).exists());
        commit_file(FileType::DataFile, file_id, &from_dir, &to_dir).unwrap();
        assert!(!FileType::DataFile.get_path(&from_dir, file_id).exists());
        assert!(FileType::DataFile.get_path(&to_dir, file_id).exists());
    }

    #[test]
    fn test_change_file_id() {
        let dir = get_temporary_directory_path();
        let file_id = 123;
        let new_file_id = 456;
        create_file(&dir, FileType::DataFile, Some(file_id)).unwrap();
        assert!(FileType::DataFile.get_path(&dir, Some(file_id)).exists());
        change_file_id(&dir, FileType::DataFile, file_id, new_file_id).unwrap();
        assert!(!FileType::DataFile.get_path(&dir, Some(file_id)).exists());
        assert!(FileType::DataFile
            .get_path(&dir, Some(new_file_id))
            .exists());
    }

    #[test]
    fn test_create_dir() {
        let dir = get_temporary_directory_path().join(TESTING_DIRECTORY);
        assert!(!dir.exists());
        create_dir(&dir).unwrap();
        assert!(dir.exists());
    }

    #[test]
    fn test_delete_dir() {
        let dir = get_temporary_directory_path().join(TESTING_DIRECTORY);
        create_dir(&dir).unwrap();
        create_file(&dir, FileType::DataFile, Some(1230)).unwrap();
        assert!(dir.exists());
        delete_dir(&dir).unwrap();
        assert!(!dir.exists());
    }

    #[test]
    fn test_is_empty_dir() {
        let dir = get_temporary_directory_path().join(TESTING_DIRECTORY);
        create_dir(&dir).unwrap();
        assert!(is_empty_dir(&dir).unwrap());
        create_file(&dir, FileType::DataFile, Some(1230)).unwrap();
        assert!(!is_empty_dir(&dir).unwrap());
    }

    #[test]
    fn test_get_file_ids_in_dir() {
        let dir = get_temporary_directory_path();
        create_file(&dir, FileType::DataFile, Some(103)).unwrap();
        create_file(&dir, FileType::HintFile, Some(100)).unwrap();
        create_file(&dir, FileType::DataFile, Some(102)).unwrap();
        create_file(&dir, FileType::DataFile, Some(101)).unwrap();
        let file_ids = get_file_ids_in_dir(&dir, FileType::DataFile);
        assert_eq!(vec![101, 102, 103], file_ids);
    }
}
