use std::{
    fs::{self, File},
    io::Result,
    path::{Path, PathBuf},
};

use log::debug;

use crate::common::{fs::FileType, storage_id::StorageId};

const TESTING_DIRECTORY: &str = "Testing";

pub struct IdentifiedFile {
    pub file_type: FileType,
    pub file: File,
    pub storage_id: Option<StorageId>,
    pub path: PathBuf,
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

pub fn create_file<P: AsRef<Path>>(
    base_dir: P,
    file_type: FileType,
    storage_id: Option<StorageId>,
) -> std::io::Result<File> {
    let path = file_type.get_path(base_dir, storage_id);
    File::options()
        .write(true)
        .create(true)
        .read(true)
        .open(path)
}

pub fn open_file<P: AsRef<Path>>(
    base_dir: P,
    file_type: FileType,
    storage_id: Option<StorageId>,
) -> std::io::Result<IdentifiedFile> {
    let path = file_type.get_path(base_dir, storage_id);
    let file = if std::fs::metadata(&path)?.permissions().readonly() {
        File::options().read(true).open(&path)?
    } else {
        File::options().read(true).write(true).open(&path)?
    };
    Ok(IdentifiedFile {
        file_type,
        file,
        storage_id,
        path,
    })
}

pub fn delete_file(
    base_dir: &Path,
    file_type: FileType,
    storage_id: Option<StorageId>,
) -> Result<()> {
    let path = file_type.get_path(base_dir, storage_id);
    if path.exists() {
        fs::remove_file(path)?;
        if let Some(id) = storage_id {
            debug!(
                "Delete {} type file with id: {} under path: {:?}",
                file_type, id, base_dir
            )
        } else {
            debug!("Delete {} type file under path: {:?}", file_type, base_dir)
        }
    }
    Ok(())
}

pub fn move_file(
    file_type: FileType,
    storage_id: Option<StorageId>,
    from_dir: &Path,
    to_dir: &Path,
) -> Result<()> {
    let from_p = file_type.get_path(from_dir, storage_id);
    if from_p.exists() {
        let to_p = file_type.get_path(to_dir, storage_id);
        return fs::rename(from_p, to_p);
    }
    Ok(())
}

pub fn truncate_file(file: &mut File, capacity: usize) -> std::io::Result<()> {
    // fs4 provides some cross-platform bindings which help for Windows.
    #[cfg(not(unix))]
    file.allocate(capacity as u64)?;
    // For all unix systems we can just use ftruncate directly
    #[cfg(unix)]
    {
        rustix::fs::ftruncate(file, capacity as u64)?;
    }
    Ok(())
}

pub fn change_storage_id(
    base_dir: &Path,
    file_type: FileType,
    from_storage_id: StorageId,
    to_storage_id: StorageId,
) -> Result<()> {
    debug!(
        "Change file id from {} to {}",
        from_storage_id, to_storage_id
    );
    let from_p = file_type.get_path(base_dir, Some(from_storage_id));
    let to_p = file_type.get_path(base_dir, Some(to_storage_id));
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

pub fn get_storage_ids_in_dir(dir_path: &Path, file_type: FileType) -> Vec<StorageId> {
    let mut actual_storage_ids = vec![];
    for path in fs::read_dir(dir_path).unwrap() {
        let file_dir_entry = path.unwrap();
        let file_path = file_dir_entry.path();
        if file_path.is_dir() {
            continue;
        }
        if !file_type.check_file_belongs_to_type(&file_path) {
            continue;
        }

        let id = file_type
            .parse_storage_id_from_file_name(&file_path)
            .unwrap();
        actual_storage_ids.push(id);
    }
    actual_storage_ids.sort();
    actual_storage_ids
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
    use crate::utilities::common::get_temporary_directory_path;
    use bytes::{Buf, Bytes, BytesMut};
    use test_log::test;

    fn open_file_by_path(file_type: FileType, file_path: &Path) -> Result<IdentifiedFile> {
        if file_type.check_file_belongs_to_type(file_path) {
            let storage_id = file_type.parse_storage_id_from_file_name(file_path);
            let file = File::options().read(true).open(file_path)?;
            return Ok(IdentifiedFile {
                file_type,
                file,
                storage_id,
                path: file_path.to_path_buf(),
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
        let storage_id = Some(123);
        let file_path = FileType::DataFile.get_path(&dir, storage_id);
        assert!(!file_path.exists());
        create_file(&dir, FileType::DataFile, storage_id).unwrap();
        assert!(file_path.exists());
    }

    #[test]
    fn test_delete_file() {
        let dir = get_temporary_directory_path();
        let storage_id = Some(123);
        let file_path = FileType::DataFile.get_path(&dir, storage_id);
        assert!(!file_path.exists());
        create_file(&dir, FileType::DataFile, storage_id).unwrap();
        assert!(file_path.exists());
        delete_file(&dir, FileType::DataFile, storage_id).unwrap();
        assert!(!file_path.exists());
    }

    #[test]
    fn test_open_file() {
        let dir = get_temporary_directory_path();
        let storage_id = Some(123);
        let mut file = create_file(&dir, FileType::DataFile, storage_id).unwrap();
        let mut bs = BytesMut::with_capacity(8);
        let expect_val: u64 = 12345;
        bs.extend_from_slice(&expect_val.to_be_bytes());
        let bs = bs.freeze();
        file.write_all(&bs).unwrap();

        {
            let mut f = open_file(&dir, FileType::DataFile, storage_id).unwrap();
            assert_eq!(storage_id, f.storage_id);
            assert_eq!(FileType::DataFile, f.file_type);

            let mut header_buf = vec![0; 8];
            f.file.read_exact(&mut header_buf).unwrap();

            let mut actual = Bytes::from(header_buf);
            assert_eq!(expect_val, actual.get_u64());
        }

        {
            let p = FileType::DataFile.get_path(&dir, storage_id);
            let mut f = open_file_by_path(FileType::DataFile, &p).unwrap();
            assert_eq!(storage_id, f.storage_id);
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
        let storage_id = Some(123);
        create_file(&from_dir, FileType::DataFile, storage_id).unwrap();
        assert!(FileType::DataFile.get_path(&from_dir, storage_id).exists());
        move_file(FileType::DataFile, storage_id, &from_dir, &to_dir).unwrap();
        assert!(!FileType::DataFile.get_path(&from_dir, storage_id).exists());
        assert!(FileType::DataFile.get_path(&to_dir, storage_id).exists());
    }

    #[test]
    fn test_change_storage_id() {
        let dir = get_temporary_directory_path();
        let storage_id = 123;
        let new_storage_id = 456;
        create_file(&dir, FileType::DataFile, Some(storage_id)).unwrap();
        assert!(FileType::DataFile.get_path(&dir, Some(storage_id)).exists());
        change_storage_id(&dir, FileType::DataFile, storage_id, new_storage_id).unwrap();
        assert!(!FileType::DataFile.get_path(&dir, Some(storage_id)).exists());
        assert!(FileType::DataFile
            .get_path(&dir, Some(new_storage_id))
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
    fn test_get_storage_ids_in_dir() {
        let dir = get_temporary_directory_path();
        create_file(&dir, FileType::DataFile, Some(103)).unwrap();
        create_file(&dir, FileType::HintFile, Some(100)).unwrap();
        create_file(&dir, FileType::DataFile, Some(102)).unwrap();
        create_file(&dir, FileType::DataFile, Some(101)).unwrap();
        let storage_ids = get_storage_ids_in_dir(&dir, FileType::DataFile);
        assert_eq!(vec![101, 102, 103], storage_ids);
    }
}
