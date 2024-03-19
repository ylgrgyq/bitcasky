use std::{
    fs::{self, File},
    path::Path,
};

use super::FileType;

use fs4::FileExt;

pub fn lock_directory(base_dir: &Path) -> std::io::Result<Option<File>> {
    fs::create_dir_all(base_dir)?;
    let p = FileType::LockFile.get_path(base_dir, None);
    let file = File::create(p)?;
    match file.try_lock_exclusive() {
        Ok(_) => Ok(Some(file)),
        _ => Ok(None),
    }
}
