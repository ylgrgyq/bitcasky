use std::{
    fs::{self, File},
    path::Path,
};

use crate::error::BitcaskResult;

use super::FileType;

use fs4::FileExt;
use log::error;

pub fn lock_directory(base_dir: &Path) -> BitcaskResult<Option<File>> {
    fs::create_dir_all(base_dir)?;
    let p = FileType::LockFile.get_path(base_dir, None);
    let file = File::options()
        .write(true)
        .create(true)
        .read(true)
        .open(p)?;
    match file.try_lock_exclusive() {
        Ok(_) => Ok(Some(file)),
        _ => Ok(None),
    }
}

pub fn unlock_directory(file: &File) {
    match file.unlock() {
        Ok(_) => (),
        Err(e) => error!(target: "FileManager", "Unlock directory failed with reason: {}", e),
    }
}
