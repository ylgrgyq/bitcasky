use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
};

use walkdir::WalkDir;

use crate::error::{BitcaskError, BitcaskResult};

const DATA_FILE_POSTFIX: &str = ".data";
const HINT_FILE_POSTFIX: &str = ".hint";

pub enum FileType {
    DataFile,
    HintFile,
}

impl FileType {
    fn generate_name(&self, database_dir: &Path, file_id: u32) -> PathBuf {
        database_dir.join(format!(
            "{}{}",
            match self {
                Self::DataFile => DATA_FILE_POSTFIX,
                Self::HintFile => HINT_FILE_POSTFIX,
            },
            file_id,
        ))
    }
}

pub struct IdentifiedFile {
    pub file_id: u32,
    pub file: File,
}

pub fn create_file(database_dir: &Path, file_id: u32, file_type: FileType) -> BitcaskResult<File> {
    let path = file_type.generate_name(database_dir, file_id);
    Ok(File::options()
        .write(true)
        .create(true)
        .read(true)
        .open(path)?)
}

pub fn open_stable_database_files(database_dir: &Path) -> BitcaskResult<HashMap<u32, File>> {
    let file_entries = get_valid_database_file_paths(database_dir);
    let db_files = file_entries
        .iter()
        .map(|f| open_stable_data_base_file(f))
        .collect::<BitcaskResult<Vec<IdentifiedFile>>>()?;
    Ok(db_files.into_iter().map(|f| (f.file_id, f.file)).collect())
}

fn get_valid_database_file_paths(database_dir: &Path) -> Vec<PathBuf> {
    WalkDir::new(database_dir)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let file_name = e.file_name().to_string_lossy();
            if file_name.starts_with(DATA_FILE_POSTFIX)
                && parse_file_id_from_database_file(e.path()).is_ok()
            {
                Some(e.into_path())
            } else {
                None
            }
        })
        .collect()
}

fn open_stable_data_base_file(file_path: &Path) -> BitcaskResult<IdentifiedFile> {
    let file_id = parse_file_id_from_database_file(file_path)?;
    let file = File::options().read(true).open(file_path)?;
    Ok(IdentifiedFile { file_id, file })
}

fn parse_file_id_from_database_file(file_path: &Path) -> BitcaskResult<u32> {
    let binding = file_path.file_name().unwrap().to_string_lossy();
    let (_, file_id_str) = binding.split_at(DATA_FILE_POSTFIX.len());
    file_id_str
        .parse::<u32>()
        .map_err(|_| BitcaskError::InvalidDatabaseFileName(binding.to_string()))
}
