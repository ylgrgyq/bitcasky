use std::{
    collections::HashMap,
    error,
    fs::File,
    path::{Path, PathBuf},
};

use walkdir::WalkDir;

const DATABASE_FILE_PREFIX: &str = "database-";

pub enum FileType {
    DatabaseFile,
}

pub struct DataBaseFile {
    pub file_id: u32,
    pub file: File,
}

pub fn create_database_file(
    database_dir: &Path,
    file_id: u32,
) -> Result<File, Box<dyn error::Error>> {
    let path = database_dir.join(database_file_name(file_id));
    Ok(File::options()
        .write(true)
        .create(true)
        .read(true)
        .open(path)?)
}

pub fn open_database_files(
    database_dir: &Path,
) -> Result<HashMap<u32, File>, Box<dyn error::Error>> {
    let file_entries = get_valid_database_file_paths(database_dir)?;
    let db_files = file_entries
        .iter()
        .map(|f| open_data_base_file(f))
        .collect::<Result<Vec<DataBaseFile>, _>>()?;
    Ok(db_files.into_iter().map(|f| (f.file_id, f.file)).collect())
}

fn get_valid_database_file_paths(
    database_dir: &Path,
) -> Result<Vec<PathBuf>, Box<dyn error::Error>> {
    Ok(WalkDir::new(database_dir)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let file_name = e.file_name().to_string_lossy();
            if file_name.starts_with(DATABASE_FILE_PREFIX)
                && parse_file_id_from_database_file(e.path()).is_ok()
            {
                Some(e.into_path())
            } else {
                None
            }
        })
        .collect())
}

fn open_data_base_file(file_path: &Path) -> Result<DataBaseFile, Box<dyn error::Error>> {
    let file_id = parse_file_id_from_database_file(file_path)?;
    let file = File::options()
        .write(true)
        .create(true)
        .read(true)
        .open(file_path)?;
    Ok(DataBaseFile { file_id, file })
}

fn parse_file_id_from_database_file(file_path: &Path) -> Result<u32, Box<dyn error::Error>> {
    let binding = file_path.file_name().unwrap().to_string_lossy();
    let (_, file_id_str) = binding.split_at(DATABASE_FILE_PREFIX.len());
    file_id_str.parse::<u32>().map_err(|e| "".into())
}

fn database_file_name(file_id: u32) -> String {
    format!("{}{}", DATABASE_FILE_PREFIX, file_id)
}
