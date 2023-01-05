use std::{borrow::Cow, collections::HashMap, error, fs::File, path::Path};

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
    let path = database_dir
        .join(DATABASE_FILE_PREFIX)
        .join(file_id.to_string());
    Ok(File::options()
        .write(true)
        .create(true)
        .read(true)
        .open(path)?)
}

pub fn open_database_files(
    database_dir: &Path,
) -> Result<HashMap<u32, File>, Box<dyn error::Error>> {
    let file_names = get_valid_database_file_names(database_dir)?;
    let db_files = file_names
        .iter()
        .map(|f| open_data_base_file(database_dir, f))
        .collect::<Result<Vec<DataBaseFile>, _>>()?;
    Ok(db_files.iter().map(|f| (f.file_id, f.file)).collect())
}

fn get_valid_database_file_names(
    database_dir: &Path,
) -> Result<Vec<Cow<str>>, Box<dyn error::Error>> {
    Ok(WalkDir::new(database_dir)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let file_name = e.file_name().to_string_lossy();
            if file_name.starts_with(DATABASE_FILE_PREFIX) {
                Some(file_name)
            } else {
                None
            }
        })
        .filter(|f| parse_file_id_from_database_file(f).is_ok())
        .collect::<Vec<Cow<str>>>())
}

fn open_data_base_file(
    database_dir: &Path,
    file_name: &Cow<str>,
) -> Result<DataBaseFile, Box<dyn error::Error>> {
    let file_id = parse_file_id_from_database_file(file_name)?;
    let file = File::options()
        .write(true)
        .create(true)
        .read(true)
        .open(database_dir.join(file_name.as_ref()))?;
    Ok(DataBaseFile { file_id, file })
}

fn parse_file_id_from_database_file(file_name: &Cow<str>) -> Result<u32, Box<dyn error::Error>> {
    let (_, file_id_str) = file_name.split_at(DATABASE_FILE_PREFIX.len());
    file_id_str.parse::<u32>().map_err(|e| "".into())
}

fn database_file_name(file_id: u32) -> String {
    format!("{}{}", DATABASE_FILE_PREFIX, file_id)
}
