use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    vec,
};

use bytes::{Buf, Bytes};
use log::warn;

use crate::{
    error::{BitcaskError, BitcaskResult},
    file_id::FileIdGenerator,
    fs::{self, FileType},
};

use super::stable_file::StableFile;

const MERGE_FILES_DIRECTORY: &str = "Merge";
const DEFAULT_LOG_TARGET: &str = "DatabaseMerge";

pub fn merge_file_dir(base_dir: &Path) -> PathBuf {
    base_dir.join(MERGE_FILES_DIRECTORY)
}

pub fn create_merge_file_dir(base_dir: &Path) -> BitcaskResult<PathBuf> {
    let merge_dir_path = merge_file_dir(base_dir);

    fs::create_dir(&merge_dir_path)?;

    let mut merge_dir_empty = true;
    let paths = std::fs::read_dir(merge_dir_path.clone())?;
    for path in paths {
        let file_path = path?;
        if file_path.path().is_dir() {
            continue;
        }
        if FileType::MergeMeta.check_file_belongs_to_type(&file_path.path()) {
            continue;
        }
        warn!(
            target: DEFAULT_LOG_TARGET,
            "Merge file directory:{} is not empty, it at least has file: {}",
            merge_dir_path.display().to_string(),
            file_path.path().display()
        );

        merge_dir_empty = false;
        break;
    }
    if !merge_dir_empty {
        let clear_ret = fs::clear_dir(&merge_dir_path).and_then(|_| {
            std::fs::create_dir(merge_dir_path.clone())?;
            Ok(())
        });
        if clear_ret.is_err() {
            warn!(
                target: DEFAULT_LOG_TARGET,
                "clear merge directory failed. {}",
                clear_ret.unwrap_err()
            );
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
        fs::commit_file(
            FileType::DataFile,
            Some(*file_id),
            &merge_dir_path,
            base_dir,
        )?;
        fs::commit_file(
            FileType::HintFile,
            Some(*file_id),
            &merge_dir_path,
            base_dir,
        )?;
    }
    Ok(())
}

pub fn recover_merge(
    database_dir: &Path,
    file_id_generator: &Arc<FileIdGenerator>,
) -> BitcaskResult<()> {
    let recover_ret = do_recover_merge(&database_dir, &file_id_generator);
    if let Err(err) = recover_ret {
        let merge_dir = merge_file_dir(&database_dir);
        warn!(
            "recover merge under path: {} failed with error: \"{}\"",
            merge_dir.display(),
            err
        );
        match err {
            BitcaskError::InvalidMergeDataFile(_, _) => {
                // clear Merge directory when recover merge failed
                fs::clear_dir(&merge_file_dir(&database_dir))?;
            }
            _ => return Err(err),
        }
    }
    Ok(())
}

fn do_recover_merge(
    database_dir: &Path,
    file_id_generator: &Arc<FileIdGenerator>,
) -> BitcaskResult<()> {
    let merge_file_dir = merge_file_dir(database_dir);

    if !merge_file_dir.exists() {
        return Ok(());
    }

    let mut merge_data_file_ids = fs::get_valid_data_file_ids(&merge_file_dir);
    if merge_data_file_ids.is_empty() {
        return Ok(());
    }

    merge_data_file_ids.sort();
    let merge_meta = read_merge_meta(&merge_file_dir)?;
    if *merge_data_file_ids.first().unwrap() <= merge_meta.known_max_file_id {
        return Err(BitcaskError::InvalidMergeDataFile(
            merge_meta.known_max_file_id,
            *merge_data_file_ids.first().unwrap(),
        ));
    }

    file_id_generator.update_file_id(*merge_data_file_ids.last().unwrap());

    shift_data_files(
        database_dir,
        merge_meta.known_max_file_id,
        file_id_generator,
    )?;

    commit_merge_files(database_dir, &merge_data_file_ids)?;

    fs::purge_outdated_data_files(database_dir, merge_meta.known_max_file_id)?;

    let clear_ret = fs::clear_dir(&merge_file_dir);
    if clear_ret.is_err() {
        warn!(target: "Database", "clear merge directory failed. {}", clear_ret.unwrap_err());
    }
    Ok(())
}

pub fn shift_data_files(
    database_dir: &Path,
    known_max_file_id: u32,
    file_id_generator: &Arc<FileIdGenerator>,
) -> BitcaskResult<Vec<u32>> {
    let mut data_file_ids = fs::get_valid_data_file_ids(database_dir)
        .into_iter()
        .filter(|id| *id > known_max_file_id)
        .collect::<Vec<u32>>();
    // must change name in descending order to keep data file's order even when any change name operation failed
    data_file_ids.sort_by(|a, b| b.cmp(a));

    // rename files which file id >= knwon_max_file_id to files which file id greater than all merged files
    // because values in these files is written after merged files
    let mut new_file_ids = vec![];
    for from_id in data_file_ids {
        let new_file_id = file_id_generator.generate_next_file_id();
        fs::change_data_file_id(database_dir, from_id, new_file_id)?;
        new_file_ids.push(new_file_id);
    }
    Ok(new_file_ids)
}

pub fn load_merged_files(
    database_dir: &Path,
    file_id_generator: &Arc<FileIdGenerator>,
    merged_file_ids: &Vec<u32>,
    known_max_file_id: u32,
    tolerate_data_file_corruption: bool,
) -> BitcaskResult<Vec<StableFile>> {
    let data_file_ids = shift_data_files(database_dir, known_max_file_id, file_id_generator)?;

    let mut stable_files = vec![];
    for file_id in data_file_ids {
        if let Some(f) = StableFile::open(database_dir, file_id, tolerate_data_file_corruption)? {
            stable_files.push(f);
        }
    }

    if merged_file_ids.is_empty() {
        return Ok(stable_files);
    }

    commit_merge_files(database_dir, merged_file_ids)?;

    for file_id in merged_file_ids {
        if let Some(f) = StableFile::open(database_dir, *file_id, tolerate_data_file_corruption)? {
            stable_files.push(f);
        }
    }

    Ok(stable_files)
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct MergeMeta {
    pub known_max_file_id: u32,
}

pub fn read_merge_meta(merge_file_dir: &Path) -> BitcaskResult<MergeMeta> {
    let mut merge_meta_file = fs::open_file(merge_file_dir, FileType::MergeMeta, None)?;
    let mut buf = vec![0; 4];
    merge_meta_file.file.read_exact(&mut buf)?;
    let mut bs = Bytes::from(buf);
    let known_max_file_id = bs.get_u32();
    Ok(MergeMeta { known_max_file_id })
}

pub fn write_merge_meta(merge_file_dir: &Path, merge_meta: MergeMeta) -> BitcaskResult<()> {
    let mut merge_meta_file = fs::create_file(merge_file_dir, FileType::MergeMeta, None)?;
    merge_meta_file.write_all(&merge_meta.known_max_file_id.to_be_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use bitcask_tests::common::get_temporary_directory_path;
    use test_log::test;

    #[test]
    fn test_create_merge_file_dir() {
        let dir = get_temporary_directory_path();
        let merge_file_path = create_merge_file_dir(&dir).unwrap();
        fs::create_file(&merge_file_path, FileType::DataFile, Some(0)).unwrap();

        create_merge_file_dir(&dir).unwrap();

        let paths = std::fs::read_dir(merge_file_path.clone()).unwrap();
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
        fs::create_file(&merge_file_path, FileType::DataFile, Some(0)).unwrap();
        fs::create_file(&merge_file_path, FileType::DataFile, Some(1)).unwrap();
        fs::create_file(&merge_file_path, FileType::DataFile, Some(2)).unwrap();

        assert_eq!(
            vec![0, 1, 2,],
            fs::get_data_file_ids_in_dir(&merge_file_path)
        );
        assert!(fs::get_data_file_ids_in_dir(&dir_path).is_empty());

        commit_merge_files(&dir_path, &vec![0, 1, 2]).unwrap();

        assert!(fs::is_empty_dir(&merge_file_path).unwrap());

        assert_eq!(vec![0, 1, 2,], fs::get_data_file_ids_in_dir(&dir_path));
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
