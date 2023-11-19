use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom},
    path::{Path, PathBuf},
};

use formatter::BitcaskFormatter;
use fs::FileType;
#[cfg(not(unix))]
use fs4::FileExt;
use storage_id::StorageId;

use crate::formatter::FILE_HEADER_SIZE;

pub mod formatter;
pub mod fs;
pub mod storage_id;
pub mod tombstone;

#[cfg(test)]
#[macro_use]
extern crate assert_matches;

pub fn create_file<P: AsRef<Path>>(
    base_dir: P,
    file_type: FileType,
    storage_id: Option<StorageId>,
    formatter: &BitcaskFormatter,
    init_data_file_capacity: usize,
) -> std::io::Result<File> {
    // Round capacity down to the nearest 8-byte alignment, since the
    // data storage would not be able to take advantage of the space.
    let capacity = std::cmp::max(FILE_HEADER_SIZE, init_data_file_capacity) & !7;

    let path = file_type.get_path(&base_dir, storage_id);
    let file_name = path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .expect("File name required");

    let tmp_file_path = match path.parent() {
        Some(parent) => parent.join(format!("tmp-{file_name}")),
        None => PathBuf::from(format!("tmp-{file_name}")),
    };

    {
        // Prepare properly formatted file in a temporary file, so in case of failure it won't be corrupted.
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&tmp_file_path)?;

        // fs4 provides some cross-platform bindings which help for Windows.
        #[cfg(not(unix))]
        file.allocate(capacity as u64)?;
        // For all unix systems we can just use ftruncate directly
        #[cfg(unix)]
        {
            rustix::fs::ftruncate(&file, capacity as u64)?;
        }

        formatter::initialize_new_file(&mut file, formatter.version())?;

        // Manually sync each file in Windows since sync-ing cannot be done for the whole directory.
        #[cfg(target_os = "windows")]
        {
            file.sync_all()?;
        }
    };

    // File renames are atomic, so we can safely rename the temporary file to the final file.
    std::fs::rename(&tmp_file_path, &path)?;

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(&path)?;
    file.seek(SeekFrom::Start(FILE_HEADER_SIZE as u64))?;

    Ok(file)
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use crate::formatter::get_formatter_from_file;

    use super::*;

    use bitcask_tests::common::get_temporary_directory_path;
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use test_log::test;

    #[test]
    fn test_create_file() {
        let dir = get_temporary_directory_path();
        let storage_id = 1;
        let formatter = BitcaskFormatter::default();
        let mut file =
            create_file(&dir, FileType::DataFile, Some(storage_id), &formatter, 100).unwrap();

        let mut bs = BytesMut::with_capacity(4);
        bs.put_u32(101);

        file.write_all(&bs.freeze()).unwrap();
        file.flush().unwrap();

        let mut reopen_file = fs::open_file(dir, FileType::DataFile, Some(storage_id))
            .unwrap()
            .file;
        assert_eq!(
            formatter,
            get_formatter_from_file(&mut reopen_file).unwrap()
        );

        let mut buf = vec![0; 4];
        reopen_file.read_exact(&mut buf).unwrap();
        assert_eq!(101, Bytes::from(buf).get_u32());
    }
}
