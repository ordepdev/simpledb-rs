use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::RwLock;
use crate::blockid::BlockId;
use crate::page::Page;


pub struct FileMgr {
    db_dir: PathBuf,
    block_size: usize,
    open_files: RwLock<HashMap<String, File>>,
    stats: RwLock<Stats>
}

impl FileMgr {
    pub(crate) fn new(db_dir: PathBuf, block_size: usize) -> FileMgr {
        if !fs::exists(&db_dir).unwrap_or(false) {
            fs::create_dir(&db_dir).unwrap();
        }

        fs::read_dir(&db_dir).unwrap().for_each(|entry| {
            let path = entry.unwrap().path();
            if path.is_file() && path.file_name().unwrap().to_str().unwrap().starts_with("temp") {
                fs::remove_file(path).unwrap();
            }
        });

        FileMgr { db_dir, block_size, open_files: RwLock::new(HashMap::new()), stats: RwLock::new(Stats::new()) }
    }

    pub(crate) fn read(&self, block: &BlockId, page: &mut Page) {
        let path = self.db_dir.join(block.filename());
        let number = block.number() as usize;
        let mut file = self.open_file(path);
        file.seek(SeekFrom::Start(((number * self.block_size) as u64).into())).unwrap();
        file.read(page.bytebuffer.as_mut_slice()).unwrap();
        self.stats.write().unwrap().increment_read_blocks();
    }

    pub(crate) fn write(&self, block: &BlockId, page: &Page) {
        let filename = self.db_dir.join(block.filename());
        let number = block.number() as usize;
        let mut file = self.open_file(filename);
        file.seek(SeekFrom::Start(((number * self.block_size) as u64).into())).unwrap();
        file.write(page.bytebuffer.as_slice()).unwrap();
        self.stats.write().unwrap().increment_written_blocks();
    }

    pub(crate) fn append(&self, filename: &str) -> BlockId {
        let buffer = vec![0; self.block_size];
        let number = self.length(filename) as usize;
        let block = BlockId::new(filename, number);
        let filename = self.db_dir.join(block.filename());
        let mut file = self.open_file(filename);
        file.seek(SeekFrom::Start(((number * self.block_size) as u64).into())).unwrap();
        file.write(buffer.as_slice()).unwrap();
        self.stats.write().unwrap().increment_written_blocks();
        block
    }

    fn open_file(&self, path: PathBuf) -> File {
        let filename = path.to_str().unwrap().to_string();
        let mut files = self.open_files.write().unwrap();
        match files.get(filename.as_str()) {
            Some(file) => file.try_clone().unwrap(),
            None => {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(path)
                    .unwrap();
                files.insert(filename.clone(), file.try_clone().unwrap());
                file
            }
        }
    }

    pub(crate) fn length(&self, file: &str) -> u32 {
        let path = self.db_dir.join(file);
        self.open_file(path).metadata().unwrap().len() as u32 / self.block_size as u32
    }

    pub(crate) fn block_size(&self) -> usize {
        self.block_size
    }
}

struct Stats {
    read_blocks: u32,
    written_blocks: u32
}

impl Stats {
    fn new() -> Stats {
        Stats { read_blocks: 0, written_blocks: 0 }
    }

    fn read_blocks(&self) -> u32 {
        self.read_blocks
    }

    fn written_blocks(&self) -> u32 {
        self.written_blocks
    }

    fn increment_read_blocks(&mut self) {
        self.read_blocks += 1;
    }

    fn increment_written_blocks(&mut self) {
        self.written_blocks += 1;
    }
}


mod tests {
    use super::*;

    #[test]
    fn test_file_mgr() {
        let block_size = 200;
        let fm = FileMgr::new(PathBuf::from("testdb"), block_size);
        let block = BlockId::new("testfile", 2);

        let mut page1 = Page::new(block_size);
        page1.set_string(88, "abcdefghijklm");
        let offset = Page::max_length("abcdefghijklm".len());
        page1.set_byte(88 + offset, 255);
        fm.write(&block, &page1);

        let mut page2 = Page::new(block_size);
        fm.read(&block, &mut page2);

        assert_eq!(page2.get_string(88), "abcdefghijklm");
        assert_eq!(page2.get_byte(88 + offset), Some(255));
        assert_eq!(fm.stats.read().unwrap().read_blocks(), 1);
        assert_eq!(fm.stats.read().unwrap().written_blocks(), 1);
    }

    #[test]
    fn test_file_length() {
        let block_size = 200;
        let fm = FileMgr::new(PathBuf::from("testdb"), block_size);
        let block = BlockId::new("testfile", 2);

        let mut page1 = Page::new(block_size);
        page1.set_string(88, "abcdefghijklm");
        let offset = Page::max_length("abcdefghijklm".len());
        page1.set_byte(88 + offset, 255);
        fm.write(&block, &page1);

        assert_eq!(fm.length("testfile"), 3);
    }
}