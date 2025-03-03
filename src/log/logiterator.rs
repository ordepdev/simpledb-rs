use std::sync::Arc;
use crate::file::blockid::BlockId;
use crate::file::filemgr::FileMgr;
use crate::file::page::Page;

pub struct LogIterator {
    fm: Arc<FileMgr>,
    block: BlockId,
    page: Page,
    currentpos: i32,
    boundary: i32,
}

impl LogIterator {
    pub fn new(fm: Arc<FileMgr>, block: &BlockId) -> LogIterator {
        let buffer = vec![0; fm.block_size()];
        let page = Page::wrap(buffer);
        let mut iterator = LogIterator { fm, block: block.clone(), page, currentpos: 0, boundary: 0 };
        iterator.move_to_block(block);
        iterator
    }

    fn move_to_block(&mut self, block: &BlockId) {
        self.fm.read(block, &mut self.page);
        self.boundary = self.page.get_int(0);
        self.currentpos = self.boundary;
    }
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.currentpos >= self.fm.block_size() as i32 && self.block.number() <= 0 {
            return None;
        }

        if self.currentpos == self.fm.block_size() as i32 {
            self.block = BlockId::new(self.block.filename(), self.block.number() - 1);
            self.move_to_block(&self.block.clone());
        }

        let record = self.page.get_bytes(self.currentpos as usize);
        self.currentpos += 4 + record.len() as i32;
        Some(record.to_vec())
    }
}