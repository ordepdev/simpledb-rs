use std::sync::{Arc, Mutex};
use crate::blockid::BlockId;
use crate::filemgr::FileMgr;
use crate::log::logmgr::LogMgr;
use crate::page::Page;

struct  Buffer {
    fm: Arc<FileMgr>,
    lm: Arc<Mutex<LogMgr>>,
    contents: Page,
    block: Option<BlockId>,
    pins: i32,
    txnum: Option<i32>,
    lsn: Option<i32>,
}

impl Buffer {
    fn new(fm: Arc<FileMgr>, lm: Arc<Mutex<LogMgr>>) -> Buffer {
        let block_size = fm.block_size();
        Buffer {
            fm,
            lm,
            contents: Page::new(block_size),
            block: None,
            pins: 0,
            txnum: None,
            lsn: None,
        }
    }

    fn contents(&mut self) -> &mut Page {
        &mut self.contents
    }

    fn block(&self) -> &Option<BlockId> {
        &self.block
    }

    fn set_modified(&mut self, txnum: i32, lsn: i32) {
        self.txnum = Some(txnum);
        if lsn >= 0 {
            self.lsn = Some(lsn);
        }
    }

    fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    fn transaction(&self) -> Option<i32> {
        self.txnum
    }

    fn flush(&mut self) {
        if self.txnum.is_some() {
            self.lm.lock().unwrap().flush_record(self.lsn.unwrap());
            if let Some(ref block) = self.block {
                self.fm.write(block, &mut self.contents);
            }
            self.txnum = None;
        }
    }

    fn assign_to_block(&mut self, block: BlockId) {
        self.flush();
        self.block = Some(block.clone());
        self.fm.read(&block, &mut self.contents);
        self.pins = 0;
    }

    fn pin(&mut self) {
        self.pins += 1;
    }

    fn unpin(&mut self) {
        self.pins -= 1;
    }
}

mod tests {
    use std::path::PathBuf;
    use super::*;
    use std::sync::Arc;
    use crate::filemgr::FileMgr;
    use crate::log::logmgr::LogMgr;

    #[test]
    fn test_buffer() {
        let fm = Arc::new(FileMgr::new(PathBuf::from("testdb"), 400));
        let lm = Arc::new(Mutex::new(LogMgr::new(fm.clone(), "testlog.log")));
        let mut buffer = Buffer::new(fm.clone(), lm.clone());

        assert_eq!(buffer.is_pinned(), false);
        buffer.pin();
        assert_eq!(buffer.is_pinned(), true);

        buffer.assign_to_block(BlockId::new("testfile", 1));
        let page = buffer.contents();
        let number = page.get_int(80);
        page.set_int(80, number + 1);
        buffer.set_modified(1, 0);
        buffer.unpin();

        assert_eq!(buffer.is_pinned(), false);
        assert_eq!(buffer.transaction(), Some(1));
        assert_eq!(buffer.block(), &Some(BlockId::new("testfile", 1)));
        assert_eq!(buffer.contents().get_int(80), number + 1);

        buffer.flush();

        assert_eq!(buffer.transaction(), None);
    }
}