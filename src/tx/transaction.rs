use crate::buffer::buffermgr::BufferMgr;
use crate::file::blockid::BlockId;
use crate::file::filemgr::FileMgr;
use crate::log::logmgr::LogMgr;
pub use crate::tx::bufferlist::BufferList;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

struct Transaction {
    txnum: i32,
    buffers: BufferList,
    fm: Arc<FileMgr>,
    bm: Arc<Mutex<BufferMgr>>
}

static NEXT_TXNUM: AtomicI32 = AtomicI32::new(0);

impl Transaction {
    fn new(fm: Arc<FileMgr>, bm: Arc<Mutex<BufferMgr>>, lm: Arc<Mutex<LogMgr>>) -> Transaction {
        Transaction {
            txnum: next_txnum(),
            buffers: BufferList::new(bm.clone()),
            fm,
            bm
        }
    }

    fn commit(&mut self) {
        println!("Transaction {} committed", self.txnum);
        println!("Stats: {:?}", self.fm.stats());
        self.buffers.unpin_all();
    }

    fn rollback(&mut self) {
        println!("Transaction {} rolled back", self.txnum);
        self.buffers.unpin_all();
    }

    fn recover(&mut self) {
        self.bm.lock().unwrap().flush_all(self.txnum);
    }

    fn pin(&mut self, blk: BlockId) {
        self.buffers.pin(blk);
    }

    fn unpin(&mut self, blk: &BlockId) {
        self.buffers.unpin(blk);
    }

    fn get_int(&mut self, blk: &BlockId, offset: usize) -> Option<i32> {
        match self.buffers.buffer(blk) {
            Some(idx) => Some(self.bm.lock().unwrap().buffer(idx).contents().get_int(offset)),
            None => None
        }
    }

    fn set_int(&mut self, blk: &BlockId, offset: usize, val: i32) {
        match self.buffers.buffer(blk) {
            Some(idx) => {
                let lsn = -1;
                let mut bm = self.bm.lock().unwrap();
                let buffer = bm.buffer(idx);
                buffer.contents().set_int(offset, val);
                buffer.set_modified(self.txnum, lsn);
            }
            _ => {}
        }
    }

    fn size(&self, filename: &str) -> usize {
        self.fm.length(filename) as usize
    }

    fn append(&mut self, filename: &str) -> BlockId {
        self.fm.append(filename)
    }

    fn block_size(&self) -> usize {
        self.fm.block_size()
    }

    fn available_buffers(&self) -> usize {
        self.bm.lock().unwrap().available()
    }
}

fn next_txnum() -> i32 {
    NEXT_TXNUM.fetch_add(1, Ordering::SeqCst);
    NEXT_TXNUM.load(Ordering::SeqCst)
}

mod tests {
    use super::*;
    use crate::buffer::buffermgr::BufferMgr;
    use crate::file::filemgr::FileMgr;
    use crate::log::logmgr::LogMgr;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[test]
    fn test_transaction() {
        let fm = Arc::new(FileMgr::new(PathBuf::from("testdb"), 400));
        let lm = Arc::new(Mutex::new(LogMgr::new(fm.clone(), "testlog.log")));
        let bm = Arc::new(Mutex::new(BufferMgr::new(fm.clone(), lm.clone(), 3)));

        let mut tx1 = Transaction::new(fm.clone(), bm.clone(), lm.clone());
        let blk = BlockId::new("testfile", 1);
        tx1.pin(blk.clone());
        tx1.set_int(&blk, 80, 1);
        tx1.commit();

        let mut tx2 = Transaction::new(fm.clone(), bm.clone(), lm.clone());
        tx2.pin(blk.clone());
        assert_eq!(tx2.get_int(&blk, 80).unwrap(), 1);
        tx2.set_int(&blk, 80, 2);
        tx2.commit();

        let mut tx3 = Transaction::new(fm.clone(), bm.clone(), lm.clone());
        tx3.pin(blk.clone());
        assert_eq!(tx3.get_int(&blk, 80).unwrap(), 2);
        tx3.set_int(&blk, 80, 9999);
        tx3.rollback();
    }
}