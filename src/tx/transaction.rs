use crate::buffer::buffermgr::BufferMgr;
use crate::file::blockid::BlockId;
use crate::file::filemgr::FileMgr;
use crate::log::logmgr::LogMgr;
pub use crate::tx::bufferlist::BufferList;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use crate::tx::recovery::checkpointrecord::CheckpointRecord;
use crate::tx::recovery::logrecord::{create_log_record, Op};
use crate::tx::recovery::recoverymgr::RecoveryMgr;
use crate::tx::recovery::rollbackrecord::RollbackRecord;
use crate::tx::concurrency::concurrencymgr::ConcurrencyMgr;

pub struct Transaction {
    txnum: i32,
    buffers: BufferList,
    fm: Arc<FileMgr>,
    rm: Arc<RecoveryMgr>,
    lm: Arc<Mutex<LogMgr>>,
    cm: ConcurrencyMgr,
    bm: Arc<Mutex<BufferMgr>>,
}

static NEXT_TXNUM: AtomicI32 = AtomicI32::new(0);

impl Transaction {
    const END_OF_FILE: i32 = -1;

    pub(crate) fn new(fm: Arc<FileMgr>, bm: Arc<Mutex<BufferMgr>>, lm: Arc<Mutex<LogMgr>>) -> Transaction {
        let txnum = next_txnum();
        Transaction {
            txnum,
            buffers: BufferList::new(bm.clone()),
            fm,
            rm: Arc::new(RecoveryMgr::new(txnum, lm.clone(), bm.clone())),
            lm,
            cm: ConcurrencyMgr::new(),
            bm,
        }
    }

    pub(crate) fn commit(&mut self) {
        self.rm.commit();
        println!("Transaction {} committed", self.txnum);
        println!("Stats: {:?}", self.fm.stats());
        self.cm.release();
        self.buffers.unpin_all();
    }

    fn rollback(&mut self) {
        self.do_rollback();
        self.bm.lock().unwrap().flush_all(self.txnum);
        let lsn = RollbackRecord::write_to_log(&self.lm, self.txnum);
        self.lm.lock().unwrap().flush_record(lsn);
        println!("Transaction {} rolled back", self.txnum);
        self.cm.release();
        self.buffers.unpin_all();
    }

    fn do_rollback(&mut self) {
        let mut iter = self.lm.lock().unwrap().iterator();
        while let Some(record) = iter.next() {
            match create_log_record(record) {
                Some(lr) => {
                    if lr.txnum().unwrap() == self.txnum {
                        if lr.op() == Op::Start {
                            break;
                        }
                        lr.undo(self).unwrap()
                    }
                }
                None => {}
            }
        }
    }

    fn recover(&mut self) {
        self.bm.lock().unwrap().flush_all(self.txnum);
        self.do_recover();
        self.bm.lock().unwrap().flush_all(self.txnum);
        let lsn = CheckpointRecord::write_to_log(&self.lm);
        self.lm.lock().unwrap().flush_record(lsn);
    }

    fn do_recover(&mut self) {
        let mut finished_txs = Vec::new();
        let mut iter = self.lm.lock().unwrap().iterator();
        while let Some(record) = iter.next() {
            if let Some(lr) = create_log_record(record) {
                match lr.op() {
                    Op::Checkpoint => break,
                    Op::Commit | Op::Rollback => finished_txs.push(lr.txnum().unwrap()),
                    _ => {
                        if !finished_txs.contains(&lr.txnum().unwrap()) {
                            lr.undo(self).unwrap();
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn pin(&mut self, blk: &BlockId) {
        self.buffers.pin(blk);
    }

    pub(crate) fn unpin(&mut self, blk: &BlockId) {
        self.buffers.unpin(blk);
    }

    pub fn get_int(&mut self, blk: &BlockId, offset: usize) -> Option<i32> {
        self.cm.slock(blk);
        match self.buffers.buffer(blk) {
            Some(idx) => Some(self.bm.lock().unwrap().buffer(idx).contents().get_int(offset)),
            None => None
        }
    }

    pub(crate) fn set_int(&mut self, blk: &BlockId, offset: usize, val: i32, log: bool) {
        self.cm.xlock(blk);
        match self.buffers.buffer(blk) {
            Some(idx) => {
                let mut bm = self.bm.lock().unwrap();
                let buffer = bm.buffer(idx);
                let mut lsn = -1;
                if log {
                    lsn = self.rm.set_int(buffer, offset, val);
                }
                buffer.contents().set_int(offset, val);
                buffer.set_modified(self.txnum, lsn);
            }
            _ => {}
        }
    }

    pub fn size(&mut self, filename: &str) -> usize {
        let block = BlockId::new(filename, Transaction::END_OF_FILE as usize);
        self.cm.slock(&block);
        self.fm.length(filename) as usize
    }

    pub fn append(&mut self, filename: &str) -> BlockId {
        let block = BlockId::new(filename, Transaction::END_OF_FILE as usize);
        self.cm.slock(&block);
        self.fm.append(filename)
    }

    pub fn block_size(&self) -> usize {
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
    use std::thread;
    use std::thread::{current, sleep};
    use crate::file::page::Page;

    #[test]
    fn test_transaction() {
        let fm = Arc::new(FileMgr::new(PathBuf::from("testdb"), 400));
        let lm = Arc::new(Mutex::new(LogMgr::new(fm.clone(), "testlog.log")));
        let bm = Arc::new(Mutex::new(BufferMgr::new(fm.clone(), lm.clone(), 3)));

        let mut tx1 = Transaction::new(fm.clone(), bm.clone(), lm.clone());
        let blk = BlockId::new("testfile", 1);
        tx1.pin(&blk.clone());
        tx1.set_int(&blk, 80, 1, true);
        tx1.commit();

        let mut tx2 = Transaction::new(fm.clone(), bm.clone(), lm.clone());
        tx2.pin(&blk.clone());
        assert_eq!(tx2.get_int(&blk, 80).unwrap(), 1);
        tx2.set_int(&blk, 80, 2, true);
        tx2.commit();

        let mut tx3 = Transaction::new(fm.clone(), bm.clone(), lm.clone());
        tx3.pin(&blk.clone());
        assert_eq!(tx3.get_int(&blk, 80).unwrap(), 2);
        tx3.set_int(&blk, 80, 9999, true);
        tx3.rollback();

        let mut tx4 = Transaction::new(fm.clone(), bm.clone(), lm.clone());
        tx4.pin(&blk.clone());
        assert_eq!(tx4.get_int(&blk, 80).unwrap(), 2);
        tx4.commit();
    }

    #[test]
    fn test_recovery() {
        let fm = Arc::new(FileMgr::new(PathBuf::from("recoverytestdb"), 400));
        let lm = Arc::new(Mutex::new(LogMgr::new(fm.clone(), "testlog.log")));
        let bm = Arc::new(Mutex::new(BufferMgr::new(fm.clone(), lm.clone(), 3)));

        let blk0 = BlockId::new("testfile", 0);
        let blk1 = BlockId::new("testfile", 1);

        let mut tx1 = Transaction::new(fm.clone(), bm.clone(), lm.clone());
        let mut tx2 = Transaction::new(fm.clone(), bm.clone(), lm.clone());

        tx1.pin(&blk0.clone());
        tx2.pin(&blk1.clone());

        (0..6).for_each(|i| {
            tx1.set_int(&blk0, i * 4, (i * 4) as i32, true);
            tx2.set_int(&blk1, i * 4, (i * 4) as i32, true);
        });

        tx1.commit();
        tx2.commit();

        let mut tx3 = Transaction::new(fm.clone(), bm.clone(), lm.clone());
        let mut tx4 = Transaction::new(fm.clone(), bm.clone(), lm.clone());
        tx3.pin(&blk0.clone());
        tx4.pin(&blk1.clone());

        print_values("After committed changes:", &fm, &blk0, &blk1);

        (0..6).for_each(|i| {
            tx3.set_int(&blk0, i * 4, (i * 4 + 100) as i32, true);
            tx4.set_int(&blk1, i * 4, (i * 4 + 200) as i32, true);
        });

        bm.lock().unwrap().flush_all(tx3.txnum);
        bm.lock().unwrap().flush_all(tx4.txnum);

        print_values("After uncommitted changes:", &fm, &blk0, &blk1);

        tx3.rollback();

        print_values("After rollback:", &fm, &blk0, &blk1);

        Transaction::new(fm.clone(), bm.clone(), lm.clone()).recover();

        print_values("After recovery:", &fm, &blk0, &blk1);

        print_log_file(&fm, &lm);

        let mut page0 = Page::new(fm.block_size());
        let mut page1 = Page::new(fm.block_size());
        fm.read(&blk0, &mut page0);
        fm.read(&blk1, &mut page1);
        assert_eq!(page0.bytebuffer, page1.bytebuffer);
    }

    #[test]
    fn test_concurrency() {
        let fm = Arc::new(FileMgr::new(PathBuf::from("concurrencytestdb"), 400));
        let lm = Arc::new(Mutex::new(LogMgr::new(fm.clone(), "testlog.log")));
        let bm = Arc::new(Mutex::new(BufferMgr::new(fm.clone(), lm.clone(), 3)));

        let mut txA = Transaction::new(fm.clone(), bm.clone(), lm.clone());
        let mut txB = Transaction::new(fm.clone(), bm.clone(), lm.clone());
        let mut txC = Transaction::new(fm.clone(), bm.clone(), lm.clone());

        let A = thread::spawn(move || {
            let blk0 = BlockId::new("testfile", 0);
            let blk1 = BlockId::new("testfile", 1);
            txA.pin(&blk0.clone());
            txA.pin(&blk1.clone());
            println!("Tx A: requesting slock 0");
            txA.get_int(&blk0, 0);
            println!("Tx A: received slock 0");
            sleep(std::time::Duration::from_secs(1));
            println!("Tx A: requesting slock 1");
            txA.get_int(&blk1, 0);
            println!("Tx A: received slock 1");
            txA.commit();
            println!("Tx A: committed");
        });

        let B = thread::spawn(move || {
            let blk0 = BlockId::new("testfile", 0);
            let blk1 = BlockId::new("testfile", 1);
            txB.pin(&blk0.clone());
            txB.pin(&blk1.clone());
            println!("Tx B: requesting xlock 1");
            txB.set_int(&blk1, 0, 0, false);
            println!("Tx B: received xlock 1");
            sleep(std::time::Duration::from_secs(1));
            println!("Tx B: requesting slock 0");
            txB.get_int(&blk0, 0);
            println!("Tx B: received slock 0");
            txB.commit();
            println!("Tx B: committed");
        });

        let C = thread::spawn(move || {
            let blk0 = BlockId::new("testfile", 0);
            let blk1 = BlockId::new("testfile", 1);
            txC.pin(&blk0.clone());
            txC.pin(&blk1.clone());
            sleep(std::time::Duration::from_millis(500));
            println!("Tx C: requesting xlock 0");
            txC.set_int(&blk0, 0, 0, false);
            println!("Tx C: received xlock 0");
            sleep(std::time::Duration::from_secs(1));
            println!("Tx C: requesting slock 1");
            txC.get_int(&blk1, 0);
            println!("Tx C: received slock 1");
            txC.commit();
            println!("Tx C: committed");
        });

        // This test executes three concurrent threads, corresponding to three transactions A, B,
        // and C. These transactions do not explicitly lock and unlock blocks. Instead, `get_int`
        // method obtains an _slock_, `set_int` method obtains an _xlock_, and `commit` method
        // unlocks all its locks. The sequence of locks and unlocks for each transaction looks
        // like this:
        // txA: sLock(blk1); sLock(blk2); unlock(blk1); unlock(blk2)
        // txB: xLock(blk2); sLock(blk1); unlock(blk1); unlock(blk2)
        // txC: xLock(blk1); sLock(blk2); unlock(blk1); unlock(blk2)
        A.join().unwrap();
        B.join().unwrap();
        C.join().unwrap();
    }

    fn print_values(msg: &str, fm: &Arc<FileMgr>, blk0: &BlockId, blk1: &BlockId) {
        println!("{}", msg);
        let mut page0 = Page::new(fm.block_size());
        let mut page1 = Page::new(fm.block_size());
        fm.read(&blk0, &mut page0);
        fm.read(&blk1, &mut page1);
        (0..6).for_each(|i| {
            print!("{:?} ", page0.get_int(i * 4));
            print!("{:?} ", page1.get_int(i * 4));
        });
        println!();
    }

    fn print_log_file(fm: &Arc<FileMgr>, lm: &Arc<Mutex<LogMgr>>) {
        let block = BlockId::new("testlog.log", (fm.length("testlog.log") - 1) as usize);
        let mut page = Page::new(fm.block_size());
        fm.read(&block, &mut page);
        let mut iter = lm.lock().unwrap().iterator();
        while let Some(record) = iter.next() {
            let log_record = create_log_record(record).unwrap();
            println!("{}", log_record);
        }
    }
}