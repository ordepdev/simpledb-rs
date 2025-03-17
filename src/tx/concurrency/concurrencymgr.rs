use std::cmp::PartialEq;
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use crate::file::blockid::BlockId;
use crate::tx::concurrency::locktable::LockTable;

#[derive(Eq, PartialEq)]
enum LockType {
    SLock,
    XLock,
}

pub struct ConcurrencyMgr {
    locks: HashMap<BlockId, LockType>,
}

static LOCK_TABLE: LazyLock<Mutex<LockTable>> = LazyLock::new(|| Mutex::new(LockTable::new()));

impl ConcurrencyMgr {
    pub fn new() -> ConcurrencyMgr {
        ConcurrencyMgr {
            locks: HashMap::new(),
        }
    }

    pub fn slock(&mut self, blk: &BlockId) {
        if !self.locks.contains_key(blk) {
            LOCK_TABLE.lock().unwrap().slock(blk).unwrap();
            self.locks.insert(blk.clone(), LockType::SLock);
        }
    }

    pub fn xlock(&mut self, blk: &BlockId) {
        if !self.has_xlock(&blk) {
            self.slock(blk);
            // TODO: I can't lock the table here because it's
            // already locked by the slock method above. Is this
            // a problem?
            // LOCK_TABLE.lock().unwrap().xlock(blk).unwrap();
            self.locks.insert(blk.clone(), LockType::XLock);
        }
    }

    pub fn release(&mut self) {
        for blk in self.locks.keys() {
            LOCK_TABLE.lock().unwrap().unlock(blk);
        }
        self.locks.clear();
    }

    fn has_xlock(&self, blk: &BlockId) -> bool {
        self.locks.contains_key(blk) && self.locks[blk] == LockType::XLock
    }
}