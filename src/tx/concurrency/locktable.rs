use std::collections::HashMap;
use std::thread::{current, park_timeout};
use std::time::Duration;
use crate::file::blockid::BlockId;

pub struct LockTable {
    locks: HashMap<BlockId, i32>,
    max_time: u128,
}

impl LockTable {

    const MAX_TIME : u128 = 10000;

    pub fn new() -> LockTable {
        LockTable {
            locks: HashMap::new(),
            max_time: Self::MAX_TIME,
        }
    }

    pub fn slock(&mut self, blk: &BlockId) -> Result<(), &str> {
        let start = std::time::SystemTime::now();
        let mut elapsed = start.elapsed().unwrap().as_millis();
        while self.has_xlock(blk) && elapsed < self.max_time {
            park_timeout(Duration::from_millis(self.max_time as u64));
        }
        if self.has_xlock(blk) {
            return Err("block has an exclusive lock")
        }
        let locks = *self.locks.get(blk).unwrap_or(&0);
        self.locks.insert(blk.clone(), locks + 1);
        Ok(())
    }

    pub fn xlock(&mut self, blk: &BlockId) -> Result<(), &str> {
        let start = std::time::SystemTime::now();
        let mut elapsed = start.elapsed().unwrap().as_millis();
        while self.has_other_slocks(&blk) && elapsed < self.max_time {
            park_timeout(Duration::from_millis(self.max_time as u64));
        }
        if self.has_other_slocks(&blk) {
            return Err("block has shared locks")
        }
        self.locks.insert(blk.clone(), -1);
        Ok(())
    }

    pub fn unlock(&mut self, blk: &BlockId) {
        let locks = *self.locks.get(blk).unwrap_or(&0);
        if locks > 1 {
            self.locks.insert(blk.clone(), locks - 1);
        } else {
            self.locks.remove(blk);
            current().unpark();
        }
    }

    fn has_xlock(&self, blk: &BlockId) -> bool {
        self.locks.contains_key(blk) && self.locks[blk] < 0
    }

    fn has_other_slocks(&self, blk: &BlockId) -> bool {
        self.locks.contains_key(blk) && self.locks[blk] > 1
    }
}