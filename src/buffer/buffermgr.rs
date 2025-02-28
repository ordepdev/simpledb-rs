use crate::blockid::BlockId;
use crate::buffer::buffer::Buffer;
use crate::filemgr::FileMgr;
use crate::log::logmgr::LogMgr;
use std::sync::{Arc, Mutex};
use std::thread::{current, park_timeout};
use std::time::{Duration, Instant};

struct BufferMgr {
    pool: Vec<Buffer>,
    available: usize,
    max_time: u128,
}

impl BufferMgr {
   const MAX_TIME : u128 = 10000;

    fn new(fm: Arc<FileMgr>, lm: Arc<Mutex<LogMgr>>, buffsize:  usize) -> BufferMgr {
        let mut pool = Vec::with_capacity(buffsize);
        for _ in 0..buffsize {
            pool.push(Buffer::new(fm.clone(), lm.clone()));
        }
        BufferMgr {
            pool,
            available: buffsize,
            max_time: Self::MAX_TIME,
        }
    }

    pub fn buffer(&mut self, idx: usize) -> &mut Buffer {
        &mut self.pool[idx]
    }

    fn available(&self) -> usize {
        self.available
    }

    fn flush_all(&mut self, txnum: i32) {
        for buffer in self.pool.iter_mut() {
            if buffer.transaction().eq(&Some(txnum)) {
                buffer.flush();
            }
        }
    }

    fn unpin(&mut self, idx: usize) {
        self.pool[idx].unpin();
        if !self.pool[idx].is_pinned() {
            self.available += 1;
            current().unpark();
        }
    }

    fn pin(&mut self, block: &BlockId) -> Result<usize, &str> {
        let timestamp = Instant::now();
        let mut idx = self.try_pin(block);
        while idx.is_none() && timestamp.elapsed().as_millis() < self.max_time {
            park_timeout(Duration::from_millis(self.max_time as u64));
            idx = self.try_pin(block);
        }
        match idx {
            Some(idx) => Ok(idx),
            None => Err("Timeout while waiting for buffer to be unpinned"),
        }
    }

    fn try_pin(&mut self, block: &BlockId) -> Option<usize> {
        if let Some(idx) = self.find_existing_buffer(block) {
            if !self.pool[idx].is_pinned() {
                self.available -= 1;
            }
            self.pool[idx].pin();
            return Some(idx);
        }

        if let Some(idx) = self.choose_unpinned_buffer() {
            self.pool[idx].assign_to_block(block.clone());
            if !self.pool[idx].is_pinned() {
                self.available -= 1;
            }
            self.pool[idx].pin();
            return Some(idx);
        }

        None
    }

    fn find_existing_buffer(&self, block: &BlockId) -> Option<usize> {
        for (idx, buffer) in self.pool.iter().enumerate() {
            if let Some(b) = buffer.block() {
                if b.eq(block) {
                    // Instead of returning the buffer, we're returning
                    // the index of the buffer.It differs from the original
                    // implementation but avoids the need to clone the buffer.
                    return Some(idx);
                }
            }
        }
        None
    }

    fn choose_unpinned_buffer(&mut self) -> Option<usize> {
        for (idx, buffer) in self.pool.iter().enumerate() {
            if !buffer.is_pinned() {
                // Instead of returning the buffer, we're returning
                // the index of the buffer.It differs from the original
                // implementation but avoids the need to clone the buffer.
                return Some(idx);
            }
        }
        None
    }
}

mod tests {
    use super::*;
    use crate::filemgr::FileMgr;
    use crate::log::logmgr::LogMgr;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[test]
    fn test_buffer_mgr() {
        let fm = Arc::new(FileMgr::new(PathBuf::from("testdb"), 400));
        let lm = Arc::new(Mutex::new(LogMgr::new(fm.clone(), "testlog.log")));
        let mut bm = BufferMgr::new(fm.clone(), lm.clone(), 3);

        // Set the maximum time to wait for a buffer to be unpinned to 1ms
        // to test the timeout functionality without waiting for too long.
        bm.max_time = 1;

        // Let's have our own buffer pool here instead of using
        // the one in the `BufferMgr` struct.
        let mut buff = Vec::with_capacity(6);

        buff.push(bm.pin(&BlockId::new("testfile", 0)).unwrap());
        buff.push(bm.pin(&BlockId::new("testfile", 1)).unwrap());
        buff.push(bm.pin(&BlockId::new("testfile", 2)).unwrap());
        bm.unpin(buff[1]);
        buff.push(bm.pin(&BlockId::new("testfile", 0)).unwrap());
        buff.push(bm.pin(&BlockId::new("testfile", 1)).unwrap());
        println!("Available buffers: {}", bm.available());

        println!("Attempting to pin block 3...");
        match bm.pin(&BlockId::new("testfile", 3)) {
            Ok(_) => println!("Block 3 pinned successfully"),
            Err(error) => println!("{}", error),
        }

        bm.unpin(buff[2]);
        buff.push(bm.pin(&BlockId::new("testfile", 3)).unwrap());

        assert_eq!(*bm.buffer(buff[0]).block().as_ref().unwrap(), BlockId::new("testfile", 0));
        assert_eq!(*bm.buffer(buff[3]).block().as_ref().unwrap(), BlockId::new("testfile", 0));
        assert_eq!(*bm.buffer(buff[4]).block().as_ref().unwrap(), BlockId::new("testfile", 1));
        assert_eq!(*bm.buffer(buff[5]).block().as_ref().unwrap(), BlockId::new("testfile", 3));
    }
}