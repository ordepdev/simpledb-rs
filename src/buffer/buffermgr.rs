use crate::file::blockid::BlockId;
use crate::buffer::buffer::Buffer;
use crate::file::filemgr::FileMgr;
use crate::log::logmgr::LogMgr;
use std::sync::{Arc, Mutex};
use std::thread::{current, park_timeout};
use std::time::{Duration, Instant};

pub(crate) struct BufferMgr {
    pool: Vec<Buffer>,
    available: usize,
    max_time: u128,
}

impl BufferMgr {
   const MAX_TIME : u128 = 10000;

    // Creates a new buffer manager with the specified number of buffers.
    // Each buffer is initialized with an empty block. The buffer manager
    // keeps track of the number of available buffers and the maximum time
    // to wait for a buffer to be unpinned.
    pub(crate) fn new(fm: Arc<FileMgr>, lm: Arc<Mutex<LogMgr>>, buffsize:  usize) -> BufferMgr {
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

    // Returns the buffer at the specified index. Better than the
    // original implementation because it avoids the need to clone
    // the buffer and sequentially search for it in the pool.
    pub fn buffer(&mut self, idx: usize) -> &mut Buffer {
        &mut self.pool[idx]
    }

    pub(crate) fn available(&self) -> usize {
        self.available
    }

    // Flushes all buffers assigned to the specified transaction.
    pub fn flush_all(&mut self, txnum: i32) {
        for buffer in self.pool.iter_mut() {
            if buffer.transaction().eq(&Some(txnum)) {
                buffer.flush();
            }
        }
    }

    // Unpins the buffer at the specified index, making it available
    // for other threads to use. The thread is also unparked to allow
    // other threads to continue execution.
    pub(crate) fn unpin(&mut self, idx: usize) {
        self.pool[idx].unpin();
        if !self.pool[idx].is_pinned() {
            self.available += 1;
            current().unpark();
        }
    }

    // Pins the buffer containing the specified block. If the buffer
    // is already pinned, the thread is placed on a waiting state until
    // the buffer is unpinned. If the buffer is not unpinned after the
    // maximum time, the buffer manager returns an error.
    pub(crate) fn pin(&mut self, block: &BlockId) -> Result<usize, &str> {
        let timestamp = Instant::now();
        let mut idx = self.try_pin(block);
        // we keep track of how long we've been waiting for a buffer to be unpinned
        // and if it exceeds the maximum time, the buffer manager assumes the caller
        // is in a deadlock and returns an error that must be handled by the caller.
        while idx.is_none() && timestamp.elapsed().as_millis() < self.max_time {
            park_timeout(Duration::from_millis(self.max_time as u64));
            idx = self.try_pin(block);
        }
        match idx {
            Some(idx) => Ok(idx),
            None => Err("Timeout while waiting for buffer to be unpinned"),
        }
    }

    // Attempts to pin the buffer containing the specified block. If the buffer
    // is already pinned, the function returns the index of the buffer. If the
    // buffer is not pinned, the function assigns the block to the buffer and
    // returns the index of the buffer. If there are no available buffers, the
    // function returns None, indicating that the caller must wait for a buffer
    // to be unpinned.
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

    // Sequentially searches for a buffer containing the specified block.
    fn find_existing_buffer(&self, block: &BlockId) -> Option<usize> {
        for (idx, buffer) in self.pool.iter().enumerate() {
            if let Some(b) = buffer.block() {
                if b.eq(block) {
                    // Instead of returning the buffer, we're returning
                    // the index of the buffer. It differs from the original
                    // implementation but avoids the need to clone the buffer.
                    return Some(idx);
                }
            }
        }
        None
    }

    // Sequentially searches for the first unpinned buffer in the pool.
    // This is the Naive Buffer Replacement Strategy, which is not efficient
    // but is good enough for the purpose of this engine.
    fn choose_unpinned_buffer(&mut self) -> Option<usize> {
        for (idx, buffer) in self.pool.iter().enumerate() {
            if !buffer.is_pinned() {
                // Instead of returning the buffer, we're returning
                // the index of the buffer. It differs from the original
                // implementation but avoids the need to clone the buffer.
                return Some(idx);
            }
        }
        None
    }
}

mod tests {
    use super::*;
    use crate::file::filemgr::FileMgr;
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

        // The buffer pool is full at this point, attempting to pin block 3
        // will place the thread on a waiting state until a buffer is unpinned.
        // Given that no buffer will be unpinned, the buffer manager will return
        // a timeout error.
        println!("Attempting to pin block 3...");
        match bm.pin(&BlockId::new("testfile", 3)) {
            Ok(_) => println!("Block 3 pinned successfully"),
            Err(error) => println!("{}", error),
        }

        // Unpinning buffer 2 will make it available for pinning block 3.
        bm.unpin(buff[2]);
        buff.push(bm.pin(&BlockId::new("testfile", 3)).unwrap());

        assert_eq!(*bm.buffer(buff[0]).block().as_ref().unwrap(), BlockId::new("testfile", 0));
        assert_eq!(*bm.buffer(buff[3]).block().as_ref().unwrap(), BlockId::new("testfile", 0));
        assert_eq!(*bm.buffer(buff[4]).block().as_ref().unwrap(), BlockId::new("testfile", 1));
        assert_eq!(*bm.buffer(buff[5]).block().as_ref().unwrap(), BlockId::new("testfile", 3));
    }
}