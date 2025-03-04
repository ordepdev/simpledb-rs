use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::buffer::buffer::Buffer;
use crate::file::blockid::BlockId;
use crate::buffer::buffermgr::BufferMgr;

pub struct BufferList {
    buffers: HashMap<BlockId, usize>,
    pins: Vec<BlockId>,
    bm: Arc<Mutex<BufferMgr>>,
}

impl BufferList {

    pub(crate) fn new(bm: Arc<Mutex<BufferMgr>>) -> BufferList {
        BufferList { buffers: HashMap::new(), pins: Vec::new(), bm }
    }

    pub(crate) fn buffer(&mut self, blk: &BlockId) -> Option<usize> {
        self.buffers.get(blk).map(|&i| i)
    }

    pub(crate) fn pin(&mut self, blk: BlockId) {
        match self.bm.lock().unwrap().pin(&blk) {
            Ok(idx) => {
                self.buffers.insert(blk.clone(), idx);
                self.pins.push(blk);

            }
            Err(_) => {}
        }
    }

    pub(crate) fn unpin(&mut self, blk: &BlockId) {
        self.buffers.get(blk).map(|&i| self.bm.lock().unwrap().unpin(i));
        self.pins.retain(|b| b != blk);
        //if !self.buffers.contains_key(blk) {
        self.buffers.remove(blk);
        //}
    }

    pub(crate) fn unpin_all(&mut self) {
        self.pins.iter().for_each(|b| {
            self.buffers.get(b).map(|&i| self.bm.lock().unwrap().unpin(i));
        });
        self.buffers.clear();
        self.pins.clear();
    }
}