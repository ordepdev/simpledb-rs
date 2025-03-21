use crate::buffer::buffer::Buffer;
use crate::buffer::buffermgr::BufferMgr;
use crate::log::logmgr::LogMgr;
use crate::tx::recovery::checkpointrecord::CheckpointRecord;
use crate::tx::recovery::commitrecord::CommitRecord;
use crate::tx::recovery::rollbackrecord::RollbackRecord;
use crate::tx::recovery::setintrecord::SetIntRecord;
use crate::tx::recovery::setstringrecord::SetStringRecord;
use crate::tx::recovery::startrecord::StartRecord;
use std::sync::{Arc, Mutex};

pub struct RecoveryMgr {
    txnum: i32,
    lm: Arc<Mutex<LogMgr>>,
    bm: Arc<Mutex<BufferMgr>>,
}

impl RecoveryMgr {
    pub(crate) fn new(txnum: i32, lm: Arc<Mutex<LogMgr>>, bm: Arc<Mutex<BufferMgr>>) -> RecoveryMgr {
        StartRecord::write_to_log(&lm, txnum);
        RecoveryMgr { txnum, lm, bm, }
    }

    pub(crate) fn commit(&self) {
        self.bm.lock().unwrap().flush_all(self.txnum);
        let lsn = CommitRecord::write_to_log(&self.lm, self.txnum);
        self.lm.lock().unwrap().flush_record(lsn);
    }

    pub(crate) fn rollback(&self) {
        self.bm.lock().unwrap().flush_all(self.txnum);
        let lsn = RollbackRecord::write_to_log(&self.lm, self.txnum);
        self.lm.lock().unwrap().flush_record(lsn);
    }

    pub(crate) fn recover(&self) {
        self.bm.lock().unwrap().flush_all(self.txnum);
        let lsn = CheckpointRecord::write_to_log(&self.lm);
        self.lm.lock().unwrap().flush_record(lsn);
    }

    pub(crate) fn set_int(&self, buffer: &mut Buffer, offset: usize, _newval: i32) -> i32 {
        let oldval = buffer.contents().get_int(offset);
        let block = buffer.block().clone().unwrap();
        SetIntRecord::write_to_log(&self.lm, self.txnum, block, offset, oldval)
    }

    pub(crate) fn set_string(&self, buffer: &mut Buffer, offset: usize, _newval: &str) -> i32 {
        let oldval = buffer.contents().get_string(offset);
        let block = buffer.block().clone().unwrap();
        SetStringRecord::write_to_log(&self.lm, self.txnum, block, offset, &oldval)
    }
}