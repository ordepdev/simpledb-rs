use std::fmt::Display;
use std::sync::{Arc, Mutex};
use crate::file::page::Page;
use crate::log::logmgr::LogMgr;
use crate::tx::recovery::logrecord::{LogRecord, Op};
use crate::tx::transaction::Transaction;

pub struct RollbackRecord {
    txnum: i32,
}

impl LogRecord for RollbackRecord {
    fn op(&self) -> Op {
        Op::Rollback
    }

    fn txnum(&self) -> Option<i32> {
        Some(self.txnum)
    }

    fn undo(&self, _tx: &mut Transaction) -> Result<(), &str> {
        Ok(())
    }
}

impl RollbackRecord {
    pub fn new(page: Page) -> RollbackRecord {
        RollbackRecord { txnum: page.get_int(4) }
    }

    pub fn write_to_log(lm: &Arc<Mutex<LogMgr>>, txnum: i32) -> i32 {
        let mut record = Vec::with_capacity(8);
        record.resize(8, 0);
        let mut page = Page::wrap(record);
        page.set_int(0, Op::Rollback as i32);
        page.set_int(4, txnum);
        lm.lock().unwrap().append(page.contents())
    }
}

impl Display for RollbackRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<ROLLBACK {}>", self.txnum)
    }

}