use std::fmt::Display;
use std::sync::{Arc, Mutex};
use crate::file::page::Page;
use crate::log::logmgr::LogMgr;
use crate::tx::recovery::logrecord::{LogRecord, Op};
use crate::tx::transaction::Transaction;

pub struct CommitRecord {
    txnum: i32,
}

impl LogRecord for CommitRecord {
    fn op(&self) -> Op {
        Op::Commit
    }

    fn txnum(&self) -> Option<i32> {
        Some(self.txnum)
    }

    fn undo(&self, _tx: &mut Transaction) -> Result<(), &str> {
        Ok(())
    }
}

impl CommitRecord {
    pub fn new(page: Page) -> CommitRecord {
        CommitRecord { txnum: page.get_int(4) }
    }

    pub fn write_to_log(lm: &Arc<Mutex<LogMgr>>, txnum: i32) -> i32 {
        let mut record = Vec::with_capacity(8);
        record.resize(8, 0);
        let mut page = Page::wrap(record);
        page.set_int(0, Op::Commit as i32);
        page.set_int(4, txnum);
        lm.lock().unwrap().append(page.contents())
    }
}

impl Display for CommitRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<COMMIT {}>", self.txnum)
    }
}