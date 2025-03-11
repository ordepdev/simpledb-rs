use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};
use crate::file::page::Page;
use crate::log::logmgr::LogMgr;
use crate::tx::recovery::logrecord::{LogRecord, Op};
use crate::tx::transaction::Transaction;

pub struct StartRecord {
    txnum: i32,
}

impl LogRecord for StartRecord {
    fn op(&self) -> Op {
        Op::Start
    }

    fn txnum(&self) -> Option<i32> {
        Some(self.txnum)
    }

    fn undo(&self, _tx: &mut Transaction) -> Result<(), &str> {
        Ok(())
    }
}

impl StartRecord {
    pub fn new(page: Page) -> StartRecord {
        StartRecord { txnum: page.get_int(4) }
    }

    pub fn write_to_log(lm: &Arc<Mutex<LogMgr>>, txnum: i32) {
        let mut record = Vec::with_capacity(8);
        record.resize(8, 0);
        let mut page = Page::wrap(record);
        page.set_int(0, Op::Start as i32);
        page.set_int(4, txnum);
        lm.lock().unwrap().append(page.contents());
    }
}

impl Display for StartRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "<START {}>", self.txnum)
    }
}

