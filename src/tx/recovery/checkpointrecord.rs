use std::fmt::Display;
use std::sync::{Arc, Mutex};
use crate::file::page::Page;
use crate::log::logmgr::LogMgr;
use crate::tx::recovery::logrecord::{LogRecord, Op};
use crate::tx::transaction::Transaction;

pub struct CheckpointRecord {}

impl LogRecord for CheckpointRecord {
    fn op(&self) -> Op {
        Op::Checkpoint
    }

    fn txnum(&self) -> Option<i32> {
        None
    }

    fn undo(&self, _tx: &mut Transaction) -> Result<(), &str> {
        Ok(())
    }
}

impl CheckpointRecord {
    pub fn new() -> CheckpointRecord {
        CheckpointRecord {}
    }

    pub fn write_to_log(lm: &Arc<Mutex<LogMgr>>) -> i32 {
        let mut record = Vec::with_capacity(4);
        record.resize(4, 0);
        let mut page = Page::wrap(record);
        page.set_int(0, Op::Checkpoint as i32);
        lm.lock().unwrap().append(page.contents())
    }
}

impl Display for CheckpointRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", "<CHECKPOINT>")
    }
}