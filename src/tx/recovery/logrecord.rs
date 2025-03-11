use std::fmt;
use std::fmt::{Display, Formatter};
use crate::file::page::Page;
use crate::tx::recovery::checkpointrecord::CheckpointRecord;
use crate::tx::recovery::commitrecord::CommitRecord;
use crate::tx::recovery::rollbackrecord::RollbackRecord;
use crate::tx::recovery::setintrecord::SetIntRecord;
use crate::tx::recovery::startrecord::StartRecord;
use crate::tx::transaction::Transaction;

#[derive(Eq, PartialEq)]
pub enum Op {
    Checkpoint = 0,
    Start = 1,
    Commit = 2,
    Rollback = 3,
    SetInt = 4,
    SetString = 5,
}

pub trait LogRecord: Display {
    fn op(&self) -> Op;
    fn txnum(&self) -> Option<i32>;
    fn undo(&self, tx: &mut Transaction) -> Result<(), &str>;
}

pub fn create_log_record(bytes: Vec<u8>) -> Option<Box<dyn LogRecord>> {
    let page = Page::wrap(bytes);
    match page.get_int(0) {
        op if op == Op::Checkpoint as i32 => {
            Some(Box::new(CheckpointRecord::new()))
        }
        op if op == Op::Start as i32 => {
            Some(Box::new(StartRecord::new(page)))
        }
        op if op == Op::Commit as i32 => {
            Some(Box::new(CommitRecord::new(page)))
        }
        op if op == Op::Rollback as i32 => {
            Some(Box::new(RollbackRecord::new(page)))
        }
        op if op == Op::SetInt as i32 => {
            Some(Box::new(SetIntRecord::new(page)))
        }
        _ => {
            None
        }
    }
}
