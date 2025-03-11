use std::fmt::Display;
use std::sync::{Arc, Mutex};
use crate::file::blockid::BlockId;
use crate::file::page::Page;
use crate::log::logmgr::LogMgr;
use crate::tx::recovery::logrecord::{LogRecord, Op};
use crate::tx::transaction::Transaction;

pub struct SetIntRecord {
    txnum: i32,
    offset: usize,
    val: i32,
    blk: BlockId,
}

impl LogRecord for SetIntRecord {
    fn op(&self) -> Op {
        Op::SetInt
    }

    fn txnum(&self) -> Option<i32> {
        Some(self.txnum)
    }

    fn undo(&self, tx: &mut Transaction) -> Result<(), &str> {
        tx.pin(&self.blk);
        tx.set_int(&self.blk, self.offset, self.val, false);
        tx.unpin(&self.blk);
        Ok(())
    }
}

impl SetIntRecord {
    pub fn new(page: Page) -> SetIntRecord {
        let filename = page.get_string(8);
        let blkpos = 8 + Page::max_length(filename.len());
        let offsetpos = blkpos + 4;
        let valpos = offsetpos + 4;
        SetIntRecord {
            txnum: page.get_int(4),
            blk: BlockId::new(&filename, page.get_int(blkpos) as usize),
            offset: page.get_int(offsetpos) as usize,
            val: page.get_int(valpos),
        }
    }

    pub fn write_to_log(lm: &Arc<Mutex<LogMgr>>, txnum: i32, blk: BlockId, offset: usize, val: i32) -> i32 {
        let tpos = 4;
        let filepos = tpos + 4;
        let blkpos = filepos + Page::max_length(blk.filename().len());
        let offsetpos = blkpos + 4;
        let valpos = offsetpos + 4;
        let mut record = Vec::with_capacity(valpos + 4);
        record.resize(valpos + 4, 0);
        let mut page = Page::wrap(record);
        page.set_int(0, Op::SetInt as i32);
        page.set_int(tpos, txnum);
        page.set_string(filepos, &blk.filename());
        page.set_int(blkpos, blk.number() as i32);
        page.set_int(offsetpos, offset as i32);
        page.set_int(valpos, val);
        lm.lock().unwrap().append(page.contents())
    }
}

impl Display for SetIntRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<SETINT {} {} {} {}>", self.txnum, self.blk, self.offset, self.val)
    }
}