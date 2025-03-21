use std::fmt::Display;
use std::sync::{Arc, Mutex};
use crate::file::blockid::BlockId;
use crate::file::page::Page;
use crate::log::logmgr::LogMgr;
use crate::tx::recovery::logrecord::{LogRecord, Op};
use crate::tx::transaction::Transaction;

pub struct SetStringRecord {
    txnum: i32,
    offset: usize,
    val: String,
    blk: BlockId,
}

impl LogRecord for SetStringRecord {
    fn op(&self) -> Op {
        Op::SetString
    }

    fn txnum(&self) -> Option<i32> {
        Some(self.txnum)
    }

    fn undo(&self, tx: &mut Transaction) -> Result<(), &str> {
        tx.pin(&self.blk);
        tx.set_string(&self.blk, self.offset, &self.val, false);
        tx.unpin(&self.blk);
        Ok(())
    }
}

impl SetStringRecord {
    pub fn new(page: Page) -> SetStringRecord {
        let filename = page.get_string(8);
        let blkpos = 8 + Page::max_length(filename.len());
        let offsetpos = blkpos + 4;
        let valpos = offsetpos + 4;
        SetStringRecord {
            txnum: page.get_int(4),
            blk: BlockId::new(&filename, page.get_int(blkpos) as usize),
            offset: page.get_int(offsetpos) as usize,
            val: page.get_string(valpos),
        }
    }

    pub fn write_to_log(lm: &Arc<Mutex<LogMgr>>, txnum: i32, blk: BlockId, offset: usize, val: &str) -> i32 {
        let tpos = 4;
        let filepos = tpos + 4;
        let blkpos = filepos + Page::max_length(blk.filename().len());
        let offsetpos = blkpos + 4;
        let valpos = offsetpos + 4;
        let reclen = valpos + Page::max_length(val.len());
        let mut record = Vec::with_capacity(reclen);
        record.resize(reclen, 0);
        let mut page = Page::wrap(record);
        page.set_int(0, Op::SetString as i32);
        page.set_int(tpos, txnum);
        page.set_string(filepos, &blk.filename());
        page.set_int(blkpos, blk.number() as i32);
        page.set_int(offsetpos, offset as i32);
        page.set_string(valpos, val);
        lm.lock().unwrap().append(page.contents())
    }
}

impl Display for SetStringRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<SETSTRING {} {} {} {}>", self.txnum, self.blk, self.offset, self.val)
    }
}