use std::sync::{Arc, Mutex};
use crate::file::blockid::BlockId;
use crate::record::layout::Layout;
use crate::record::recordpage::RecordPage;
use crate::tx::transaction::Transaction;

#[derive(Debug)]
struct RecordId {
    blocknum: i32,
    slot: i32,
}

trait UpdateScan {
    fn set_int(&mut self, field: &str, val: i32);
    // fn set_string(&mut self, field: &str, val: &str);
    fn insert(&mut self);
    fn delete(&mut self);
    fn rid(&self) -> Option<RecordId>;
    fn move_to_rid(&mut self, rid: &RecordId);
}

struct TableScan {
    tx: Arc<Mutex<Transaction>>,
    layout: Layout,
    rp: Option<RecordPage>,
    filename: String,
    current_slot: Option<i32>,
}

// The TableScan keeps track of the current record, allowing the client to change the record
// and access its fields. It hides the block structure from the client. The client will not know,
// or even care, that the table is stored in blocks.
impl TableScan {
    pub fn new(tx: Arc<Mutex<Transaction>>, layout: Layout, table: &str) -> TableScan {
        let filename = format!("{}.tbl", table);
        let mut ts = TableScan { tx: tx.clone(), layout, rp: None, filename: filename.clone(), current_slot: None };
        if tx.lock().unwrap().size(&filename) == 0 {
            ts.move_to_new_block();
        } else {
            ts.move_to_block(0);
        }
        ts
    }

    fn before_first(&mut self) {
        self.move_to_block(0);
    }

    fn next(&mut self) -> bool {
        if let Some(rp) = &mut self.rp {
            self.current_slot = rp.next_after(self.current_slot);
        }
        while self.current_slot.is_none() {
            if self.at_last_block() {
                return false;
            }
            let mut blk = None;
            if let Some(rp) = &self.rp {
                blk = Some(rp.block_id().number + 1);
            }
            if let Some(blk) = blk {
                self.move_to_block(blk as i32);
            }
            if let Some(rp) = &mut self.rp {
                self.current_slot = rp.next_after(self.current_slot);
            }
        }
        true
    }

    fn get_int(&mut self, field: &str) -> i32 {
        if let Some(rp) = &mut self.rp {
            if let Some(slot) = self.current_slot {
                return rp.get_int(slot, field)
            }
        }
        -1
    }

    fn has_field(&self, field: &str) -> bool {
        self.layout.schema().has_field(field)
    }

    fn close(&self) {
        if let Some(rp) = &self.rp {
            self.tx.lock().unwrap().unpin(&rp.block_id());
        }
    }

    fn move_to_block(&mut self, block_num: i32) {
        self.close();
        let blk = BlockId::new(&self.filename, block_num as usize);
        self.rp = Some(RecordPage::new(self.tx.clone(), blk, self.layout.clone()));
        self.current_slot = None;
    }

    fn move_to_new_block(&mut self) {
        self.close();
        let blk = self.tx.lock().unwrap().append(&self.filename);
        let mut rp = RecordPage::new(self.tx.clone(), blk, self.layout.clone());
        rp.format();
        self.rp = Some(rp);
        self.current_slot = None;
    }

    fn at_last_block(&self) -> bool {
        self.rp.as_ref().map_or(false, |rp| rp.block_id().number == self.tx.lock().unwrap().size(&self.filename) - 1)
    }
}

impl UpdateScan for TableScan {
    fn set_int(&mut self, field: &str, val: i32) {
        if let Some(rp) = &mut self.rp {
            if let Some(slot) = self.current_slot {
                rp.set_int(slot, field, val);
            }
        }
    }

    // The insert method tries to insert a new record starting after the current record.
    // If the block is full, it moves to the next one and continues until it finds an empty slot.
    // If all blocks are full, it appends a new block to the file and inserts the record there.
    fn insert(&mut self) {
        if let Some(rp) = &mut self.rp {
            self.current_slot = rp.insert_after(self.current_slot);
        }
        while self.current_slot.is_none() {
            if self.at_last_block() {
                self.move_to_new_block();
            } else {
                let mut blk = None;
                if let Some(rp) = &self.rp {
                    blk = Some(rp.block_id().number + 1);
                }
                if let Some(blk) = blk {
                    self.move_to_block(blk as i32);
                }
            }
            if let Some(rp) = &mut self.rp {
                self.current_slot = rp.insert_after(self.current_slot);
            }
        }
    }

    fn delete(&mut self) {
        if let Some(rp) = &mut self.rp {
            if let Some(slot) = self.current_slot {
                rp.delete(slot);
            }
        }
    }

    fn rid(&self) -> Option<RecordId>{
        if let Some(rp) = &self.rp {
            if let Some(slot) = self.current_slot {
                return Some(RecordId { blocknum: rp.block_id().number as i32, slot });
            }
        }
        None
    }

    fn move_to_rid(&mut self, rid: &RecordId) {
        self.close();
        let blk = BlockId::new(&self.filename, rid.blocknum as usize);
        self.rp = Some(RecordPage::new(self.tx.clone(), blk, self.layout.clone()));
        self.current_slot = Some(rid.slot);
    }
}

mod tests {
    use std::path::PathBuf;
    use rand::Rng;
    use crate::buffer::buffermgr::BufferMgr;
    use crate::file::filemgr::FileMgr;
    use crate::log::logmgr::LogMgr;
    use super::*;
    use crate::record::schema::Schema;

    #[test]
    fn test_table_scan() {
        let fm = Arc::new(FileMgr::new(PathBuf::from("tablescantestdb"), 400));
        let lm = Arc::new(Mutex::new(LogMgr::new(fm.clone(), "testlog.log")));
        let bm = Arc::new(Mutex::new(BufferMgr::new(fm.clone(), lm.clone(), 8)));
        let mut tx = Arc::new(Mutex::new(Transaction::new(fm.clone(), bm.clone(), lm.clone())));

        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_int_field("B");
        let layout = Layout::new(schema);
        for fname in layout.schema().fields() {
            let offset = layout.offset(&fname);
            println!("Field {} starts at offset {}", fname, offset);
        }
        println!("The slot size is {}", layout.slot_size());

        println!("Filling the table with 50 random records...");
        let mut ts = TableScan::new(tx.clone(), layout, "T1");
        for i in 0..50 {
            ts.insert();
            let num = rand::rng().random_range(0..50);
            ts.set_int("A", num);
            ts.set_int("B", num);
            println!("Inserting into slot {:?}: ({}, {})", ts.rid().unwrap(), num, num);
        }

        println!("Deleting records with A < 25...");
        let mut count = 0;
        ts.before_first();
        while ts.next() {
            if ts.get_int("A") < 25 {
                ts.delete();
                count += 1;
            }
        }
        println!("Deleted {} records", count);

        println!("Here are the remaining records:");
        ts.before_first();
        while ts.next() {
            println!("Slot {:?}: ({}, {})", ts.rid().unwrap(), ts.get_int("A"), ts.get_int( "B"));
        }

        ts.close();
        tx.lock().unwrap().commit();
    }
}