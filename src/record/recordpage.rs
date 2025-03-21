use std::sync::{Arc, Mutex};
use crate::file::blockid::BlockId;
use crate::record::layout::Layout;
use crate::tx::transaction::Transaction;

#[derive(Clone, Copy)]
enum Slot {
    Empty = 0,
    Used = 1,
}

pub(crate) struct RecordPage {
    tx: Arc<Mutex<Transaction>>,
    block_id: BlockId,
    layout: Layout,
}

// The RecordPage manages the records within a page. It provides methods for reading and writing
// records, as well as for navigating the page. The RecordPage is responsible for maintaining the
// slot array, which keeps track of which slots are in use -- it implements the slotted-page structure
// where the empty/used flags are implemented as 4-byte integers instead of single bytes.
impl RecordPage {
    pub fn new(tx: Arc<Mutex<Transaction>>, block_id: BlockId, layout: Layout) -> RecordPage {
        tx.lock().unwrap().pin(&block_id);
        RecordPage { tx, block_id, layout }
    }

    pub(crate) fn get_int(&mut self, slot: i32, field: &str) -> i32 {
        let fpos = self.offset(slot) + self.layout.offset(field);
        self.tx.lock().unwrap().get_int(&self.block_id, fpos as usize).unwrap()
    }

    pub(crate) fn set_int(&mut self, slot: i32, field: &str, val: i32) {
        let fpos = self.offset(slot) + self.layout.offset(field);
        self.tx.lock().unwrap().set_int(&self.block_id, fpos as usize, val, true);
    }

    pub fn next_after(&mut self, slot: Option<i32>) -> Option<i32> {
        self.search_after(slot, Slot::Used)
    }

    pub fn insert_after(&mut self, slot: Option<i32>) -> Option<i32> {
        let new_slot = self.search_after(slot, Slot::Empty);
        if let Some(new_slot) = new_slot {
            self.set_flag(new_slot, Slot::Used);
        }
        new_slot
    }

    pub fn delete(&mut self, slot: i32) {
        self.set_flag(slot, Slot::Empty);
    }

    // Formats the page by setting all slots to empty.
    pub fn format(&mut self) {
        let mut slot = 0;
        while self.is_valid_slot(slot) {
            self.tx.lock().unwrap().set_int(&self.block_id, self.offset(slot) as usize, Slot::Empty as i32, false);
            for field in self.layout.schema().fields() {
                let fpos = self.offset(slot) + self.layout.offset(&field);
                if self.layout.schema().ftype(&field) == 4 {
                    self.tx.lock().unwrap().set_int(&self.block_id, fpos as usize, 0, false);
                }
                // TODO: Add support for VARCHAR fields.
            }
            slot += 1;
        }
    }

    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }

    fn set_flag(&mut self, slot: i32, flag: Slot) {
        self.tx.lock().unwrap().set_int(&self.block_id, self.offset(slot) as usize, flag as i32, true);
    }

    // Finds the next empty or used slot after the specified slot.
    // If the slot is None, then the search starts at the beginning of the page.
    // The search continues until a slot is found with the specified flag.
    // If no slot is found, then None is returned.
    fn search_after(&mut self, slot: Option<i32>, flag: Slot) -> Option<i32> {
        let mut new_slot = 0;
        if let Some(slot) = slot {
            new_slot = slot + 1;
        }
        while self.is_valid_slot(new_slot) {
            let x = self.tx.lock().unwrap().get_int(&self.block_id, self.offset(new_slot) as usize).unwrap();
            if x == flag as i32 {
                return Some(new_slot)
            }
            new_slot += 1;
        }
        None
    }

    // The slot is valid if it fits within the layout size and the file block size.
    // Say the file block size is 400 bytes and the layout slot size is 12 bytes.
    // Then the number of slots in the block is 400 / 12 = 33.
    // If the slot is 33, then the offset is 33 * 12 = 396, which is less than 400.
    // If the slot is 34, then the offset is 34 * 12 = 408, which is greater than 400.
    // So the valid slots are 0 to 32.
    fn is_valid_slot(&self, slot: i32) -> bool {
        self.offset(slot + 1) <= self.tx.lock().unwrap().block_size() as i32
    }

    fn offset(&self, slot: i32) -> i32 {
        slot * self.layout.slot_size()
    }
}

mod tests {
    use std::path::PathBuf;
    use super::*;
    use crate::file::filemgr::FileMgr;
    use crate::record::schema::Schema;
    use std::sync::{Arc, Mutex};
    use rand::Rng;
    use crate::buffer::buffermgr::BufferMgr;
    use crate::log::logmgr::LogMgr;

    #[test]
    fn test_record_page() {
        let fm = Arc::new(FileMgr::new(PathBuf::from("recoverytestdb"), 400));
        let lm = Arc::new(Mutex::new(LogMgr::new(fm.clone(), "testlog.log")));
        let bm = Arc::new(Mutex::new(BufferMgr::new(fm.clone(), lm.clone(), 3)));
        let mut tx = Arc::new(Mutex::new(Transaction::new(fm.clone(), bm.clone(), lm.clone())));

        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 9);
        let layout = Layout::new(schema);
        for fname in layout.schema().fields() {
            let offset = layout.offset(&fname);
            println!("Field {} starts at offset {}", fname, offset);
        }

        let block = tx.lock().unwrap().append("testfile");
        tx.lock().unwrap().pin(&block);

        let mut rp = RecordPage::new(tx.clone(), block.clone(), layout);
        rp.format();

        println!("Filling the page with random records...");
        let mut slot = rp.insert_after(None);
        while slot.is_some() {
            let num = rand::rng().random_range(0..50);
            rp.set_int(slot.unwrap(), "A", num);
            rp.set_int(slot.unwrap(), "B", num);
            println!("Inserting into slot {}: ({}, {})", slot.unwrap(), num, num);
            slot = rp.insert_after(slot);
        }

        println!("Deleting records with A < 25...");
        let mut count = 0;
        slot = rp.next_after(None);
        while slot.is_some() {
            if rp.get_int(slot.unwrap(), "A") < 25 {
                rp.delete(slot.unwrap());
                count += 1;
            }
            slot = rp.next_after(slot);
        }
        println!("Deleted {} records", count);

        println!("Here are the remaining records:");
        slot = rp.next_after(None);
        while slot.is_some() {
            println!("Slot {}: ({}, {})", slot.unwrap(), rp.get_int(slot.unwrap(), "A"), rp.get_int(slot.unwrap(), "B"));
            slot = rp.next_after(slot);
        }

        tx.lock().unwrap().unpin(&block);
        tx.lock().unwrap().commit();
    }
}