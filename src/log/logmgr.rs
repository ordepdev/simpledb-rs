use std::sync::Arc;
use crate::file::blockid::BlockId;
use crate::file::page::Page;
use crate::file::filemgr::FileMgr;
use crate::log::logiterator::LogIterator;

// The log manager is responsible for writing log records
// to the log file. The tail of the log file is kept in a
// bytebuffer in memory, which is flushed to disk when it
// becomes full.
pub struct LogMgr {
    fm: Arc<FileMgr>,
    file: String,
    page: Page,
    current_block: BlockId,
    latest_lsn: i32,
    last_saved_lsn: i32,
}

impl LogMgr {

    // Creates a new log manager for the specified log file.
    // If the log file does not exist, it is created with an
    // empty first block.
    pub fn new(fm: Arc<FileMgr>, file: &str) -> LogMgr {
        let buffer = vec![0; fm.block_size()];
        let mut page = Page::wrap(buffer);
        let logsize = fm.length(file);
        let current_block = if logsize == 0 {
            let block = fm.append(file);
            page.set_int(0, fm.block_size() as i32);
            fm.write(&block, &page);
            block
        } else {
            let block = BlockId::new(file, logsize as usize - 1);
            fm.read(&block, &mut page);
            block
        };
        LogMgr {
            fm,
            file: file.to_string(),
            page,
            current_block,
            latest_lsn: 0,
            last_saved_lsn: 0
        }
    }

    // Flushes the log record with the specified LSN to disk.
    // All log records with LSN less than the specified value
    // are also written to disk.
    pub(crate) fn flush_record(&mut self, lsn: i32) {
        if lsn >= self.last_saved_lsn {
            self.flush()
        }
    }

    // Appends a new log record to the log file and returns
    // the LSN of the new record. Records are written right
    // to left in the buffer. The beginning of the log record
    // contains the location of the last-written record, called
    // the "boundary" enabling the iterator to read the records
    // in reverse order starting from the position where the last
    // record was written.
    pub(crate) fn append(&mut self, record: &Vec<u8>) -> i32 {
        let mut boundary = self.page.get_int(0);
        let record_size = record.len() as i32;
        let bytes_needed = record_size + 4;
        if boundary - bytes_needed < 4 {
            // If the log record doesn't fit in the current block
            // we need to flush the current block and move to a
            // new block.
            self.flush();
            self.current_block = self.append_new_block();
            boundary = self.page.get_int(0);
        }
        let record_position = boundary - bytes_needed;
        self.page.set_bytes(record_position as usize, &record);
        self.page.set_int(0, record_position);
        self.latest_lsn += 1;
        self.latest_lsn
    }

    // Returns an iterator that reads log records from the log
    // file in reverse order starting from the most recent record.
    // It flushes the current block before returning the iterator
    // to ensure the entire log file is on disk.
    pub(crate) fn iterator(&mut self) -> LogIterator {
        self.flush();
        LogIterator::new(self.fm.clone(), &self.current_block)
    }

    fn append_new_block(&mut self) -> BlockId {
        let block = self.fm.append(&self.file);
        self.page.set_int(0, self.fm.block_size() as i32);
        self.fm.write(&block, &self.page);
        block
    }

    fn flush(&mut self) {
        self.fm.write(&self.current_block, &self.page);
        self.last_saved_lsn = self.latest_lsn;
    }
}

mod tests {
    use super::*;
    use crate::file::filemgr::FileMgr;
    use std::path::PathBuf;

    #[test]
    fn test_log_mgr() {
        let block_size = 400;
        let fm = Arc::new(FileMgr::new(PathBuf::from("testdb"), block_size));
        let mut lm = LogMgr::new(fm, "testlog.log");
        print_log_records(&mut lm, "The inital empty log file:");
        create_log_records(&mut lm, 1, 35);
        print_log_records(&mut lm, "The log file now has these records:");
        create_log_records(&mut lm, 36, 70);
        lm.flush_record(65);
        print_log_records(&mut lm, "The log file now has these records:");
    }

    fn print_log_records(lm: &mut LogMgr, message: &str) {
        println!("{}", message);
        let mut iter = lm.iterator();
        while let Some(record) = iter.next() {
            let page = Page::wrap(record);
            let string = page.get_string(0);
            let number = page.get_int(Page::max_length(string.len()));
            println!("[{}, {}]", string, number);
        }
    }

    fn create_log_records(lm: &mut LogMgr, start: u32, end: u32) {
        println!("Appending log records from {} to {}", start, end);
        for i in start..end+1 {
            let record = create_log_record(&format!("record{}", i), 100 + i);
            let lsn = lm.append(&record);
            print!("{} ", lsn);
        }
        println!();
    }

    fn create_log_record(string: &str, number: u32) -> Vec<u8> {
        let string_pos = 0;
        let number_pos = string_pos + Page::max_length(string.len());
        let buffer = vec![0; number_pos + 4];
        let mut page = Page::wrap(buffer);
        page.set_string(string_pos, string);
        page.set_int(number_pos, number as i32);
        page.contents().to_vec()
    }
}