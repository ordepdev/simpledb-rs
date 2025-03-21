use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::tx::transaction::Transaction;
use crate::record::layout::{Layout, Schema};
use crate::record::tablescan::{TableScan, UpdateScan};

struct TableMgr {
    table_catalog_layout: Layout,
    field_catalog_layout: Layout,
}

impl TableMgr {

    const MAX_NAME: i32 = 16;

    fn new (is_new: bool, tx: Arc<Mutex<Transaction>>) -> TableMgr {
        let mut table_catalog_schema = Schema::new();
        table_catalog_schema.add_string_field("table_name", TableMgr::MAX_NAME);
        table_catalog_schema.add_int_field("slot_size");
        let table_catalog_layout = Layout::new(table_catalog_schema.clone());

        let mut field_catalog_schema = Schema::new();
        field_catalog_schema.add_string_field("table_name", TableMgr::MAX_NAME);
        field_catalog_schema.add_string_field("field_name", TableMgr::MAX_NAME);
        field_catalog_schema.add_int_field("field_type");
        field_catalog_schema.add_int_field("field_length");
        field_catalog_schema.add_int_field("field_offset");
        let field_catalog_layout = Layout::new(field_catalog_schema.clone());

        let tm = TableMgr { table_catalog_layout, field_catalog_layout };

        if is_new {
            tm.create_table("tblcat", &table_catalog_schema, tx.clone());
            tm.create_table("fldcat", &field_catalog_schema, tx.clone());
        }

        tm
    }

    fn create_table(&self, name: &str, schema: &Schema, tx: Arc<Mutex<Transaction>>) {
        let layout = Layout::new(schema.clone());
        let mut table_catalog = TableScan::new(tx.clone(), self.table_catalog_layout.clone(), "tblcat");
        table_catalog.insert();
        table_catalog.set_string("table_name", name);
        table_catalog.set_int("slot_size", layout.slot_size());
        table_catalog.close();

        let mut field_catalog = TableScan::new(tx.clone(), self.field_catalog_layout.clone(), "fldcat");
        for field_name in schema.fields() {
            field_catalog.insert();
            field_catalog.set_string("table_name", name);
            field_catalog.set_string("field_name", &field_name);
            field_catalog.set_int("field_type", schema.ftype(&field_name));
            field_catalog.set_int("field_length", schema.length(&field_name));
            field_catalog.set_int("field_offset", layout.offset(&field_name));
        }
        field_catalog.close();
    }


    fn layout(&self, table: &str, tx: Arc<Mutex<Transaction>>) -> Layout {
        let mut slot_size = -1;
        let mut table_catalog = TableScan::new(tx.clone(), self.table_catalog_layout.clone(), "tblcat");
        while table_catalog.next() {
            if table_catalog.get_string("table_name") == table {
                slot_size = table_catalog.get_int("slot_size");
                break;
            }
        }
        table_catalog.close();

        let mut schema = Schema::new();
        let mut offsets = HashMap::new();
        let mut fc = TableScan::new(tx.clone(), self.field_catalog_layout.clone(), "fldcat");
        while fc.next() {
            if fc.get_string("table_name") == table {
                let field_name = fc.get_string("field_name");
                let field_type = fc.get_int("field_type");
                let field_length = fc.get_int("field_length");
                let field_offset = fc.get_int("field_offset");
                offsets.insert(field_name.clone(), field_offset);
                schema.add_field(&field_name, field_type, field_length);
            }
        }
        fc.close();
        Layout::from(schema, offsets, slot_size)
    }
}

mod tests {
    use std::path::PathBuf;
    use crate::buffer::buffermgr::BufferMgr;
    use crate::file::filemgr::FileMgr;
    use crate::log::logmgr::LogMgr;
    use super::*;

    #[test]
    fn test_table_mgr() {
        let fm = Arc::new(FileMgr::new(PathBuf::from("tablescantestdb"), 400));
        let lm = Arc::new(Mutex::new(LogMgr::new(fm.clone(), "testlog.log")));
        let bm = Arc::new(Mutex::new(BufferMgr::new(fm.clone(), lm.clone(), 3)));
        let mut tx = Arc::new(Mutex::new(Transaction::new(fm.clone(), bm.clone(), lm.clone())));
        let tm = TableMgr::new(true, tx.clone());

        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 9);
        schema.add_int_field("C");
        tm.create_table("MyTable", &schema, tx.clone());

        let layout = tm.layout("MyTable", tx.clone());
        assert_eq!(layout.offset("A"), 4);
        assert_eq!(layout.offset("B"), 8);
        assert_eq!(layout.offset("C"), 21);
        assert_eq!(layout.slot_size(), 25);
        let schema = layout.schema();
        assert_eq!(schema.ftype("A"), 4);
        assert_eq!(schema.ftype("B"), 12);
        assert_eq!(schema.ftype("C"), 4);
        assert_eq!(schema.length("B"), 9);

        println!("MyTable fields:");
        for fname in schema.fields() {
            match schema.ftype(&fname) {
                4 => println!("{}: int", fname),
                12 => println!("{}: varchar({})", fname, schema.length(&fname)),
                _ => {}
            }
        }

        tx.lock().unwrap().commit();
    }

}