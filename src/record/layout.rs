use crate::file::page::Page;
use crate::record::schema::Schema;
use std::collections::HashMap;

pub struct Layout {
    schema: Schema,
    offsets: HashMap<String, i32>,
    slot_size: i32,
}

impl Layout {
    pub fn new(schema: Schema) -> Layout {
        let mut offsets = HashMap::new();
        let mut pos = 4; // 4 bytes for the flag.
        for field in schema.fields() {
            offsets.insert(field.clone(), pos);
            let length_in_bytes = match schema.ftype(&field) {
                4 => 4,
                12 => Page::max_length(schema.length(&field) as usize) as i32,
                _ => panic!("Unexpected field type: {}", schema.ftype(&field)),
            };
            pos += length_in_bytes;
        }
        Layout {
            schema,
            offsets,
            slot_size: pos,
        }
    }

    pub fn offset(&self, field: &str) -> i32 {
        *self.offsets.get(field).unwrap()
    }

    pub fn slot_size(&self) -> i32 {
        self.slot_size
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_layout() {
        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 9);
        schema.add_int_field("C");
        let layout = Layout::new(schema);

        assert_eq!(layout.offset("A"), 4);
        assert_eq!(layout.offset("B"), 8);
        assert_eq!(layout.offset("C"), 21);
        assert_eq!(layout.slot_size(), 25);
    }
}