use std::collections::HashMap;

pub enum FieldType {
    INTEGER = 4,
    VARCHAR = 12,
}

#[derive(Clone)]
struct FieldInfo {
    ftype: i32,
    length: i32,
}

#[derive(Clone)]
pub struct Schema {
    fields: Vec<String>,
    info: HashMap<String, FieldInfo>
}

// The Schema struct holds a record's _schema_, the name and type of each field, and the
// length of each field. A schema can be thought of as a list of triples, each consisting of
// a field name, a field type, and a field length.
impl Schema {
    pub fn new() -> Schema {
        Schema {
            fields: Vec::new(),
            info: HashMap::new()
        }
    }

    pub fn add_field(&mut self, field: &str, ftype: i32, length: i32) {
        self.fields.push(String::from(field));
        self.info.insert(String::from(field), FieldInfo { ftype, length });
    }

    pub fn add_int_field(&mut self, field: &str) {
        self.add_field(field, FieldType::INTEGER as i32, 0);
    }

    pub fn add_string_field(&mut self, field: &str, length: i32) {
        self.add_field(field, FieldType::VARCHAR as i32, length);
    }

    pub fn add(&mut self, field: &str, schema: &Schema) {
        let ftype = schema.ftype(field);
        let length = schema.length(field);
        self.add_field(field, ftype, length);
    }

    pub fn add_all(&mut self, schema: &Schema) {
        for field in schema.fields.iter() {
            self.add(field, schema);
        }
    }

    pub fn fields(&self) -> Vec<String> {
        self.fields.clone()
    }

    pub fn has_field(&self, field: &str) -> bool {
        self.info.contains_key(field)
    }

    pub fn ftype(&self, field: &str) -> i32 {
        self.info.get(field).unwrap().ftype
    }

    pub fn length(&self, field: &str) -> i32 {
        self.info.get(field).unwrap().length
    }
}