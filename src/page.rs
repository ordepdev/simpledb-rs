pub struct Page {
    pub bytebuffer: Vec<u8>
}

impl Page {
    pub fn new(blocksize: u32) -> Page {
        Page { bytebuffer: vec![0; blocksize as usize] }
    }

    pub fn get_byte(&self, offset: usize) -> Option<u8> {
        self.bytebuffer.get(offset).copied()
    }

    pub fn set_byte(&mut self, offset: usize, value: u8) {
        self.bytebuffer[offset] = value;
    }

    pub fn get_bytes(&self, offset: usize) -> &[u8] {
        let len = self.get_byte(offset).unwrap() as usize;
        &self.bytebuffer[offset + 4..offset + 4 + len]
    }

    pub fn set_bytes(&mut self, offset: usize, value: &[u8]) {
        self.set_byte(offset, value.len() as u8);
        self.bytebuffer[offset + 4..offset + 4 + value.len()].copy_from_slice(value);
    }

    pub fn get_int(&self, offset: usize) -> i32 {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(&self.bytebuffer[offset..offset + 4]);
        i32::from_be_bytes(bytes)
    }

    pub fn set_int(&mut self, offset: usize, value: i32) {
        self.bytebuffer[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
    }

    pub fn get_long(&self, offset: usize) -> i64 {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(&self.bytebuffer[offset..offset + 8]);
        i64::from_be_bytes(bytes)
    }

    pub fn set_long(&mut self, offset: usize, value: i64) {
        self.bytebuffer[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
    }

    pub fn get_string(&self, offset: usize) -> String {
        let bytes = self.get_bytes(offset);
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    pub fn set_string(&mut self, offset: usize, value: &str) {
        self.set_bytes(offset, value.as_bytes());
    }

    pub fn set_bool(&mut self, offset: usize, value: bool) {
        self.set_byte(offset, value as u8);
    }

    pub fn get_bool(&self, offset: usize) -> bool {
        self.get_byte(offset).unwrap() != 0
    }

    pub fn max_length(strlen: usize) -> usize {
        let max_bytes_per_char = 1;
        4 + strlen * max_bytes_per_char
    }

    pub(crate) fn contents(&mut self) -> &mut Vec<u8> {
        &mut self.bytebuffer
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_page_int() {
        let mut page = Page::new(10);
        page.set_int(0, -355);
        page.set_int(4, 499);

        println!("{:?}", page.contents());

        assert_eq!(page.get_int(0), -355);
        assert_eq!(page.get_int(4), 499);
    }

    #[test]
    fn test_page_long() {
        let mut page = Page::new(20);
        page.set_long(0, 1234567890123);
        page.set_long(8, 9876543210123);

        println!("{:?}", page.contents());

        assert_eq!(page.get_long(0), 1234567890123);
        assert_eq!(page.get_long(8), 9876543210123);
    }

    #[test]
    fn test_page_bytes() {
        let mut page = Page::new(20);

        page.set_bytes(0, &[1, 2, 3]);
        page.set_bytes(7, &[4, 5, 6]);

        println!("{:?}", page.contents());

        assert_eq!(page.get_byte(0), Some(3));
        assert_eq!(page.get_byte(1), Some(0));
        assert_eq!(page.get_bytes(0), &[1, 2, 3]);
        assert_eq!(page.get_byte(7), Some(3));
        assert_eq!(page.get_bytes(7), &[4, 5, 6]);
    }

    #[test]
    fn test_page_string() {
        let mut page = Page::new(20);
        page.set_string(0, "hello");
        page.set_string(0 + Page::max_length("world".len()), "world");

        println!("{:?}", page.contents());

        assert_eq!(page.get_string(0), "hello");
        assert_eq!(page.get_string(0 + Page::max_length("world".len())), "world");
    }

    #[test]
    fn test_page_bool() {
        let mut page = Page::new(2);
        page.set_bool(0, true);
        page.set_bool(1, false);

        println!("{:?}", page.contents());

        assert_eq!(page.get_bool(0), true);
        assert_eq!(page.get_bool(1), false);
    }
}