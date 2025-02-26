use std::fmt::Display;

#[derive(Clone, Debug, Eq, Hash)]
pub struct BlockId {
    pub filename: String,
    pub number: usize,
}

impl BlockId {
    pub fn new(filename: &str, number: usize) -> BlockId {
        BlockId { filename: filename.to_string(), number }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn number(&self) -> usize {
        self.number
    }
}

impl PartialEq for BlockId {
    fn eq(&self, other: &BlockId) -> bool {
        self.filename == other.filename && self.number == other.number
    }
}

impl Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[file {}, block {}]", self.filename, self.number)
    }
}

mod tests {
    use super::*;

    #[test]
    fn test_block_id() {
        let block_id = BlockId::new("test.txt", 42);
        assert_eq!(block_id.filename(), "test.txt");
        assert_eq!(block_id.number(), 42);
        assert_eq!(block_id.to_string(), "[file test.txt, block 42]");
    }
}
