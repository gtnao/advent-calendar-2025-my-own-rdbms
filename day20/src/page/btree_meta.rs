use super::PAGE_SIZE;

// B-Tree Meta Page Layout:
//   [0]      node_type: u8 = 2 (meta)
//   [1..5]   root_page_id: u32 (u32::MAX = no root yet)
//   [5..PAGE_SIZE] reserved

pub const NODE_TYPE_META: u8 = 2;
pub const NO_ROOT: u32 = u32::MAX;

pub struct MetaNode;

impl MetaNode {
    pub fn init(data: &mut [u8]) {
        assert!(data.len() >= PAGE_SIZE);
        data[..PAGE_SIZE].fill(0);
        data[0] = NODE_TYPE_META;
        // Set root_page_id = NO_ROOT
        data[1..5].copy_from_slice(&NO_ROOT.to_le_bytes());
    }

    #[allow(dead_code)]
    pub fn is_meta(data: &[u8]) -> bool {
        data[0] == NODE_TYPE_META
    }

    pub fn get_root_page_id(data: &[u8]) -> Option<u32> {
        let page_id = u32::from_le_bytes(data[1..5].try_into().unwrap());
        if page_id == NO_ROOT {
            None
        } else {
            Some(page_id)
        }
    }

    pub fn set_root_page_id(data: &mut [u8], root_page_id: Option<u32>) {
        let value = root_page_id.unwrap_or(NO_ROOT);
        data[1..5].copy_from_slice(&value.to_le_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta_init() {
        let mut data = vec![0u8; PAGE_SIZE];
        MetaNode::init(&mut data);

        assert!(MetaNode::is_meta(&data));
        assert_eq!(MetaNode::get_root_page_id(&data), None);
    }

    #[test]
    fn test_meta_set_root() {
        let mut data = vec![0u8; PAGE_SIZE];
        MetaNode::init(&mut data);

        MetaNode::set_root_page_id(&mut data, Some(42));
        assert_eq!(MetaNode::get_root_page_id(&data), Some(42));

        MetaNode::set_root_page_id(&mut data, None);
        assert_eq!(MetaNode::get_root_page_id(&data), None);
    }
}
