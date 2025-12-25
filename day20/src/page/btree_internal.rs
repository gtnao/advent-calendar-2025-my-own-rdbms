use crate::btree::IndexKey;
use super::PAGE_SIZE;

// B-Tree Internal Node Page Layout:
//   Header (16 bytes):
//     [0]      node_type: u8 = 1 (internal)
//     [1..3]   key_count: u16
//     [3..5]   free_space_offset: u16
//     [5..9]   rightmost_child: u32 (child for keys >= last key)
//     [9..16]  reserved
//   Slot array (grows forward from offset 16):
//     each slot: [offset: u16][length: u16] = 4 bytes
//   Free space
//   Entries (grow backward from end of page):
//     each entry: [key_data...][child_page_id: u32]
//
// Semantics:
//   Entry i = (key_i, child_i) where child_i leads to keys < key_i
//   rightmost_child leads to keys >= last key
//
// For keys [10, 20, 30] with children [P0, P1, P2] and rightmost P3:
//   P0: keys < 10
//   P1: 10 <= keys < 20
//   P2: 20 <= keys < 30
//   P3: keys >= 30

const HEADER_SIZE: usize = 16;
const SLOT_SIZE: usize = 4;
const CHILD_SIZE: usize = 4; // page_id is u32

pub const NODE_TYPE_INTERNAL: u8 = 1;

pub struct InternalNode;

impl InternalNode {
    // === Initialization ===

    /// Initialize an internal node with a single child (rightmost).
    /// Used when creating a new root after split.
    pub fn init(data: &mut [u8], rightmost_child: u32) {
        assert!(data.len() >= PAGE_SIZE);
        data[..HEADER_SIZE].fill(0);
        data[0] = NODE_TYPE_INTERNAL;
        // key_count = 0
        data[1..3].copy_from_slice(&0u16.to_le_bytes());
        // free_space_offset = PAGE_SIZE
        data[3..5].copy_from_slice(&(PAGE_SIZE as u16).to_le_bytes());
        // rightmost_child
        data[5..9].copy_from_slice(&rightmost_child.to_le_bytes());
    }

    // === Read operations ===

    #[allow(dead_code)]
    pub fn is_leaf(data: &[u8]) -> bool {
        data[0] == super::btree_leaf::NODE_TYPE_LEAF
    }

    pub fn key_count(data: &[u8]) -> u16 {
        u16::from_le_bytes(data[1..3].try_into().unwrap())
    }

    fn set_key_count(data: &mut [u8], count: u16) {
        data[1..3].copy_from_slice(&count.to_le_bytes());
    }

    fn free_space_offset(data: &[u8]) -> u16 {
        u16::from_le_bytes(data[3..5].try_into().unwrap())
    }

    fn set_free_space_offset(data: &mut [u8], offset: u16) {
        data[3..5].copy_from_slice(&offset.to_le_bytes());
    }

    pub fn get_rightmost_child(data: &[u8]) -> u32 {
        u32::from_le_bytes(data[5..9].try_into().unwrap())
    }

    pub fn set_rightmost_child(data: &mut [u8], child: u32) {
        data[5..9].copy_from_slice(&child.to_le_bytes());
    }

    // Slot access
    fn get_slot(data: &[u8], idx: u16) -> (u16, u16) {
        let slot_offset = HEADER_SIZE + (idx as usize) * SLOT_SIZE;
        let offset = u16::from_le_bytes(data[slot_offset..slot_offset + 2].try_into().unwrap());
        let length = u16::from_le_bytes(data[slot_offset + 2..slot_offset + 4].try_into().unwrap());
        (offset, length)
    }

    fn set_slot(data: &mut [u8], idx: u16, offset: u16, length: u16) {
        let slot_offset = HEADER_SIZE + (idx as usize) * SLOT_SIZE;
        data[slot_offset..slot_offset + 2].copy_from_slice(&offset.to_le_bytes());
        data[slot_offset + 2..slot_offset + 4].copy_from_slice(&length.to_le_bytes());
    }

    pub fn get_key(data: &[u8], idx: u16) -> IndexKey {
        let (offset, length) = Self::get_slot(data, idx);
        let entry_data = &data[offset as usize..(offset + length) as usize];
        // Entry format: [key_data][child_page_id]
        let key_len = length as usize - CHILD_SIZE;
        IndexKey::deserialize(&entry_data[..key_len])
    }

    pub fn get_child(data: &[u8], idx: u16) -> u32 {
        let (offset, length) = Self::get_slot(data, idx);
        let entry_end = (offset + length) as usize;
        let child_start = entry_end - CHILD_SIZE;
        u32::from_le_bytes(data[child_start..child_start + 4].try_into().unwrap())
    }

    pub fn set_child(data: &mut [u8], idx: u16, child: u32) {
        let (offset, length) = Self::get_slot(data, idx);
        let entry_end = (offset + length) as usize;
        let child_start = entry_end - CHILD_SIZE;
        data[child_start..child_start + 4].copy_from_slice(&child.to_le_bytes());
    }

    // === Search ===

    /// Find the child page to follow for the given key.
    /// Returns the page_id of the appropriate child.
    pub fn find_child(data: &[u8], key: &IndexKey) -> u32 {
        let count = Self::key_count(data);

        // Linear search (can optimize to binary search later)
        for i in 0..count {
            let k = Self::get_key(data, i);
            if *key < k {
                return Self::get_child(data, i);
            }
        }
        // Key >= all keys, return rightmost child
        Self::get_rightmost_child(data)
    }

    /// Find insertion point for key. Returns index where key should be inserted.
    fn find_insert_point(data: &[u8], key: &IndexKey) -> u16 {
        let count = Self::key_count(data);
        for i in 0..count {
            let k = Self::get_key(data, i);
            if *key < k {
                return i;
            }
        }
        count
    }

    // === Free space ===

    fn free_space(data: &[u8]) -> usize {
        let count = Self::key_count(data) as usize;
        let slots_end = HEADER_SIZE + count * SLOT_SIZE;
        let free_offset = Self::free_space_offset(data) as usize;
        if free_offset > slots_end {
            free_offset - slots_end
        } else {
            0
        }
    }

    pub fn has_space_for(data: &[u8], key: &IndexKey) -> bool {
        let entry_size = key.serialized_size() + CHILD_SIZE;
        let required = entry_size + SLOT_SIZE;
        Self::free_space(data) >= required
    }

    // === Insert ===

    /// Insert a key and its left child pointer.
    /// When we split a child, we push up a key. The key's left child is the original node,
    /// and the right child (new split node) becomes the next entry's left child or rightmost.
    ///
    /// insert(key, left_child) inserts the entry (key, left_child).
    /// The caller must update rightmost_child or the next entry appropriately.
    pub fn insert(data: &mut [u8], key: &IndexKey, left_child: u32) -> Result<(), NodeFull> {
        let key_bytes = key.serialize();
        let entry_size = key_bytes.len() + CHILD_SIZE;
        let required = entry_size + SLOT_SIZE;

        if Self::free_space(data) < required {
            return Err(NodeFull);
        }

        let count = Self::key_count(data);
        let insert_idx = Self::find_insert_point(data, key);

        // Allocate space for entry
        let new_offset = Self::free_space_offset(data) - entry_size as u16;

        // Write entry: [key_data][child_page_id]
        let offset = new_offset as usize;
        data[offset..offset + key_bytes.len()].copy_from_slice(&key_bytes);
        data[offset + key_bytes.len()..offset + key_bytes.len() + 4]
            .copy_from_slice(&left_child.to_le_bytes());

        // Shift slots to make room
        for i in (insert_idx..count).rev() {
            let (off, len) = Self::get_slot(data, i);
            Self::set_slot(data, i + 1, off, len);
        }

        // Set new slot
        Self::set_slot(data, insert_idx, new_offset, entry_size as u16);

        // Update header
        Self::set_key_count(data, count + 1);
        Self::set_free_space_offset(data, new_offset);

        Ok(())
    }

    // === Split ===

    pub fn need_split(data: &[u8], key: &IndexKey) -> bool {
        !Self::has_space_for(data, key)
    }

    /// Split this internal node. Returns (split_key, new_rightmost_for_src).
    /// After split:
    /// - src contains keys[0..mid-1] with original children, rightmost = child[mid-1]
    /// - dst contains keys[mid+1..] with children, rightmost = original rightmost
    /// - split_key = keys[mid] is pushed up to parent
    pub fn split(src: &mut [u8], dst: &mut [u8]) -> (IndexKey, u32) {
        let count = Self::key_count(src);
        let mid = count / 2;

        // Get the middle key (this will be pushed up)
        let split_key = Self::get_key(src, mid);
        let mid_child = Self::get_child(src, mid);

        // Initialize dst with rightmost_child from src
        let src_rightmost = Self::get_rightmost_child(src);
        InternalNode::init(dst, src_rightmost);

        // Copy entries [mid+1..count) to dst
        for i in (mid + 1)..count {
            let key = Self::get_key(src, i);
            let child = Self::get_child(src, i);
            Self::insert(dst, &key, child).expect("dst should have space");
        }

        // Truncate src to keep entries [0..mid)
        // The new rightmost_child for src is mid_child (the child that was with split_key)
        let mut write_offset = PAGE_SIZE;
        for i in 0..mid {
            let key = Self::get_key(src, i);
            let child = Self::get_child(src, i);
            let key_bytes = key.serialize();
            let entry_size = key_bytes.len() + CHILD_SIZE;

            write_offset -= entry_size;
            let offset = write_offset;
            src[offset..offset + key_bytes.len()].copy_from_slice(&key_bytes);
            src[offset + key_bytes.len()..offset + key_bytes.len() + 4]
                .copy_from_slice(&child.to_le_bytes());

            Self::set_slot(src, i, write_offset as u16, entry_size as u16);
        }

        Self::set_key_count(src, mid);
        Self::set_free_space_offset(src, write_offset as u16);
        Self::set_rightmost_child(src, mid_child);

        (split_key, mid_child)
    }
}

#[derive(Debug)]
pub struct NodeFull;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuple::Value;

    #[test]
    fn test_internal_init() {
        let mut data = vec![0u8; PAGE_SIZE];
        InternalNode::init(&mut data, 100);

        assert!(!InternalNode::is_leaf(&data));
        assert_eq!(InternalNode::key_count(&data), 0);
        assert_eq!(InternalNode::get_rightmost_child(&data), 100);
    }

    #[test]
    fn test_internal_insert_and_find() {
        let mut data = vec![0u8; PAGE_SIZE];
        InternalNode::init(&mut data, 100); // rightmost child = page 100

        // Insert key 20 with child 10 (keys < 20 go to page 10)
        let key20 = IndexKey::new(vec![Value::Int(20)]);
        InternalNode::insert(&mut data, &key20, 10).unwrap();

        // Insert key 40 with child 30 (keys < 40 go to page 30)
        let key40 = IndexKey::new(vec![Value::Int(40)]);
        InternalNode::insert(&mut data, &key40, 30).unwrap();

        // Insert key 10 with child 5 (keys < 10 go to page 5)
        let key10 = IndexKey::new(vec![Value::Int(10)]);
        InternalNode::insert(&mut data, &key10, 5).unwrap();

        assert_eq!(InternalNode::key_count(&data), 3);

        // Test find_child
        // key 5 < 10, should go to child of key 10, which is 5
        assert_eq!(
            InternalNode::find_child(&data, &IndexKey::new(vec![Value::Int(5)])),
            5
        );

        // key 15: 10 <= 15 < 20, should go to child of key 20, which is 10
        assert_eq!(
            InternalNode::find_child(&data, &IndexKey::new(vec![Value::Int(15)])),
            10
        );

        // key 25: 20 <= 25 < 40, should go to child of key 40, which is 30
        assert_eq!(
            InternalNode::find_child(&data, &IndexKey::new(vec![Value::Int(25)])),
            30
        );

        // key 50: >= 40, should go to rightmost child, which is 100
        assert_eq!(
            InternalNode::find_child(&data, &IndexKey::new(vec![Value::Int(50)])),
            100
        );
    }

    #[test]
    fn test_internal_split() {
        let mut src = vec![0u8; PAGE_SIZE];
        let mut dst = vec![0u8; PAGE_SIZE];
        InternalNode::init(&mut src, 999);

        // Insert many keys
        for i in 0..30 {
            let key = IndexKey::new(vec![Value::Int(i * 10)]);
            InternalNode::insert(&mut src, &key, i as u32).unwrap();
        }

        let (split_key, _) = InternalNode::split(&mut src, &mut dst);

        let src_count = InternalNode::key_count(&src);
        let dst_count = InternalNode::key_count(&dst);

        // Both should have entries
        assert!(src_count > 0);
        assert!(dst_count > 0);

        // All keys in src should be < split_key
        for i in 0..src_count {
            assert!(InternalNode::get_key(&src, i) < split_key);
        }

        // All keys in dst should be > split_key (split_key itself is pushed up)
        for i in 0..dst_count {
            assert!(InternalNode::get_key(&dst, i) > split_key);
        }
    }

    #[test]
    fn test_set_child() {
        let mut data = vec![0u8; PAGE_SIZE];
        InternalNode::init(&mut data, 100);

        // Insert entries
        let key10 = IndexKey::new(vec![Value::Int(10)]);
        let key20 = IndexKey::new(vec![Value::Int(20)]);
        let key30 = IndexKey::new(vec![Value::Int(30)]);
        InternalNode::insert(&mut data, &key10, 1).unwrap();
        InternalNode::insert(&mut data, &key20, 2).unwrap();
        InternalNode::insert(&mut data, &key30, 3).unwrap();

        // Verify initial children
        assert_eq!(InternalNode::get_child(&data, 0), 1);
        assert_eq!(InternalNode::get_child(&data, 1), 2);
        assert_eq!(InternalNode::get_child(&data, 2), 3);

        // Update children using set_child
        InternalNode::set_child(&mut data, 0, 11);
        InternalNode::set_child(&mut data, 1, 22);
        InternalNode::set_child(&mut data, 2, 33);

        // Verify updated children
        assert_eq!(InternalNode::get_child(&data, 0), 11);
        assert_eq!(InternalNode::get_child(&data, 1), 22);
        assert_eq!(InternalNode::get_child(&data, 2), 33);

        // Keys should be unchanged
        assert_eq!(InternalNode::get_key(&data, 0), key10);
        assert_eq!(InternalNode::get_key(&data, 1), key20);
        assert_eq!(InternalNode::get_key(&data, 2), key30);

        // find_child should work correctly with updated children
        assert_eq!(
            InternalNode::find_child(&data, &IndexKey::new(vec![Value::Int(5)])),
            11
        );
        assert_eq!(
            InternalNode::find_child(&data, &IndexKey::new(vec![Value::Int(15)])),
            22
        );
        assert_eq!(
            InternalNode::find_child(&data, &IndexKey::new(vec![Value::Int(25)])),
            33
        );
        assert_eq!(
            InternalNode::find_child(&data, &IndexKey::new(vec![Value::Int(50)])),
            100
        );
    }
}
