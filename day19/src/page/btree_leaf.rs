use crate::btree::IndexKey;
use crate::executor::Rid;
use super::PAGE_SIZE;

// B-Tree Leaf Node Page Layout:
//   Header (16 bytes):
//     [0]      node_type: u8 = 0 (leaf)
//     [1..3]   key_count: u16
//     [3..5]   free_space_offset: u16
//     [5..9]   prev_leaf_page_id: u32 (u32::MAX = none)
//     [9..13]  next_leaf_page_id: u32 (u32::MAX = none)
//     [13..16] reserved
//   Slot array (grows forward from offset 16):
//     each slot: [offset: u16][length: u16] = 4 bytes
//   Free space
//   Entries (grow backward from end of page):
//     each entry: [key_data...][page_id: u32][slot_id: u16] = key_len + 6 bytes

const HEADER_SIZE: usize = 16;
const SLOT_SIZE: usize = 4;
const RID_SIZE: usize = 6; // page_id (4) + slot_id (2)

pub const NODE_TYPE_LEAF: u8 = 0;
pub const INVALID_PAGE_ID: u32 = u32::MAX;

pub struct LeafNode;

impl LeafNode {
    // === Initialization ===

    pub fn init(data: &mut [u8]) {
        assert!(data.len() >= PAGE_SIZE);
        // Clear header
        data[..HEADER_SIZE].fill(0);
        // Set node type
        data[0] = NODE_TYPE_LEAF;
        // Set key count = 0
        data[1..3].copy_from_slice(&0u16.to_le_bytes());
        // Set free space offset = PAGE_SIZE
        data[3..5].copy_from_slice(&(PAGE_SIZE as u16).to_le_bytes());
        // Set prev/next leaf = INVALID
        data[5..9].copy_from_slice(&INVALID_PAGE_ID.to_le_bytes());
        data[9..13].copy_from_slice(&INVALID_PAGE_ID.to_le_bytes());
    }

    // === Read operations ===

    pub fn is_leaf(data: &[u8]) -> bool {
        data[0] == NODE_TYPE_LEAF
    }

    pub fn key_count(data: &[u8]) -> u16 {
        u16::from_le_bytes(data[1..3].try_into().unwrap())
    }

    fn free_space_offset(data: &[u8]) -> u16 {
        u16::from_le_bytes(data[3..5].try_into().unwrap())
    }

    fn set_free_space_offset(data: &mut [u8], offset: u16) {
        data[3..5].copy_from_slice(&offset.to_le_bytes());
    }

    fn set_key_count(data: &mut [u8], count: u16) {
        data[1..3].copy_from_slice(&count.to_le_bytes());
    }

    pub fn get_prev_leaf(data: &[u8]) -> Option<u32> {
        let page_id = u32::from_le_bytes(data[5..9].try_into().unwrap());
        if page_id == INVALID_PAGE_ID {
            None
        } else {
            Some(page_id)
        }
    }

    pub fn get_next_leaf(data: &[u8]) -> Option<u32> {
        let page_id = u32::from_le_bytes(data[9..13].try_into().unwrap());
        if page_id == INVALID_PAGE_ID {
            None
        } else {
            Some(page_id)
        }
    }

    pub fn set_prev_leaf(data: &mut [u8], page_id: Option<u32>) {
        let value = page_id.unwrap_or(INVALID_PAGE_ID);
        data[5..9].copy_from_slice(&value.to_le_bytes());
    }

    pub fn set_next_leaf(data: &mut [u8], page_id: Option<u32>) {
        let value = page_id.unwrap_or(INVALID_PAGE_ID);
        data[9..13].copy_from_slice(&value.to_le_bytes());
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
        // Entry format: [key_data][rid]
        let key_len = length as usize - RID_SIZE;
        IndexKey::deserialize(&entry_data[..key_len])
    }

    pub fn get_rid(data: &[u8], idx: u16) -> Rid {
        let (offset, length) = Self::get_slot(data, idx);
        let entry_end = (offset + length) as usize;
        let rid_start = entry_end - RID_SIZE;
        let page_id = u32::from_le_bytes(data[rid_start..rid_start + 4].try_into().unwrap());
        let slot_id = u16::from_le_bytes(data[rid_start + 4..rid_start + 6].try_into().unwrap());
        Rid { page_id, slot_id }
    }

    // === Search ===

    /// Binary search for key. Returns Some(rid) if found.
    pub fn search(data: &[u8], key: &IndexKey) -> Option<Rid> {
        let count = Self::key_count(data);
        if count == 0 {
            return None;
        }

        // Binary search
        let mut left = 0u16;
        let mut right = count;
        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = Self::get_key(data, mid);
            match key.cmp(&mid_key) {
                std::cmp::Ordering::Equal => return Some(Self::get_rid(data, mid)),
                std::cmp::Ordering::Less => right = mid,
                std::cmp::Ordering::Greater => left = mid + 1,
            }
        }
        None
    }

    /// Find insertion point for (key, rid). Returns index where entry should be inserted.
    /// Entries are ordered by (key, rid) to ensure stable ordering for duplicate keys.
    pub fn find_insert_point(data: &[u8], key: &IndexKey, rid: &Rid) -> u16 {
        let count = Self::key_count(data);
        if count == 0 {
            return 0;
        }

        // Binary search for first (key, rid) > (target_key, target_rid)
        let mut left = 0u16;
        let mut right = count;
        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = Self::get_key(data, mid);
            let mid_rid = Self::get_rid(data, mid);
            // Compare (key, rid) tuples
            let cmp = match mid_key.cmp(key) {
                std::cmp::Ordering::Equal => mid_rid.cmp(rid),
                other => other,
            };
            if cmp == std::cmp::Ordering::Less {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        left
    }

    /// Find insertion point for key only (used for search).
    /// Returns index of first entry with key >= target.
    pub fn find_key_position(data: &[u8], key: &IndexKey) -> u16 {
        let count = Self::key_count(data);
        if count == 0 {
            return 0;
        }

        let mut left = 0u16;
        let mut right = count;
        while left < right {
            let mid = left + (right - left) / 2;
            let mid_key = Self::get_key(data, mid);
            if mid_key < *key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        left
    }

    // === Free space calculation ===

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
        let entry_size = key.serialized_size() + RID_SIZE;
        let required = entry_size + SLOT_SIZE;
        Self::free_space(data) >= required
    }

    // === Insert ===

    /// Insert key-rid pair. Returns Err if no space.
    /// Entries are ordered by (key, rid) for stable duplicate key handling.
    pub fn insert(data: &mut [u8], key: &IndexKey, rid: Rid) -> Result<(), NodeFull> {
        let key_bytes = key.serialize();
        let entry_size = key_bytes.len() + RID_SIZE;
        let required = entry_size + SLOT_SIZE;

        if Self::free_space(data) < required {
            return Err(NodeFull);
        }

        let count = Self::key_count(data);
        let insert_idx = Self::find_insert_point(data, key, &rid);

        // Allocate space for entry (grow backward)
        let new_offset = Self::free_space_offset(data) - entry_size as u16;

        // Write entry: [key_data][page_id][slot_id]
        let offset = new_offset as usize;
        data[offset..offset + key_bytes.len()].copy_from_slice(&key_bytes);
        data[offset + key_bytes.len()..offset + key_bytes.len() + 4]
            .copy_from_slice(&rid.page_id.to_le_bytes());
        data[offset + key_bytes.len() + 4..offset + key_bytes.len() + 6]
            .copy_from_slice(&rid.slot_id.to_le_bytes());

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

    /// Check if node needs to split before inserting key
    pub fn need_split(data: &[u8], key: &IndexKey) -> bool {
        !Self::has_space_for(data, key)
    }

    /// Split this node. Move upper half to dst. Returns the split key (first key of dst).
    pub fn split(src: &mut [u8], dst: &mut [u8]) -> IndexKey {
        let count = Self::key_count(src);
        let mid = count / 2;

        // Initialize destination
        LeafNode::init(dst);

        // Copy upper half entries to dst
        for i in mid..count {
            let key = Self::get_key(src, i);
            let rid = Self::get_rid(src, i);
            // This should always succeed since dst is empty
            Self::insert(dst, &key, rid).expect("dst should have space");
        }

        // Get split key before truncating src
        let split_key = Self::get_key(dst, 0);

        // Compact src: rewrite entries for lower half
        let mut write_offset = PAGE_SIZE;
        for i in 0..mid {
            let key = Self::get_key(src, i);
            let rid = Self::get_rid(src, i);
            let key_bytes = key.serialize();
            let entry_size = key_bytes.len() + RID_SIZE;

            write_offset -= entry_size;
            let offset = write_offset;
            src[offset..offset + key_bytes.len()].copy_from_slice(&key_bytes);
            src[offset + key_bytes.len()..offset + key_bytes.len() + 4]
                .copy_from_slice(&rid.page_id.to_le_bytes());
            src[offset + key_bytes.len() + 4..offset + key_bytes.len() + 6]
                .copy_from_slice(&rid.slot_id.to_le_bytes());

            Self::set_slot(src, i, write_offset as u16, entry_size as u16);
        }

        Self::set_key_count(src, mid);
        Self::set_free_space_offset(src, write_offset as u16);

        // Link leaves: src -> dst
        let src_next = Self::get_next_leaf(src);
        Self::set_next_leaf(src, None); // Will be set by caller with actual page_id
        Self::set_prev_leaf(dst, None); // Will be set by caller
        Self::set_next_leaf(dst, src_next);

        split_key
    }
}

#[derive(Debug)]
pub struct NodeFull;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuple::Value;

    #[test]
    fn test_leaf_init() {
        let mut data = vec![0u8; PAGE_SIZE];
        LeafNode::init(&mut data);

        assert!(LeafNode::is_leaf(&data));
        assert_eq!(LeafNode::key_count(&data), 0);
        assert_eq!(LeafNode::get_prev_leaf(&data), None);
        assert_eq!(LeafNode::get_next_leaf(&data), None);
    }

    #[test]
    fn test_leaf_insert_and_search() {
        let mut data = vec![0u8; PAGE_SIZE];
        LeafNode::init(&mut data);

        let key1 = IndexKey::new(vec![Value::Int(10)]);
        let rid1 = Rid {
            page_id: 1,
            slot_id: 0,
        };
        LeafNode::insert(&mut data, &key1, rid1).unwrap();

        let key2 = IndexKey::new(vec![Value::Int(20)]);
        let rid2 = Rid {
            page_id: 2,
            slot_id: 1,
        };
        LeafNode::insert(&mut data, &key2, rid2).unwrap();

        let key3 = IndexKey::new(vec![Value::Int(5)]);
        let rid3 = Rid {
            page_id: 0,
            slot_id: 5,
        };
        LeafNode::insert(&mut data, &key3, rid3).unwrap();

        assert_eq!(LeafNode::key_count(&data), 3);

        // Search
        assert_eq!(LeafNode::search(&data, &key1), Some(rid1));
        assert_eq!(LeafNode::search(&data, &key2), Some(rid2));
        assert_eq!(LeafNode::search(&data, &key3), Some(rid3));
        assert_eq!(
            LeafNode::search(&data, &IndexKey::new(vec![Value::Int(15)])),
            None
        );

        // Keys should be sorted
        assert_eq!(LeafNode::get_key(&data, 0), key3); // 5
        assert_eq!(LeafNode::get_key(&data, 1), key1); // 10
        assert_eq!(LeafNode::get_key(&data, 2), key2); // 20
    }

    #[test]
    fn test_leaf_split() {
        let mut src = vec![0u8; PAGE_SIZE];
        let mut dst = vec![0u8; PAGE_SIZE];
        LeafNode::init(&mut src);

        // Insert many keys
        for i in 0..50 {
            let key = IndexKey::new(vec![Value::Int(i * 10)]);
            let rid = Rid {
                page_id: i as u32,
                slot_id: 0,
            };
            LeafNode::insert(&mut src, &key, rid).unwrap();
        }

        let split_key = LeafNode::split(&mut src, &mut dst);

        // Both nodes should have entries
        let src_count = LeafNode::key_count(&src);
        let dst_count = LeafNode::key_count(&dst);
        assert!(src_count > 0);
        assert!(dst_count > 0);
        assert_eq!(src_count + dst_count, 50);

        // Split key should be first key of dst
        assert_eq!(split_key, LeafNode::get_key(&dst, 0));

        // All keys in src should be < split_key
        for i in 0..src_count {
            assert!(LeafNode::get_key(&src, i) < split_key);
        }

        // All keys in dst should be >= split_key
        for i in 0..dst_count {
            assert!(LeafNode::get_key(&dst, i) >= split_key);
        }
    }
}
