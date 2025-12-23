use std::cmp::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::Result;

use crate::buffer_pool::BufferPoolManager;
use crate::executor::Rid;
use crate::page::btree_internal::InternalNode;
use crate::page::btree_leaf::LeafNode;
use crate::tuple::{DataType, Value};

// ============================================================================
// IndexKey - Composite key for B-Tree index
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKey(pub Vec<Value>);

impl IndexKey {
    pub fn new(values: Vec<Value>) -> Self {
        IndexKey(values)
    }

    #[allow(dead_code)]
    pub fn single(value: Value) -> Self {
        IndexKey(vec![value])
    }

    /// Serialize the key to bytes
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // Write number of values
        buf.push(self.0.len() as u8);
        for value in &self.0 {
            match value {
                Value::Null => {
                    buf.push(0); // type tag
                }
                Value::Int(n) => {
                    buf.push(1); // type tag
                    buf.extend_from_slice(&n.to_le_bytes());
                }
                Value::Varchar(s) => {
                    buf.push(2); // type tag
                    let bytes = s.as_bytes();
                    buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                    buf.extend_from_slice(bytes);
                }
                Value::Bool(b) => {
                    buf.push(3); // type tag
                    buf.push(if *b { 1 } else { 0 });
                }
            }
        }
        buf
    }

    /// Deserialize key from bytes
    pub fn deserialize(data: &[u8]) -> Self {
        let mut pos = 0;
        let count = data[pos] as usize;
        pos += 1;

        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let type_tag = data[pos];
            pos += 1;
            let value = match type_tag {
                0 => Value::Null,
                1 => {
                    let n = i32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
                    pos += 4;
                    Value::Int(n)
                }
                2 => {
                    let len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                    pos += 2;
                    let s = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
                    pos += len;
                    Value::Varchar(s)
                }
                3 => {
                    let b = data[pos] != 0;
                    pos += 1;
                    Value::Bool(b)
                }
                _ => Value::Null,
            };
            values.push(value);
        }
        IndexKey(values)
    }

    /// Get serialized size
    pub fn serialized_size(&self) -> usize {
        let mut size = 1; // count byte
        for value in &self.0 {
            size += 1; // type tag
            match value {
                Value::Null => {}
                Value::Int(_) => size += 4,
                Value::Varchar(s) => size += 2 + s.len(),
                Value::Bool(_) => size += 1,
            }
        }
        size
    }
}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        for (a, b) in self.0.iter().zip(other.0.iter()) {
            match compare_values(a, b) {
                Ordering::Equal => continue,
                other => return other,
            }
        }
        self.0.len().cmp(&other.0.len())
    }
}

fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Varchar(a), Value::Varchar(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        // Type mismatch: order by type discriminant for deterministic ordering
        (Value::Int(_), _) => Ordering::Less,
        (_, Value::Int(_)) => Ordering::Greater,
        (Value::Varchar(_), _) => Ordering::Less,
        (_, Value::Varchar(_)) => Ordering::Greater,
    }
}

// ============================================================================
// BTree - Main B-Tree index structure
// ============================================================================

pub struct BTree {
    bpm: Arc<Mutex<BufferPoolManager>>,
    root_page_id: Option<u32>,
    #[allow(dead_code)]
    key_schema: Vec<DataType>,
}

impl BTree {
    pub fn new(bpm: Arc<Mutex<BufferPoolManager>>, key_schema: Vec<DataType>) -> Self {
        BTree {
            bpm,
            root_page_id: None,
            key_schema,
        }
    }

    /// Create an empty B-Tree with a single leaf node as root
    pub fn create_empty(&mut self) -> Result<()> {
        let mut bpm = self.bpm.lock().unwrap();
        let (page_id, page) = bpm.new_page()?;
        {
            let mut page_guard = page.write().unwrap();
            LeafNode::init(&mut page_guard.data);
        }
        bpm.unpin_page(page_id, true)?;
        self.root_page_id = Some(page_id);
        Ok(())
    }

    /// Get root page ID
    pub fn root_page_id(&self) -> Option<u32> {
        self.root_page_id
    }

    /// Set root page ID (used when loading existing index)
    #[allow(dead_code)]
    pub fn set_root_page_id(&mut self, page_id: u32) {
        self.root_page_id = Some(page_id);
    }

    // ========================================================================
    // Search
    // ========================================================================

    /// Search for a key and return the associated RID if found
    pub fn search(&self, key: &IndexKey) -> Result<Option<Rid>> {
        let root_id = match self.root_page_id {
            Some(id) => id,
            None => return Ok(None),
        };

        let leaf_page_id = self.find_leaf(root_id, key)?;

        let mut bpm = self.bpm.lock().unwrap();
        let page = bpm.fetch_page(leaf_page_id)?;
        let page_guard = page.read().unwrap();
        let result = LeafNode::search(&page_guard.data, key);
        drop(page_guard);
        bpm.unpin_page(leaf_page_id, false)?;

        Ok(result)
    }

    /// Find the leaf page that should contain the given key
    fn find_leaf(&self, start_page_id: u32, key: &IndexKey) -> Result<u32> {
        let mut current_page_id = start_page_id;

        loop {
            let mut bpm = self.bpm.lock().unwrap();
            let page = bpm.fetch_page(current_page_id)?;
            let page_guard = page.read().unwrap();

            if LeafNode::is_leaf(&page_guard.data) {
                drop(page_guard);
                bpm.unpin_page(current_page_id, false)?;
                return Ok(current_page_id);
            }

            // Internal node - find child to follow
            let child_page_id = InternalNode::find_child(&page_guard.data, key);
            drop(page_guard);
            bpm.unpin_page(current_page_id, false)?;

            current_page_id = child_page_id;
        }
    }

    // ========================================================================
    // Insert
    // ========================================================================

    /// Insert a key-RID pair into the B-Tree
    pub fn insert(&mut self, key: &IndexKey, rid: Rid) -> Result<()> {
        // If tree is empty, create root
        if self.root_page_id.is_none() {
            self.create_empty()?;
        }

        let root_id = self.root_page_id.unwrap();
        let leaf_page_id = self.find_leaf(root_id, key)?;

        // Try to insert into leaf
        let split_result = self.insert_into_leaf(leaf_page_id, key, rid)?;

        // If leaf split, propagate up
        if let Some((split_key, new_page_id)) = split_result {
            self.insert_into_parent(leaf_page_id, split_key, new_page_id)?;
        }

        Ok(())
    }

    /// Insert into a leaf node. Returns Some((split_key, new_page_id)) if split occurred.
    fn insert_into_leaf(
        &mut self,
        page_id: u32,
        key: &IndexKey,
        rid: Rid,
    ) -> Result<Option<(IndexKey, u32)>> {
        let mut bpm = self.bpm.lock().unwrap();
        let page = bpm.fetch_page_mut(page_id)?;
        let mut page_guard = page.write().unwrap();

        // Check if we need to split
        if LeafNode::need_split(&page_guard.data, key) {
            // Create new page for split
            let (new_page_id, new_page) = bpm.new_page()?;
            let mut new_page_guard = new_page.write().unwrap();

            // Split the node
            let split_key = LeafNode::split(&mut page_guard.data, &mut new_page_guard.data);

            // Link the leaves
            LeafNode::set_next_leaf(&mut page_guard.data, Some(new_page_id));
            LeafNode::set_prev_leaf(&mut new_page_guard.data, Some(page_id));

            // Determine which node should receive the new key
            if *key < split_key {
                LeafNode::insert(&mut page_guard.data, key, rid)
                    .expect("should have space after split");
            } else {
                LeafNode::insert(&mut new_page_guard.data, key, rid)
                    .expect("should have space after split");
            }

            drop(new_page_guard);
            drop(page_guard);
            bpm.unpin_page(new_page_id, true)?;
            bpm.unpin_page(page_id, true)?;

            return Ok(Some((split_key, new_page_id)));
        }

        // No split needed - just insert
        LeafNode::insert(&mut page_guard.data, key, rid).expect("should have space");
        drop(page_guard);
        bpm.unpin_page(page_id, true)?;

        Ok(None)
    }

    /// Insert a split key into the parent node
    fn insert_into_parent(
        &mut self,
        left_page_id: u32,
        key: IndexKey,
        right_page_id: u32,
    ) -> Result<()> {
        // Find parent of left_page_id
        let parent_id = self.find_parent(left_page_id)?;

        match parent_id {
            None => {
                // left_page_id is root - create new root
                self.create_new_root(left_page_id, key, right_page_id)?;
            }
            Some(parent_page_id) => {
                // Insert into existing parent
                let split_result =
                    self.insert_into_internal(parent_page_id, &key, left_page_id, right_page_id)?;

                // If parent split, continue propagating
                if let Some((split_key, new_parent_id)) = split_result {
                    self.insert_into_parent(parent_page_id, split_key, new_parent_id)?;
                }
            }
        }

        Ok(())
    }

    /// Find the parent of a page. Returns None if the page is the root.
    fn find_parent(&self, target_page_id: u32) -> Result<Option<u32>> {
        let root_id = match self.root_page_id {
            Some(id) => id,
            None => return Ok(None),
        };

        if target_page_id == root_id {
            return Ok(None);
        }

        // Search from root to find parent
        self.find_parent_recursive(root_id, target_page_id)
    }

    fn find_parent_recursive(
        &self,
        current_page_id: u32,
        target_page_id: u32,
    ) -> Result<Option<u32>> {
        let mut bpm = self.bpm.lock().unwrap();
        let page = bpm.fetch_page(current_page_id)?;
        let page_guard = page.read().unwrap();

        if LeafNode::is_leaf(&page_guard.data) {
            drop(page_guard);
            bpm.unpin_page(current_page_id, false)?;
            return Ok(None);
        }

        // Collect all children and check if target is among them
        let key_count = InternalNode::key_count(&page_guard.data);
        let mut children = Vec::with_capacity(key_count as usize + 1);
        for i in 0..key_count {
            let child = InternalNode::get_child(&page_guard.data, i);
            if child == target_page_id {
                drop(page_guard);
                bpm.unpin_page(current_page_id, false)?;
                return Ok(Some(current_page_id));
            }
            children.push(child);
        }
        let rightmost = InternalNode::get_rightmost_child(&page_guard.data);
        if rightmost == target_page_id {
            drop(page_guard);
            bpm.unpin_page(current_page_id, false)?;
            return Ok(Some(current_page_id));
        }
        children.push(rightmost);

        drop(page_guard);
        bpm.unpin_page(current_page_id, false)?;

        // Recursively search in children
        for child_id in children {
            if let Some(parent) = self.find_parent_recursive(child_id, target_page_id)? {
                return Ok(Some(parent));
            }
        }

        Ok(None)
    }

    /// Create a new root node after the old root splits
    fn create_new_root(
        &mut self,
        left_page_id: u32,
        key: IndexKey,
        right_page_id: u32,
    ) -> Result<()> {
        let mut bpm = self.bpm.lock().unwrap();
        let (new_root_id, new_root_page) = bpm.new_page()?;
        let mut page_guard = new_root_page.write().unwrap();

        // Initialize as internal node with right child as rightmost
        InternalNode::init(&mut page_guard.data, right_page_id);

        // Insert the key with left child
        InternalNode::insert(&mut page_guard.data, &key, left_page_id)
            .expect("new root should have space");

        drop(page_guard);
        bpm.unpin_page(new_root_id, true)?;

        self.root_page_id = Some(new_root_id);
        Ok(())
    }

    /// Insert into an internal node
    fn insert_into_internal(
        &mut self,
        page_id: u32,
        key: &IndexKey,
        left_child: u32,
        right_child: u32,
    ) -> Result<Option<(IndexKey, u32)>> {
        let mut bpm = self.bpm.lock().unwrap();
        let page = bpm.fetch_page_mut(page_id)?;
        let mut page_guard = page.write().unwrap();

        // Check if we need to split
        if InternalNode::need_split(&page_guard.data, key) {
            // Create new page for split
            let (new_page_id, new_page) = bpm.new_page()?;
            let mut new_page_guard = new_page.write().unwrap();

            // Split the node
            let (split_key, _) = InternalNode::split(&mut page_guard.data, &mut new_page_guard.data);

            // Determine which node should receive the new key
            if *key < split_key {
                Self::insert_key_with_children(
                    &mut page_guard.data,
                    key,
                    left_child,
                    right_child,
                );
            } else {
                // key >= split_key: insert into dst
                Self::insert_key_with_children(
                    &mut new_page_guard.data,
                    key,
                    left_child,
                    right_child,
                );
            }

            drop(new_page_guard);
            drop(page_guard);
            bpm.unpin_page(new_page_id, true)?;
            bpm.unpin_page(page_id, true)?;

            return Ok(Some((split_key, new_page_id)));
        }

        // No split needed - insert directly
        Self::insert_key_with_children(&mut page_guard.data, key, left_child, right_child);

        drop(page_guard);
        bpm.unpin_page(page_id, true)?;

        Ok(None)
    }

    /// Insert a key into an internal node with its left and right children.
    /// After insertion: entries[...] -> (key, left_child) -> next entry has right_child
    fn insert_key_with_children(data: &mut [u8], key: &IndexKey, left_child: u32, right_child: u32) {
        // Insert (key, left_child)
        InternalNode::insert(data, key, left_child).expect("should have space");

        // Find where the key was inserted
        let count = InternalNode::key_count(data);
        for i in 0..count {
            if InternalNode::get_key(data, i) == *key {
                if i + 1 < count {
                    // Update next entry's child to right_child
                    InternalNode::set_child(data, i + 1, right_child);
                } else {
                    // This key is last, update rightmost_child
                    InternalNode::set_rightmost_child(data, right_child);
                }
                return;
            }
        }
    }

    // ========================================================================
    // Range Scan
    // ========================================================================

    /// Scan all entries in the range [start, end)
    pub fn range_scan(
        &self,
        start: Option<&IndexKey>,
        end: Option<&IndexKey>,
    ) -> Result<Vec<(IndexKey, Rid)>> {
        let root_id = match self.root_page_id {
            Some(id) => id,
            None => return Ok(vec![]),
        };

        // Find the starting leaf
        let mut start_leaf = match start {
            Some(key) => self.find_leaf(root_id, key)?,
            None => self.find_leftmost_leaf(root_id)?,
        };

        // For duplicate keys: scan backward to find the first leaf containing keys >= start
        // This handles the case where duplicate keys span multiple leaves
        if let Some(start_key) = start {
            start_leaf = self.find_first_leaf_with_key(start_leaf, start_key)?;
        }

        let mut results = Vec::new();
        let mut current_leaf = Some(start_leaf);
        let mut is_first_leaf = true;

        while let Some(leaf_id) = current_leaf {
            let mut bpm = self.bpm.lock().unwrap();
            let page = bpm.fetch_page(leaf_id)?;
            let page_guard = page.read().unwrap();

            let count = LeafNode::key_count(&page_guard.data);
            let next_leaf = LeafNode::get_next_leaf(&page_guard.data);

            // For the first leaf, use find_key_position to skip entries before start
            let start_idx = if is_first_leaf {
                is_first_leaf = false;
                match start {
                    Some(s) => LeafNode::find_key_position(&page_guard.data, s),
                    None => 0,
                }
            } else {
                0
            };

            // Iterate from start_idx to end
            for i in start_idx..count {
                let key = LeafNode::get_key(&page_guard.data, i);
                // Check end bound
                if let Some(e) = end {
                    if key >= *e {
                        drop(page_guard);
                        bpm.unpin_page(leaf_id, false)?;
                        return Ok(results);
                    }
                }
                let rid = LeafNode::get_rid(&page_guard.data, i);
                results.push((key, rid));
            }

            drop(page_guard);
            bpm.unpin_page(leaf_id, false)?;

            current_leaf = next_leaf;
        }

        Ok(results)
    }

    /// Find the first (leftmost) leaf that contains keys >= target_key
    /// by scanning backward from the given leaf
    fn find_first_leaf_with_key(&self, leaf_id: u32, target_key: &IndexKey) -> Result<u32> {
        let mut current = leaf_id;

        loop {
            let prev_leaf = {
                let mut bpm = self.bpm.lock().unwrap();
                let page = bpm.fetch_page(current)?;
                let page_guard = page.read().unwrap();
                let prev = LeafNode::get_prev_leaf(&page_guard.data);
                drop(page_guard);
                bpm.unpin_page(current, false)?;
                prev
            };

            match prev_leaf {
                None => return Ok(current),
                Some(prev_id) => {
                    // Check if prev leaf has any keys >= target_key
                    let has_target_keys = {
                        let mut bpm = self.bpm.lock().unwrap();
                        let prev_page = bpm.fetch_page(prev_id)?;
                        let prev_guard = prev_page.read().unwrap();
                        let result = Self::leaf_has_keys_gte(&prev_guard.data, target_key);
                        drop(prev_guard);
                        bpm.unpin_page(prev_id, false)?;
                        result
                    };

                    if has_target_keys {
                        current = prev_id;
                    } else {
                        return Ok(current);
                    }
                }
            }
        }
    }

    /// Check if a leaf has any keys >= target
    fn leaf_has_keys_gte(data: &[u8], target: &IndexKey) -> bool {
        let count = LeafNode::key_count(data);
        if count == 0 {
            return false;
        }
        // Check last key (since keys are sorted)
        let last_key = LeafNode::get_key(data, count - 1);
        last_key >= *target
    }

    /// Find the leftmost leaf node
    fn find_leftmost_leaf(&self, start_page_id: u32) -> Result<u32> {
        let mut current_page_id = start_page_id;

        loop {
            let mut bpm = self.bpm.lock().unwrap();
            let page = bpm.fetch_page(current_page_id)?;
            let page_guard = page.read().unwrap();

            if LeafNode::is_leaf(&page_guard.data) {
                drop(page_guard);
                bpm.unpin_page(current_page_id, false)?;
                return Ok(current_page_id);
            }

            // Internal node - go to leftmost child
            let key_count = InternalNode::key_count(&page_guard.data);
            let next_page_id = if key_count > 0 {
                InternalNode::get_child(&page_guard.data, 0)
            } else {
                InternalNode::get_rightmost_child(&page_guard.data)
            };
            drop(page_guard);
            bpm.unpin_page(current_page_id, false)?;

            current_page_id = next_page_id;
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::DiskManager;
    use crate::wal::WalManager;
    use std::fs;
    use tempfile::tempdir;

    fn setup_btree() -> (BTree, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_dir = dir.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        let disk_manager = DiskManager::open(db_path.to_str().unwrap()).unwrap();
        let wal_manager = Arc::new(WalManager::new(wal_dir.to_str().unwrap()).unwrap());
        let bpm = Arc::new(Mutex::new(BufferPoolManager::new(disk_manager, wal_manager)));

        let btree = BTree::new(bpm, vec![DataType::Int]);
        (btree, dir)
    }

    #[test]
    fn test_btree_create_empty() {
        let (mut btree, _dir) = setup_btree();
        btree.create_empty().unwrap();
        assert!(btree.root_page_id().is_some());
    }

    #[test]
    fn test_btree_insert_and_search() {
        let (mut btree, _dir) = setup_btree();

        let key1 = IndexKey::single(Value::Int(10));
        let rid1 = Rid {
            page_id: 1,
            slot_id: 0,
        };
        btree.insert(&key1, rid1).unwrap();

        let key2 = IndexKey::single(Value::Int(20));
        let rid2 = Rid {
            page_id: 2,
            slot_id: 1,
        };
        btree.insert(&key2, rid2).unwrap();

        let key3 = IndexKey::single(Value::Int(5));
        let rid3 = Rid {
            page_id: 0,
            slot_id: 5,
        };
        btree.insert(&key3, rid3).unwrap();

        // Search
        assert_eq!(btree.search(&key1).unwrap(), Some(rid1));
        assert_eq!(btree.search(&key2).unwrap(), Some(rid2));
        assert_eq!(btree.search(&key3).unwrap(), Some(rid3));
        assert_eq!(
            btree
                .search(&IndexKey::single(Value::Int(15)))
                .unwrap(),
            None
        );
    }

    #[test]
    fn test_btree_insert_many_causes_split() {
        let (mut btree, _dir) = setup_btree();

        // Insert many keys to cause splits
        for i in 0..100 {
            let key = IndexKey::single(Value::Int(i));
            let rid = Rid {
                page_id: i as u32,
                slot_id: 0,
            };
            btree.insert(&key, rid).unwrap();
        }

        // Verify all keys can be found
        for i in 0..100 {
            let key = IndexKey::single(Value::Int(i));
            let expected_rid = Rid {
                page_id: i as u32,
                slot_id: 0,
            };
            assert_eq!(
                btree.search(&key).unwrap(),
                Some(expected_rid),
                "Failed to find key {}",
                i
            );
        }
    }

    #[test]
    fn test_btree_range_scan() {
        let (mut btree, _dir) = setup_btree();

        // Insert keys 0, 10, 20, ..., 90
        for i in 0..10 {
            let key = IndexKey::single(Value::Int(i * 10));
            let rid = Rid {
                page_id: i as u32,
                slot_id: 0,
            };
            btree.insert(&key, rid).unwrap();
        }

        // Range scan [25, 65)
        let start = IndexKey::single(Value::Int(25));
        let end = IndexKey::single(Value::Int(65));
        let results = btree.range_scan(Some(&start), Some(&end)).unwrap();

        // Should get 30, 40, 50, 60
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].0, IndexKey::single(Value::Int(30)));
        assert_eq!(results[1].0, IndexKey::single(Value::Int(40)));
        assert_eq!(results[2].0, IndexKey::single(Value::Int(50)));
        assert_eq!(results[3].0, IndexKey::single(Value::Int(60)));
    }

    #[test]
    fn test_indexkey_serialize_deserialize() {
        let key = IndexKey::new(vec![
            Value::Int(42),
            Value::Varchar("hello".to_string()),
            Value::Bool(true),
        ]);

        let serialized = key.serialize();
        let deserialized = IndexKey::deserialize(&serialized);

        assert_eq!(key, deserialized);
    }

    #[test]
    fn test_indexkey_ordering() {
        let k1 = IndexKey::single(Value::Int(10));
        let k2 = IndexKey::single(Value::Int(20));
        let k3 = IndexKey::single(Value::Int(10));

        assert!(k1 < k2);
        assert!(k2 > k1);
        assert!(k1 == k3);

        // Composite keys
        let c1 = IndexKey::new(vec![Value::Int(10), Value::Int(5)]);
        let c2 = IndexKey::new(vec![Value::Int(10), Value::Int(10)]);
        assert!(c1 < c2);
    }

    #[test]
    fn test_btree_insert_reverse_order() {
        let (mut btree, _dir) = setup_btree();

        // Insert keys in reverse order to stress test splits
        for i in (0..100).rev() {
            let key = IndexKey::single(Value::Int(i));
            let rid = Rid {
                page_id: i as u32,
                slot_id: 0,
            };
            btree.insert(&key, rid).unwrap();
        }

        // Verify all keys can be found
        for i in 0..100 {
            let key = IndexKey::single(Value::Int(i));
            let expected_rid = Rid {
                page_id: i as u32,
                slot_id: 0,
            };
            assert_eq!(
                btree.search(&key).unwrap(),
                Some(expected_rid),
                "Failed to find key {}",
                i
            );
        }

        // Verify range scan returns sorted results
        let results = btree.range_scan(None, None).unwrap();
        assert_eq!(results.len(), 100);
        for i in 0..100 {
            assert_eq!(results[i].0, IndexKey::single(Value::Int(i as i32)));
        }
    }

    #[test]
    fn test_btree_insert_many_deep_splits() {
        let (mut btree, _dir) = setup_btree();

        // Insert enough keys to cause multiple levels of splits
        for i in 0..500 {
            let key = IndexKey::single(Value::Int(i));
            let rid = Rid {
                page_id: i as u32,
                slot_id: (i % 10) as u16,
            };
            btree.insert(&key, rid).unwrap();
        }

        // Verify all keys can be found
        for i in 0..500 {
            let key = IndexKey::single(Value::Int(i));
            let expected_rid = Rid {
                page_id: i as u32,
                slot_id: (i % 10) as u16,
            };
            assert_eq!(
                btree.search(&key).unwrap(),
                Some(expected_rid),
                "Failed to find key {}",
                i
            );
        }

        // Verify range scan
        let results = btree.range_scan(Some(&IndexKey::single(Value::Int(100))), Some(&IndexKey::single(Value::Int(200)))).unwrap();
        assert_eq!(results.len(), 100);
        for (i, (key, _)) in results.iter().enumerate() {
            assert_eq!(*key, IndexKey::single(Value::Int(100 + i as i32)));
        }
    }
}

#[cfg(test)]
mod duplicate_tests {
    use super::*;
    use crate::disk::DiskManager;
    use crate::wal::WalManager;
    use std::fs;
    use tempfile::tempdir;

    fn setup_btree() -> (BTree, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_dir = dir.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        let disk_manager = DiskManager::open(db_path.to_str().unwrap()).unwrap();
        let wal_manager = Arc::new(WalManager::new(wal_dir.to_str().unwrap()).unwrap());
        let bpm = Arc::new(Mutex::new(BufferPoolManager::new(disk_manager, wal_manager)));

        let btree = BTree::new(bpm, vec![DataType::Int]);
        (btree, dir)
    }

    #[test]
    fn test_duplicate_keys() {
        let (mut btree, _dir) = setup_btree();

        // Insert same key 10 times with different RIDs
        for i in 0..10 {
            let key = IndexKey::single(Value::Int(100));
            let rid = Rid {
                page_id: i as u32,
                slot_id: i as u16,
            };
            btree.insert(&key, rid).unwrap();
        }

        // All should be findable via range scan
        let key = IndexKey::single(Value::Int(100));
        let results = btree.range_scan(Some(&key), Some(&IndexKey::single(Value::Int(101)))).unwrap();
        assert_eq!(results.len(), 10, "Should find all 10 duplicate keys, found {}", results.len());
    }

    #[test]
    fn test_duplicate_keys_with_split() {
        let (mut btree, _dir) = setup_btree();

        // Insert enough duplicate keys to cause a split
        for i in 0..200 {
            let key = IndexKey::single(Value::Int(50));
            let rid = Rid {
                page_id: i as u32,
                slot_id: 0,
            };
            btree.insert(&key, rid).unwrap();
        }

        // All should be findable via range scan
        let key = IndexKey::single(Value::Int(50));
        let results = btree.range_scan(Some(&key), Some(&IndexKey::single(Value::Int(51)))).unwrap();
        assert_eq!(results.len(), 200, "Should find all 200 duplicate keys, found {}", results.len());
    }

    #[test]
    fn test_duplicate_keys_large() {
        let (mut btree, _dir) = setup_btree();

        // Insert 1000 duplicate keys to force multiple splits
        for i in 0..1000 {
            let key = IndexKey::single(Value::Int(50));
            let rid = Rid {
                page_id: i as u32,
                slot_id: 0,
            };
            btree.insert(&key, rid).unwrap();
        }

        let key = IndexKey::single(Value::Int(50));
        let results = btree.range_scan(Some(&key), Some(&IndexKey::single(Value::Int(51)))).unwrap();
        assert_eq!(results.len(), 1000, "Should find all 1000 duplicate keys, found {}", results.len());
    }

    #[test]
    fn test_mixed_duplicates_and_unique() {
        let (mut btree, _dir) = setup_btree();

        // Insert: 10, 20, 20, 20, 30, 40, 40, 50
        let entries = vec![
            (10, 0), (20, 1), (20, 2), (20, 3), (30, 4), (40, 5), (40, 6), (50, 7),
        ];
        for (key_val, rid_page) in &entries {
            let key = IndexKey::single(Value::Int(*key_val));
            let rid = Rid {
                page_id: *rid_page,
                slot_id: 0,
            };
            btree.insert(&key, rid).unwrap();
        }

        // Range scan [20, 45) should get 20, 20, 20, 30, 40, 40
        let results = btree.range_scan(
            Some(&IndexKey::single(Value::Int(20))),
            Some(&IndexKey::single(Value::Int(45))),
        ).unwrap();
        assert_eq!(results.len(), 6);

        // Verify keys are in order
        assert_eq!(results[0].0, IndexKey::single(Value::Int(20)));
        assert_eq!(results[1].0, IndexKey::single(Value::Int(20)));
        assert_eq!(results[2].0, IndexKey::single(Value::Int(20)));
        assert_eq!(results[3].0, IndexKey::single(Value::Int(30)));
        assert_eq!(results[4].0, IndexKey::single(Value::Int(40)));
        assert_eq!(results[5].0, IndexKey::single(Value::Int(40)));
    }
}

#[cfg(test)]
mod edge_case_tests {
    use super::*;
    use crate::disk::DiskManager;
    use crate::wal::WalManager;
    use std::fs;
    use tempfile::tempdir;

    fn setup_btree() -> (BTree, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_dir = dir.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        let disk_manager = DiskManager::open(db_path.to_str().unwrap()).unwrap();
        let wal_manager = Arc::new(WalManager::new(wal_dir.to_str().unwrap()).unwrap());
        let bpm = Arc::new(Mutex::new(BufferPoolManager::new(disk_manager, wal_manager)));

        let btree = BTree::new(bpm, vec![DataType::Int]);
        (btree, dir)
    }

    // === Empty Tree Operations ===

    #[test]
    fn test_search_empty_tree() {
        let (btree, _dir) = setup_btree();
        let key = IndexKey::single(Value::Int(10));
        assert_eq!(btree.search(&key).unwrap(), None);
    }

    #[test]
    fn test_range_scan_empty_tree() {
        let (btree, _dir) = setup_btree();
        let results = btree.range_scan(None, None).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_range_scan_empty_tree_with_bounds() {
        let (btree, _dir) = setup_btree();
        let start = IndexKey::single(Value::Int(10));
        let end = IndexKey::single(Value::Int(20));
        let results = btree.range_scan(Some(&start), Some(&end)).unwrap();
        assert!(results.is_empty());
    }

    // === Boundary Conditions ===

    #[test]
    fn test_range_scan_no_matches() {
        let (mut btree, _dir) = setup_btree();

        // Insert keys 10, 20, 30
        for i in 1..=3 {
            let key = IndexKey::single(Value::Int(i * 10));
            let rid = Rid { page_id: i as u32, slot_id: 0 };
            btree.insert(&key, rid).unwrap();
        }

        // Range [100, 200) - no matches
        let results = btree.range_scan(
            Some(&IndexKey::single(Value::Int(100))),
            Some(&IndexKey::single(Value::Int(200))),
        ).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_range_scan_start_equals_end() {
        let (mut btree, _dir) = setup_btree();

        for i in 0..10 {
            let key = IndexKey::single(Value::Int(i * 10));
            let rid = Rid { page_id: i as u32, slot_id: 0 };
            btree.insert(&key, rid).unwrap();
        }

        // Range [50, 50) - empty range
        let results = btree.range_scan(
            Some(&IndexKey::single(Value::Int(50))),
            Some(&IndexKey::single(Value::Int(50))),
        ).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_range_scan_full_table() {
        let (mut btree, _dir) = setup_btree();

        for i in 0..50 {
            let key = IndexKey::single(Value::Int(i));
            let rid = Rid { page_id: i as u32, slot_id: 0 };
            btree.insert(&key, rid).unwrap();
        }

        // Full scan with None bounds
        let results = btree.range_scan(None, None).unwrap();
        assert_eq!(results.len(), 50);
        for i in 0..50 {
            assert_eq!(results[i].0, IndexKey::single(Value::Int(i as i32)));
        }
    }

    #[test]
    fn test_range_scan_start_before_all() {
        let (mut btree, _dir) = setup_btree();

        for i in 10..20 {
            let key = IndexKey::single(Value::Int(i));
            let rid = Rid { page_id: i as u32, slot_id: 0 };
            btree.insert(&key, rid).unwrap();
        }

        // Start from 0, which is before all entries
        let results = btree.range_scan(
            Some(&IndexKey::single(Value::Int(0))),
            Some(&IndexKey::single(Value::Int(15))),
        ).unwrap();
        assert_eq!(results.len(), 5); // 10, 11, 12, 13, 14
    }

    #[test]
    fn test_range_scan_end_after_all() {
        let (mut btree, _dir) = setup_btree();

        for i in 10..20 {
            let key = IndexKey::single(Value::Int(i));
            let rid = Rid { page_id: i as u32, slot_id: 0 };
            btree.insert(&key, rid).unwrap();
        }

        // End at 100, which is after all entries
        let results = btree.range_scan(
            Some(&IndexKey::single(Value::Int(15))),
            Some(&IndexKey::single(Value::Int(100))),
        ).unwrap();
        assert_eq!(results.len(), 5); // 15, 16, 17, 18, 19
    }

    // === NULL Values ===

    #[test]
    fn test_null_key_insert_and_search() {
        let (mut btree, _dir) = setup_btree();

        let null_key = IndexKey::single(Value::Null);
        let rid = Rid { page_id: 0, slot_id: 0 };
        btree.insert(&null_key, rid).unwrap();

        assert_eq!(btree.search(&null_key).unwrap(), Some(rid));
    }

    #[test]
    fn test_null_ordering() {
        let (mut btree, _dir) = setup_btree();

        // Insert NULL, then some integers
        btree.insert(&IndexKey::single(Value::Null), Rid { page_id: 0, slot_id: 0 }).unwrap();
        btree.insert(&IndexKey::single(Value::Int(10)), Rid { page_id: 1, slot_id: 0 }).unwrap();
        btree.insert(&IndexKey::single(Value::Int(-5)), Rid { page_id: 2, slot_id: 0 }).unwrap();

        // Full scan - NULL should come first (NULL < any other value)
        let results = btree.range_scan(None, None).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, IndexKey::single(Value::Null));
        assert_eq!(results[1].0, IndexKey::single(Value::Int(-5)));
        assert_eq!(results[2].0, IndexKey::single(Value::Int(10)));
    }

    // === Composite Keys ===

    #[test]
    fn test_composite_key_insert_and_search() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_dir = dir.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        let disk_manager = DiskManager::open(db_path.to_str().unwrap()).unwrap();
        let wal_manager = Arc::new(WalManager::new(wal_dir.to_str().unwrap()).unwrap());
        let bpm = Arc::new(Mutex::new(BufferPoolManager::new(disk_manager, wal_manager)));

        let mut btree = BTree::new(bpm, vec![DataType::Int, DataType::Varchar]);

        let key1 = IndexKey::new(vec![Value::Int(10), Value::Varchar("alice".to_string())]);
        let key2 = IndexKey::new(vec![Value::Int(10), Value::Varchar("bob".to_string())]);
        let key3 = IndexKey::new(vec![Value::Int(20), Value::Varchar("alice".to_string())]);

        btree.insert(&key1, Rid { page_id: 1, slot_id: 0 }).unwrap();
        btree.insert(&key2, Rid { page_id: 2, slot_id: 0 }).unwrap();
        btree.insert(&key3, Rid { page_id: 3, slot_id: 0 }).unwrap();

        assert_eq!(btree.search(&key1).unwrap(), Some(Rid { page_id: 1, slot_id: 0 }));
        assert_eq!(btree.search(&key2).unwrap(), Some(Rid { page_id: 2, slot_id: 0 }));
        assert_eq!(btree.search(&key3).unwrap(), Some(Rid { page_id: 3, slot_id: 0 }));

        // Verify ordering: (10, alice) < (10, bob) < (20, alice)
        let results = btree.range_scan(None, None).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, key1);
        assert_eq!(results[1].0, key2);
        assert_eq!(results[2].0, key3);
    }

    // === String Keys ===

    #[test]
    fn test_varchar_key_ordering() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let wal_dir = dir.path().join("wal");
        fs::create_dir_all(&wal_dir).unwrap();

        let disk_manager = DiskManager::open(db_path.to_str().unwrap()).unwrap();
        let wal_manager = Arc::new(WalManager::new(wal_dir.to_str().unwrap()).unwrap());
        let bpm = Arc::new(Mutex::new(BufferPoolManager::new(disk_manager, wal_manager)));

        let mut btree = BTree::new(bpm, vec![DataType::Varchar]);

        let names = vec!["charlie", "alice", "bob", "david"];
        for (i, name) in names.iter().enumerate() {
            let key = IndexKey::single(Value::Varchar(name.to_string()));
            btree.insert(&key, Rid { page_id: i as u32, slot_id: 0 }).unwrap();
        }

        // Full scan - should be in alphabetical order
        let results = btree.range_scan(None, None).unwrap();
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].0, IndexKey::single(Value::Varchar("alice".to_string())));
        assert_eq!(results[1].0, IndexKey::single(Value::Varchar("bob".to_string())));
        assert_eq!(results[2].0, IndexKey::single(Value::Varchar("charlie".to_string())));
        assert_eq!(results[3].0, IndexKey::single(Value::Varchar("david".to_string())));
    }

    // === Single Entry ===

    #[test]
    fn test_single_entry_operations() {
        let (mut btree, _dir) = setup_btree();

        let key = IndexKey::single(Value::Int(42));
        let rid = Rid { page_id: 1, slot_id: 0 };
        btree.insert(&key, rid).unwrap();

        // Search
        assert_eq!(btree.search(&key).unwrap(), Some(rid));
        assert_eq!(btree.search(&IndexKey::single(Value::Int(0))).unwrap(), None);

        // Range scan exact match
        let results = btree.range_scan(
            Some(&IndexKey::single(Value::Int(42))),
            Some(&IndexKey::single(Value::Int(43))),
        ).unwrap();
        assert_eq!(results.len(), 1);

        // Range scan miss
        let results = btree.range_scan(
            Some(&IndexKey::single(Value::Int(0))),
            Some(&IndexKey::single(Value::Int(10))),
        ).unwrap();
        assert!(results.is_empty());

        // Full scan
        let results = btree.range_scan(None, None).unwrap();
        assert_eq!(results.len(), 1);
    }

    // === Duplicate Keys with (key, rid) Ordering Verification ===

    #[test]
    fn test_duplicate_keys_rid_ordering() {
        let (mut btree, _dir) = setup_btree();

        // Insert same key with different RIDs in random order
        let rids = vec![
            Rid { page_id: 5, slot_id: 0 },
            Rid { page_id: 2, slot_id: 0 },
            Rid { page_id: 8, slot_id: 0 },
            Rid { page_id: 1, slot_id: 0 },
            Rid { page_id: 3, slot_id: 0 },
        ];

        for rid in &rids {
            let key = IndexKey::single(Value::Int(100));
            btree.insert(&key, *rid).unwrap();
        }

        // Range scan should return entries ordered by (key, rid)
        let results = btree.range_scan(
            Some(&IndexKey::single(Value::Int(100))),
            Some(&IndexKey::single(Value::Int(101))),
        ).unwrap();

        assert_eq!(results.len(), 5);
        // Verify RIDs are in order by page_id: 1, 2, 3, 5, 8
        assert_eq!(results[0].1.page_id, 1);
        assert_eq!(results[1].1.page_id, 2);
        assert_eq!(results[2].1.page_id, 3);
        assert_eq!(results[3].1.page_id, 5);
        assert_eq!(results[4].1.page_id, 8);
    }
}
