pub mod btree_internal;
pub mod btree_leaf;
pub mod btree_meta;
pub mod heap;

pub const PAGE_SIZE: usize = 4096;

pub use heap::HeapPage;
pub use heap::NO_NEXT_PAGE;
