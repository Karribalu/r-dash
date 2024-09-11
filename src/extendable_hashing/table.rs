use std::cmp::min;
use crate::extendable_hashing::bucket::Bucket;
use crate::extendable_hashing::directory::Directory;
use crate::extendable_hashing::{BUCKET_MASK, K_FINGER_BITS, K_NUM_BUCKET, K_STASH_BUCKET};
use crate::utils::pair::Key;
use std::fmt::Debug;
use std::ops::BitXor;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[derive(Debug)]
enum TableState {
    Merging,
    Splitting,
    NewTable,
    Normal,
}
#[derive(Debug, Error)]
pub enum TableError {
    #[error("The table is full")]
    TableFull,
    #[error("Table internal error")]
    Internal,
    #[error("Item does not exist")]
    ItemDoesntExist,
    #[error(transparent)]
    UnableToAcquireLock(String)
}
// Segment
#[derive(Debug)]
pub struct Table<T: PartialEq + Debug + Clone> {
    // TODO: Check if we need the dummy array
    // dummy: [char; 48],
    bucket: Vec<Bucket<T>>,
    local_depth: usize,
    pattern: usize,
    number: i32,
    state: Arc<TableState>, /*-1 means this bucket is merging, -2 means this bucket is splitting (SPLITTING), 0 meaning normal bucket, -3 means new bucket (NEW)*/
    lock_bit: Arc<Mutex<u32>>, /* for the synchronization of the lazy recovery in one segment*/
}

impl<T: PartialEq + Debug + Clone> Table<T> {
    pub fn new() -> Self {
        let mut buckets = vec![];
        for _i in 0..(K_NUM_BUCKET + K_STASH_BUCKET) {
            buckets.push(Bucket::new());
        }
        Table {
            bucket: buckets,
            local_depth: 0,
            pattern: 0,
            number: 0,
            state: Arc::from(TableState::Normal),
            lock_bit: Arc::new(Mutex::new(0)),
        }
    }
    pub fn acquire_locks(&self) {
        for i in 0..K_NUM_BUCKET {
            self.bucket[i].get_lock();
        }
    }
    pub fn release_locks(&self) {
        for i in 0..K_NUM_BUCKET {
            self.bucket[i].reset_lock();
        }
    }
    pub fn insert(&mut self, key: Key<T>, value: T, key_hash: usize, meta_hash: u8, directory: &Directory<T>) -> Result<i32, TableError>{
        let bucket_index = bucket_index(key_hash, K_FINGER_BITS, BUCKET_MASK);
        let target = &self.bucket[bucket_index];
        let neighbor = &self.bucket[(bucket_index + 1) & BUCKET_MASK];
        target.get_lock();
        if !neighbor.try_get_lock() {
            target.reset_lock();
            return Err(TableError::UnableToAcquireLock("Unable to acquire neighbor lock".to_string()));
        }
        let dir = directory;
        Ok(10)
    }
}
pub fn bucket_index(hash: usize, finger_bits: usize, bucket_mask: usize) -> usize {
    // We do the finger_bits right shift because we use that last 8 bits for finger-print.
    (hash >> finger_bits) & bucket_mask
}

mod tests {
    use crate::extendable_hashing::table::Table;
    use crate::extendable_hashing::{K_NUM_BUCKET, K_STASH_BUCKET};

    #[test]
    pub fn test_new_table() {
        let table = Table::<i32>::new();
        assert_eq!(table.bucket.len(), K_NUM_BUCKET + K_STASH_BUCKET);
        assert_eq!(table.local_depth, 0);
        assert_eq!(table.pattern, 0);
        assert_eq!(table.number, 0);
    }
    #[test]
    pub fn test_acquire_locks() {
        let mut table = Table::<i32>::new();
        table.acquire_locks();
        assert_eq!(
            table.bucket[0..K_NUM_BUCKET]
                .iter()
                .map(|item| item.is_lock())
                .all(|x| x),
            true
        );
        table.release_locks();
        assert_eq!(
            table.bucket[0..K_NUM_BUCKET]
                .iter()
                .map(|item| item.is_lock())
                .all(|x| !x),
            true
        );
    }
}
