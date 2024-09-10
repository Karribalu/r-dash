use crate::extendable_hashing::bucket::Bucket;
use crate::extendable_hashing::{K_NUM_BUCKET, K_STASH_BUCKET};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
#[derive(Debug)]
enum TableState {
    Merging,
    Splitting,
    NewTable,
    Normal,
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
}

mod tests {
    use crate::extendable_hashing::table::Table;
    use crate::extendable_hashing::{K_NUM_BUCKET, K_STASH_BUCKET};
    use std::ptr;

    #[test]
    pub fn test_new_table() {
        let table = Table::<i32>::new();
        assert_eq!(table.bucket.len(), K_NUM_BUCKET + K_STASH_BUCKET);
        assert_eq!(table.local_depth, 0);
        assert_eq!(table.pattern, 0);
        assert_eq!(table.number, 0);
        println!("{:p}", &table.bucket[0]);
        println!("{:p}", &table.bucket[1]);
        assert_eq!(ptr::eq(&table.bucket[0], &table.bucket[1]), false);
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
