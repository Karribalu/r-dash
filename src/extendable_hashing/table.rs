use crate::extendable_hashing::bucket::Bucket;
use crate::extendable_hashing::{K_NUM_BUCKET, K_STASH_BUCKET};
use std::sync::{Arc, Mutex};

// Segment
pub struct Table<T> {
    dummy: [char; 48],
    bucket: [Bucket<T>; K_NUM_BUCKET + K_STASH_BUCKET],
    local_depth: usize,
    pattern: usize,
    number: i32,
    state: i32, /*-1 means this bucket is merging, -2 means this bucket is splitting (SPLITTING), 0 meaning normal bucket, -3 means new bucket (NEW)*/
    lock_bit: Arc<Mutex<u32>>, /* for the synchronization of the lazy recovery in one segment*/
}
