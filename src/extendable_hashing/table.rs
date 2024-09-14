use crate::extendable_hashing::bucket::{get_count, Bucket, BucketError};
use crate::extendable_hashing::directory::Directory;
use crate::extendable_hashing::{BUCKET_MASK, K_FINGER_BITS, K_NUM_BUCKET, K_STASH_BUCKET};
use crate::hash::ValueT;
use crate::utils::pair::{Key, Pair};
use std::cmp::min;
use std::fmt::Debug;
use std::ops::{BitXor, Shr};
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
    #[error("Unable to aquire lock")]
    UnableToAcquireLock(String),
    #[error("Duplicate key insertion")]
    KeyExists,
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
    pub fn insert(
        &mut self,
        key: Key<T>,
        value: ValueT,
        key_hash: usize,
        meta_hash: u8,
        directory: &Directory<T>,
    ) -> Result<i32, TableError> {
        let bucket_index = bucket_index(key_hash, K_FINGER_BITS, BUCKET_MASK);
        let target = &mut self.bucket[bucket_index];
        // (bucket_index + 1) & BUCKET_MASK used for wrapping up to 0 when the bucket_index is 63
        // (63 + 1) & 63 = 64 & 63 = 0
        let neighbor = &mut self.bucket[(bucket_index + 1) & BUCKET_MASK];
        target.get_lock();
        if !neighbor.try_get_lock() {
            target.release_lock();
            return Err(TableError::UnableToAcquireLock(
                "Unable to acquire neighbor lock".to_string(),
            ));
        }
        let dir = directory;
        // Trying to get the MSBs of the key to determine the segment index
        let segment_index = key_hash >> (8 * size_of::<usize>() - dir.global_depth);
        // if dir.x[segment_index] != self {
        //     target.release_lock();
        //     neighbor.release_lock();
        //     return Err(TableError::Internal);
        // }
        if !target.unique_check(meta_hash, &key, neighbor, &self.bucket[K_NUM_BUCKET..]) {
            neighbor.release_lock();
            target.release_lock();
            return Err(TableError::KeyExists);
        }
        if get_count(target.bitmap) == K_NUM_BUCKET as u32
            && get_count(neighbor.bitmap) == K_NUM_BUCKET as u32
        {
            // Both the buckets are full, We have to do the displacement
            let next_neighbor = &mut self.bucket[(bucket_index + 2) & BUCKET_MASK];
            if !next_neighbor.try_get_lock() {
                neighbor.release_lock();
                target.release_lock();
                return Err(TableError::UnableToAcquireLock(
                    "Unable to acquire the lock for next neighbor".to_string(),
                ));
            }

            let displacement_res = Self::next_displace(
                target,
                neighbor,
                next_neighbor,
                key.clone(),
                value.clone(),
                meta_hash,
            );
            if displacement_res {
                // inserted in the neighboring bucket by displacement
                return Ok(1);
            }
            next_neighbor.release_lock();

            // Now we check for previous neighbor
            let prev_index = if bucket_index == 0 {
                K_NUM_BUCKET - 1
            } else {
                bucket_index - 1
            };
            let prev_neighbor = &mut self.bucket[prev_index];
            if !prev_neighbor.try_get_lock() {
                neighbor.release_lock();
                target.release_lock();
                return Err(TableError::UnableToAcquireLock(
                    "Unable to acquire the lock for previous neighbor".to_string(),
                ));
            }

            let displacement_res =
                Self::prev_displace(target, neighbor, prev_neighbor, key, value, meta_hash);
            if displacement_res {
                // inserted in the prev neighboring bucket by displacement
                return Ok(2);
            }
            prev_neighbor.release_lock();

            // Now we try to insert in the stash buckets
        }
        Ok(10)
    }

    pub fn next_displace(
        target: &Bucket<T>,
        neighbor: &mut Bucket<T>,
        next_neighbor: &mut Bucket<T>,
        key: Key<T>,
        value: ValueT,
        meta_hash: u8,
    ) -> bool {
        let displace_index: i32 = neighbor.find_org_displacement();
        if get_count(next_neighbor.bitmap) != K_NUM_BUCKET as u32 && displace_index != -1 {
            let neighbor_pair: Pair<T> = neighbor.pairs[displace_index as usize]
                .clone()
                .unwrap()
                .clone();
            match next_neighbor.insert(
                neighbor_pair.key,
                neighbor_pair.value,
                neighbor.finger_array[displace_index as usize],
                true,
            ) {
                Ok(_) => {
                    next_neighbor.release_lock();
                    neighbor.unset_hash(displace_index as u32);
                    neighbor.insert_displace(key, value, meta_hash, displace_index, true);
                    neighbor.release_lock();
                    target.release_lock();
                    return true;
                }
                Err(_) => {
                    neighbor.release_lock();
                    next_neighbor.release_lock();
                    target.release_lock();
                    return false;
                }
            }
        }
        false
    }

    pub fn prev_displace(
        target: &Bucket<T>,
        neighbor: &mut Bucket<T>,
        prev_neighbor: &mut Bucket<T>,
        key: Key<T>,
        value: ValueT,
        meta_hash: u8,
    ) -> bool {
        let displace_index = neighbor.find_probe_displacement();
        if get_count(prev_neighbor.bitmap) != K_NUM_BUCKET as u32 && displace_index != -1 {
            let neighbor_pair: Pair<T> = neighbor.pairs[displace_index as usize]
                .clone()
                .unwrap()
                .clone();
            match prev_neighbor.insert(
                neighbor_pair.key,
                neighbor_pair.value,
                neighbor.finger_array[displace_index as usize],
                false,
            ) {
                Ok(_) => {
                    prev_neighbor.release_lock();
                    neighbor.unset_hash(displace_index as u32);
                    neighbor.insert_displace(key, value, meta_hash, displace_index, false);
                    neighbor.release_lock();
                    target.release_lock();
                    return true;
                }
                Err(_) => {
                    neighbor.release_lock();
                    prev_neighbor.release_lock();
                    target.release_lock();
                    return false;
                }
            }
        }
        false
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
