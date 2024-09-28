use crate::extendable_hashing::bucket::{get_count, stash_insert, Bucket};
use crate::extendable_hashing::{BUCKET_MASK, K_FINGER_BITS, K_NUM_BUCKET, K_STASH_BUCKET};
use crate::hash::ValueT;
use crate::utils::pair::{Key, Pair};
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
    #[error("Unable to insert key in any of the buckets")]
    UnableToInsertKey,
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
    state: Arc<TableState>,
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
    pub unsafe fn insert(
        &mut self,
        key: Key<T>,
        value: ValueT,
        key_hash: usize,
        meta_hash: u8, // directory: &Directory<T>,
    ) -> Result<i32, TableError> {
        let bucket_index = bucket_index(key_hash, K_FINGER_BITS, BUCKET_MASK);

        let buckets_ptr = self.bucket.as_mut_ptr();
        let target = &mut *buckets_ptr.add(bucket_index);
        // (bucket_index + 1) & BUCKET_MASK used for wrapping up to 0 when the bucket_index is 63
        // (63 + 1) & 63 = 64 & 63 = 0
        let neighbor = &mut *buckets_ptr.add((bucket_index + 1) & BUCKET_MASK);
        // let neighbor = &mut Bucket::new();
        target.get_lock();
        if !neighbor.try_get_lock() {
            target.release_lock();
            return Err(TableError::UnableToAcquireLock(
                "Unable to acquire neighbor lock".to_string(),
            ));
        }
        // let dir = directory;
        // TODO: Check if we need to add the next block
        // Trying to get the MSBs of the key to determine the segment index
        // let segment_index = key_hash >> (8 * size_of::<usize>() - dir.global_depth);
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
            let next_neighbor = &mut *buckets_ptr.add((bucket_index + 2) & BUCKET_MASK);
            if !next_neighbor.try_get_lock() {
                neighbor.release_lock();
                target.release_lock();
                return Err(TableError::UnableToAcquireLock(
                    "Unable to acquire the lock for next neighbor".to_string(),
                ));
            }

            let displacement_res = Self::next_displace(
                neighbor,
                next_neighbor,
                key.clone(),
                value.clone(),
                meta_hash,
            );
            next_neighbor.release_lock();
            if displacement_res {
                // inserted in the neighboring bucket by displacement
                return Ok(1);
            }
            // Now we check for previous neighbor
            let prev_index = if bucket_index == 0 {
                K_NUM_BUCKET - 1
            } else {
                bucket_index - 1
            };
            let prev_neighbor = &mut *buckets_ptr.add(prev_index);
            // let prev_neighbor = &mut Bucket::new();
            if !prev_neighbor.try_get_lock() {
                target.release_lock();
                neighbor.release_lock();
                return Err(TableError::UnableToAcquireLock(
                    "Unable to acquire the lock for previous neighbor".to_string(),
                ));
            }

            let displacement_res = Self::prev_displace(
                neighbor,
                prev_neighbor,
                key.clone(),
                value.clone(),
                meta_hash,
            );
            prev_neighbor.release_lock();
            if displacement_res {
                // inserted in the prev neighboring bucket by displacement
                return Ok(2);
            }

            // Now we try to insert in the stash buckets
            let stash_bucket = &mut *buckets_ptr.add(K_NUM_BUCKET);
            if !stash_bucket.try_get_lock() {
                return Err(TableError::UnableToAcquireLock(
                    "Unable to acquire the lock for stash bucket".to_string(),
                ));
            }
            let mut stash_buckets: Vec<&mut Bucket<T>> = vec![];
            for i in 0..K_STASH_BUCKET {
                stash_buckets.push(&mut *buckets_ptr.add(K_NUM_BUCKET + i));
            }
            let stash_insert_res = stash_insert(
                stash_buckets,
                target,
                neighbor,
                key.clone(),
                value.clone(),
                meta_hash,
            );
            stash_bucket.release_lock();
            target.release_lock();
            neighbor.release_lock();
            if stash_insert_res {
                Ok(3)
            } else {
                Err(TableError::UnableToInsertKey)
            }
        } else {
            // Insert in the bucket which has lesser keys
            if get_count(target.bitmap) <= get_count(neighbor.bitmap) {
                match target.insert(key.clone(), value.clone(), meta_hash, false) {
                    Ok(_) => {
                        target.release_lock();
                        neighbor.release_lock();
                        Ok(0)
                    }
                    Err(error) => {
                        target.release_lock();
                        neighbor.release_lock();
                        println!("Error while inserting the key in target bucket {:?}", error);
                        Err(TableError::UnableToInsertKey)
                    }
                }
            } else {
                match neighbor.insert(key.clone(), value.clone(), meta_hash, true) {
                    Ok(_) => {
                        target.release_lock();
                        neighbor.release_lock();
                        Ok(0)
                    }
                    Err(error) => {
                        target.release_lock();
                        neighbor.release_lock();
                        println!("Error while inserting the key in target bucket {:?}", error);
                        Err(TableError::UnableToInsertKey)
                    }
                }
            }
        }
    }
    /**
    Takes a reference Bucket, and it's neighbor, Moves one eligible pair to it's neighbor bucket.
    Adds the new Pair to the reference bucket.
    Returns boolean True - Success, False - Failure
    */
    pub fn next_displace(
        target: &mut Bucket<T>,
        neighbor: &mut Bucket<T>,
        key: Key<T>,
        value: ValueT,
        meta_hash: u8,
    ) -> bool {
        let displace_index: i32 = target.find_org_displacement();
        if get_count(neighbor.bitmap) != K_NUM_BUCKET as u32 && displace_index != -1 {
            let neighbor_pair: Pair<T> = target.pairs[displace_index as usize]
                .clone()
                .unwrap()
                .clone();
            return match neighbor.insert(
                neighbor_pair.key,
                neighbor_pair.value,
                target.finger_array[displace_index as usize],
                true,
            ) {
                Ok(_) => {
                    target.unset_hash(displace_index as u32);
                    target.insert_displace(key, value, meta_hash, displace_index, true);
                    true
                }
                Err(_) => false,
            };
        }
        false
    }
    /**
    Takes a reference Bucket, and it's previous neighbor, Moves one eligible pair to it's neighbor bucket.
    Adds the new Pair to the reference bucket.
    Returns boolean True - Success, False - Failure
    The only difference is we pass Probe as false to prev_neighbor bucket which defines we store the pair in extra slots other than 14
     */
    pub fn prev_displace(
        target: &mut Bucket<T>,
        prev_neighbor: &mut Bucket<T>,
        key: Key<T>,
        value: ValueT,
        meta_hash: u8,
    ) -> bool {
        let displace_index = target.find_probe_displacement();
        if get_count(prev_neighbor.bitmap) != K_NUM_BUCKET as u32 && displace_index != -1 {
            let neighbor_pair: Pair<T> = target.pairs[displace_index as usize]
                .clone()
                .unwrap()
                .clone();
            return match prev_neighbor.insert(
                neighbor_pair.key,
                neighbor_pair.value,
                target.finger_array[displace_index as usize],
                false,
            ) {
                Ok(_) => {
                    target.unset_hash(displace_index as u32);
                    target.insert_displace(key, value, meta_hash, displace_index, false);
                    true
                }
                Err(_) => false,
            };
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
    use crate::hash::ValueT;
    use crate::utils::hashing::calculate_hash;
    use crate::utils::pair::Key;

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
    #[test]
    pub fn test_insert_basic(){
        let mut table = Table::<i32>::new();
        let key = Key::new(10);
        let value = String::from("Hello World");
        let hash = calculate_hash(&key.key);
        unsafe {
            let res = table.insert(key, value.into_bytes(), hash as usize, hash);
            assert_eq!(res.unwrap(), 0);
        }
    }
}
