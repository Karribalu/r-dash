pub mod bucket;
mod directory;
mod table;

use crate::extendable_hashing::directory::Directory;
use crate::extendable_hashing::table::{Table, TableError};
use crate::hash::{Hash, ValueT};
use crate::utils::hashing::calculate_hash;
use crate::utils::pair::Key;
use std::sync::atomic::AtomicI32;

pub const K_NUM_BUCKET: usize = 64;
pub const K_STASH_BUCKET: usize = 2;
pub const K_FINGER_BITS: usize = 8;
pub const K_MASK: usize = (1 << K_FINGER_BITS) - 1;
// We use log2 to determine the number of bits to wrap the bucket index under K_NUM_BUCKET range
// Ex: for 64 the bucket mask would be 63, We had to use log instead of -1 to cover the possibility of K_NUM_BUCKET of not being power of 2;
pub const BUCKET_MASK: usize = (1 << K_NUM_BUCKET.ilog2()) - 1;
pub const STASH_MASK: usize = (1 << K_STASH_BUCKET.ilog2()) - 1;
pub const TAIL_MASK: u64 = (1 << 56) - 1;
pub const HEADER_MASK: u64 = ((1 << 8) - 1) << 56;
const DEFAULT_CAPACITY: usize = 10;
pub struct ExtendableHashing<T> {
    clean: bool,
    crash_version: u64,
    lock_and_counter: AtomicI32, // the MSB is the lock bit; remaining bits are used as the counter
    dir: Directory<T>,           // Yet to be implemented
}
impl<T> Hash<T> for ExtendableHashing<T> {
    fn new() -> Self {
        Self {
            clean: true,
            crash_version: 0,
            lock_and_counter: Default::default(),
            dir: Directory::new(DEFAULT_CAPACITY, 0),
        }
    }

    fn insert(&mut self, key: T, value: ValueT) {
        let key_hash = calculate_hash(&key);
        let meta_hash = (key_hash & K_MASK) as u8;
        'RETRY: loop {
            let dir = &self.dir;
            let dir_index = key_hash >> (8 * size_of::<usize>() - dir.global_depth);
            let target_table: &mut Table<T> = &mut dir.segments[dir_index as u64 & TAIL_MASK];
            let key = Key::new(&key);
            // TODO: Complete the recovery part
            let response = target_table.insert(key, value.clone(), key_hash, meta_hash);
            match response {
                Ok(_success_response) => {}
                Err(err) => {
                    match err {
                        TableError::TableFull => {
                            // Splitting the table
                            target_table.acquire_locks();
                            let dir_index = key_hash >> (8 * size_of::<usize>() - dir.global_depth);
                            let new_table: Table<T> = dir.segments[dir_index as u64 & TAIL_MASK];
                            // Verifying if the target table is not changed in between
                            if calculate_hash(target_table) != calculate_hash(&new_table) {
                                target_table.release_locks();
                                continue 'RETRY;
                            }
                            let new_bucket = target_table.split(key_hash);
                        }
                        TableError::Internal => {}
                        TableError::ItemDoesntExist => {}
                        TableError::UnableToAcquireLock(_) => {
                            continue 'RETRY;
                        }
                        TableError::KeyExists => {}
                        TableError::UnableToInsertKey => {}
                    }
                }
            }
        }
    }

    fn delete(key: T) {
        todo!()
    }

    fn get(key: T, buff: &mut [u8]) {
        todo!()
    }
}
impl<T> ExtendableHashing<T> {
    fn shut_down(&mut self) {
        self.clean = true;
        // Persist after that
    }
    /**
        This function assumes that the entire directory is locked before calling it
    */
    fn directory_doubling(
        &mut self,
        new_table_index: usize,
        new_bucket: &mut Table<T>,
        // old_bucket: &mut Table<T>,
    ) {
        let old_ds = &mut self.dir.segments;
        let global_depth = &self.dir.global_depth;
        println!("Directory is doubling to global depth {}", global_depth + 1);
        let current_capacity = 2.pow(global_depth);
        let mut new_d: Directory<T> = Directory::new(2 * current_capacity, self.dir.version + 1);
        let new_ds = &mut new_d.segments;
        for i in 0..current_capacity {
            new_ds[2 * i] = old_ds[i].clone();
            new_ds[2 * i + 1] = old_ds[i].clone();
        }
        // Replacing the old duplicate table with  new table
        new_ds[2 * new_table_index + 1] = new_bucket;
        new_d.depth_count += 1;

        self.dir = new_d;
    }
}
