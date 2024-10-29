use crate::extendable_hashing::bucket::{
    get_count, stash_insert, Bucket, BucketError, K_NUM_PAIR_PER_BUCKET,
};
use crate::extendable_hashing::{BUCKET_MASK, K_FINGER_BITS, K_NUM_BUCKET, K_STASH_BUCKET};
use crate::hash::ValueT;
use crate::utils::pair::{Key, Pair};
use std::fmt::Debug;
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
    #[error("Unable to acquire lock")]
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
    pub fn insert(
        &mut self,
        key: Key<T>,
        value: ValueT,
        key_hash: usize,
        meta_hash: u8, // directory: &Directory<T>,
    ) -> Result<i32, TableError> {
        let bucket_index = bucket_index(key_hash, K_FINGER_BITS, BUCKET_MASK);

        let buckets_ptr = self.bucket.as_mut_ptr();
        unsafe {
            let target = &mut *buckets_ptr.add(bucket_index);
            // (bucket_index + 1) & BUCKET_MASK used for wrapping up to 0 when the bucket_index is 63
            // (63 + 1) & 63 = 64 & 63 = 0
            let neighbor = &mut *buckets_ptr.add((bucket_index + 1) & BUCKET_MASK);
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
            if get_count(target.bitmap) == K_NUM_PAIR_PER_BUCKET
                && get_count(neighbor.bitmap) == K_NUM_PAIR_PER_BUCKET
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
                    target.release_lock();
                    neighbor.release_lock();
                    return Ok(2);
                }
                // Now we check for previous neighbor
                let prev_index = if bucket_index == 0 {
                    K_NUM_BUCKET - 1
                } else {
                    bucket_index - 1
                };
                let prev_neighbor = &mut *buckets_ptr.add(prev_index);
                if !prev_neighbor.try_get_lock() {
                    target.release_lock();
                    neighbor.release_lock();
                    return Err(TableError::UnableToAcquireLock(
                        "Unable to acquire the lock for previous neighbor".to_string(),
                    ));
                }

                let displacement_res = Self::prev_displace(
                    target,
                    prev_neighbor,
                    key.clone(),
                    value.clone(),
                    meta_hash,
                );

                prev_neighbor.release_lock();
                if displacement_res {
                    // inserted in the prev neighboring bucket by displacement
                    target.release_lock();
                    neighbor.release_lock();
                    return Ok(3);
                }

                // Now we try to insert in the stash buckets
                let stash_bucket = &mut *buckets_ptr.add(K_NUM_BUCKET);
                if !stash_bucket.try_get_lock() {
                    target.release_lock();
                    neighbor.release_lock();
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
                    Ok(4)
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
                            Ok(1)
                        }
                        Err(error) => {
                            target.release_lock();
                            neighbor.release_lock();
                            println!(
                                "Error while inserting the key in neighbor bucket {:?}",
                                error
                            );
                            Err(TableError::UnableToInsertKey)
                        }
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
    fn next_displace(
        target: &mut Bucket<T>,
        neighbor: &mut Bucket<T>,
        key: Key<T>,
        value: ValueT,
        meta_hash: u8,
    ) -> bool {
        let displace_index: i32 = target.find_org_displacement();
        if get_count(neighbor.bitmap) != K_NUM_PAIR_PER_BUCKET && displace_index != -1 {
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
        if get_count(prev_neighbor.bitmap) != K_NUM_PAIR_PER_BUCKET && displace_index != -1 {
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

    pub fn search(&mut self, key: &Key<T>, key_hash: usize, meta_hash: u8) -> Option<ValueT> {
        let bucket_index = bucket_index(key_hash, K_FINGER_BITS, BUCKET_MASK);

        let buckets_ptr = self.bucket.as_mut_ptr();
        unsafe {
            let target = &*buckets_ptr.add(bucket_index);

            let mut value: ValueT = vec![];
            if target.check_and_get(meta_hash, key, false, &mut value) {
                return Some(value);
            }
            let neighbor = &*buckets_ptr.add((bucket_index + 1) & BUCKET_MASK);
            if neighbor.check_and_get(meta_hash, key, true, &mut value) {
                return Some(value);
            }
            for i in 0..K_STASH_BUCKET {
                let current_stash_bucket = &*buckets_ptr.add(K_NUM_BUCKET + i);
                if current_stash_bucket.check_and_get(meta_hash, key, false, &mut value) {
                    return Some(value);
                }
            }
        }
        None
    }

    pub fn delete(
        &mut self,
        key: &Key<T>,
        key_hash: usize,
        meta_hash: u8,
    ) -> Result<(), BucketError> {
        let bucket_index = bucket_index(key_hash, K_FINGER_BITS, BUCKET_MASK);

        let buckets_ptr = self.bucket.as_mut_ptr();
        unsafe {
            let target = &mut *buckets_ptr.add(bucket_index);
            target.get_lock();
            match target.delete(key, meta_hash, false) {
                Ok(_) => {
                    target.release_lock();
                    return Ok(());
                }
                Err(err) => match err {
                    BucketError::KeyDoesNotExist => {}
                    _ => {
                        target.release_lock();
                        return Err(err);
                    }
                },
            };
            target.release_lock();
            let neighbor = &mut *buckets_ptr.add((bucket_index + 1) & BUCKET_MASK);
            neighbor.get_lock();
            match neighbor.delete(key, meta_hash, true) {
                Ok(_) => {
                    neighbor.release_lock();
                    return Ok(());
                }
                Err(err) => match err {
                    BucketError::KeyDoesNotExist => {}
                    _ => {
                        neighbor.release_lock();
                        return Err(err);
                    }
                },
            };
            neighbor.release_lock();
            for i in 0..K_STASH_BUCKET {
                let current_stash_bucket = &mut *buckets_ptr.add(K_NUM_BUCKET + i);
                current_stash_bucket.get_lock();
                match current_stash_bucket.delete(&key, meta_hash, false) {
                    Ok(_) => {
                        current_stash_bucket.release_lock();
                        return Ok(());
                    }
                    Err(err) => match err {
                        BucketError::KeyDoesNotExist => {}
                        _ => {
                            current_stash_bucket.release_lock();
                            return Err(err);
                        }
                    },
                };
                current_stash_bucket.release_lock();
            }
        }
        Err(BucketError::KeyDoesNotExist)
    }
}
pub fn bucket_index(hash: usize, finger_bits: usize, bucket_mask: usize) -> usize {
    // We do the finger_bits right shift because we use that last 8 bits for finger-print.
    (hash >> finger_bits) & bucket_mask
}

mod tests {
    use crate::extendable_hashing::bucket::meta_hash;
    use crate::extendable_hashing::table::{bucket_index, Table};
    use crate::extendable_hashing::{
        BUCKET_MASK, K_FINGER_BITS, K_MASK, K_NUM_BUCKET, K_STASH_BUCKET,
    };
    use crate::utils::hashing::calculate_hash;
    use crate::utils::pair::Key;
    use std::collections::HashSet;
    use std::io;
    use std::io::Write;
    use std::time::SystemTime;

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
    pub fn test_insert_basic() {
        let mut table = Table::<i32>::new();
        let key = Key::new(10);
        let value = String::from("Hello World");
        let hash = calculate_hash(&key.key);
        unsafe {
            let res = table.insert(key, value.into_bytes(), hash, meta_hash(hash));
            assert_eq!(res.unwrap(), 0);
        }
    }

    #[test]
    pub fn test_insert_for_all_buckets() {
        let mut table = Table::<i32>::new();
        let value = String::from("Hello World");
        let mut target_bucket = 0;
        let mut neighbor_bucket = 0;
        let mut next_neighbor_bucket = 0;
        let mut prev_neighbor_bucket = 0;
        let mut stash_bucket = 0;
        let mut failed_count = 0;
        let start_time = SystemTime::now();
        for i in 13000..14500 {
            let key = Key::new(i);
            let hash = calculate_hash(&key.key);
            let meta_hash = (hash & K_MASK) as u8;
            let value = value.clone();
            let bucket_index = bucket_index(hash, K_FINGER_BITS, BUCKET_MASK);
            // let str = format!(
            //     "{:?} inserted in {} with meta_hash {}",
            //     key, bucket_index, meta_hash);
            // file.write(&*str.into_bytes()).expect("TODO: panic message");
            // println!(
            //     "{:?} inserted in {} with meta_hash {}",
            //     key, bucket_index, meta_hash
            // );
            unsafe {
                let res = table.insert(key, value.into_bytes(), hash, meta_hash);
                match res {
                    Ok(ans) => match ans {
                        0 => target_bucket += 1,
                        1 => neighbor_bucket += 1,
                        2 => next_neighbor_bucket += 1,
                        3 => prev_neighbor_bucket += 1,
                        4 => stash_bucket += 1,
                        _ => {
                            println!("Some other bucket")
                        }
                    },
                    Err(err) => {
                        // println!("Error occurred while inserting element {:?}", err);
                        // println!(
                        //     "target: {} \n neighbor: {} \n next: {} \n prev: {} \n stash:{}",
                        //     target_bucket,
                        //     neighbor_bucket,
                        //     next_neighbor_bucket,
                        //     prev_neighbor_bucket,
                        //     stash_bucket
                        // );
                        failed_count += 1;
                    }
                }
            }
        }
        println!("Time elapsed {:?}", start_time.elapsed());
        println!(
            "target: {} \n neighbor: {} \n next: {} \n prev: {} \n stash:{} \n failed-count: {}",
            target_bucket,
            neighbor_bucket,
            next_neighbor_bucket,
            prev_neighbor_bucket,
            stash_bucket,
            failed_count
        );
        println!("failed {}", failed_count);
        io::stdout().flush().unwrap();
    }

    #[test]
    pub fn test_search_for_all_buckets() {
        let mut table = Table::<i32>::new();
        let value = String::from("Hello World");
        let mut target_bucket = 0;
        let mut neighbor_bucket = 0;
        let mut next_neighbor_bucket = 0;
        let mut prev_neighbor_bucket = 0;
        let mut stash_bucket = 0;
        let mut failed_count = 0;
        let mut inserted = HashSet::new();
        let mut stash_inserted = HashSet::new();
        for i in 13000..14500 {
            let key = Key::new(i);
            let hash = calculate_hash(&key.key);
            let meta_hash = (hash & K_MASK) as u8;
            let value = value.clone();

            let res = table.insert(key, value.into_bytes(), hash, meta_hash);
            match &res {
                Ok(ans) => match ans {
                    0 => target_bucket += 1,
                    1 => neighbor_bucket += 1,
                    2 => next_neighbor_bucket += 1,
                    3 => prev_neighbor_bucket += 1,
                    4 => {
                        stash_bucket += 1;
                        stash_inserted.insert(i);
                    }
                    _ => {
                        println!("Some other bucket")
                    }
                },
                Err(err) => {
                    failed_count += 1;
                    println!("failed {}", i);
                }
            }
            if res.is_ok() {
                inserted.insert(i);
                println!("inserted {} {:?}", i, res);
            }
        }
        for (i, item) in table.bucket.iter().enumerate() {
            println!("{} : {:?}", i, &item);
        }
        println!("failed count for inserting is {}", failed_count);
        let mut not_found = 0;
        let mut failed = HashSet::new();
        for i in 13000..14500 {
            let key = Key::new(i);
            let hash = calculate_hash(&key.key);
            let meta_hash = (hash & K_MASK) as u8;
            unsafe {
                match table.search(&key, hash, meta_hash) {
                    None => {
                        failed.insert(i);
                        not_found += 1;
                    }
                    Some(value) => {
                        println!("Item found for key {} value: {:?}", i, value);
                    }
                }
            }
        }
        let res = inserted.intersection(&failed);
        println!("Failed but inserted {:?}", res);
        println!("Stash inserted {:?}", stash_inserted);
        println!("Total search failures are {}", not_found);
    }

    #[test]
    pub fn test_delete_for_all_buckets() {
        let mut table = Table::<i32>::new();
        let value = String::from("Hello World");
        let mut inserted = Vec::new();
        for i in 13000..14500 {
            let key = Key::new(i);
            let hash = calculate_hash(&key.key);
            let meta_hash = (hash & K_MASK) as u8;
            let value = value.clone();
            let res = table.insert(key, value.into_bytes(), hash, meta_hash);
            match &res {
                Ok(_) => {
                    inserted.push(i);
                }
                Err(err) => {

                    // println!("failed {}", i);
                }
            }
        }
        for i in 0..(inserted.len() / 2) {
            let key = Key::new(inserted[i]);
            let hash = calculate_hash(&key.key);
            let meta_hash = (hash & K_MASK) as u8;
            assert!(table.delete(&key, hash, meta_hash).is_ok());
            assert!(table.search(&key, hash, meta_hash).is_none());
        }
        for i in (inserted.len() / 2)..inserted.len() {
            let key = Key::new(inserted[i]);
            let hash = calculate_hash(&key.key);
            let meta_hash = (hash & K_MASK) as u8;
            assert!(table.search(&key, hash, meta_hash).is_some());
        }

        // for i in deleted {
        //     let key = Key::new(i);
        //     let hash = calculate_hash(&key.key);
        //     let meta_hash = (hash & K_MASK) as u8;
        //     assert!(table.search(&key, hash, meta_hash).is_some());
        // }
        // assert_eq!(failed, inserted.len() / 2);
    }
}
