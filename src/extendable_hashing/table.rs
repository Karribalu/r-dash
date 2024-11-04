use crate::extendable_hashing::bucket::{
    check_bit_32, get_bitmap, get_count, stash_insert, Bucket, BucketError, K_NUM_PAIR_PER_BUCKET,
};
use crate::extendable_hashing::{BUCKET_MASK, K_FINGER_BITS, K_NUM_BUCKET, K_STASH_BUCKET};
use crate::hash::ValueT;
use crate::utils::hashing::calculate_hash;
use crate::utils::pair::{Key, Pair};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[derive(Debug, Hash)]
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
#[derive(Debug, Error)]
pub enum SplitError {
    #[error("Something wrong occurred")]
    InternalError(String),
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
impl<T> Hash for Table<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bucket.hash(state);
        self.local_depth.hash(state);
        self.pattern.hash(state);
        self.number.hash(state);
        self.state.hash(state);
    }
}
impl<T: PartialEq + Debug + Clone> Table<T> {
    pub fn new(pattern: usize) -> Self {
        let mut buckets = vec![];
        for _i in 0..(K_NUM_BUCKET + K_STASH_BUCKET) {
            buckets.push(Bucket::new());
        }
        Table {
            bucket: buckets,
            local_depth: 0,
            pattern,
            number: 0,
            state: Arc::from(TableState::Normal),
            lock_bit: Arc::new(Mutex::new(0)),
        }
    }
    /**
    Acquiring the lock for a table or segment is same as acquiring locks for all the buckets inside it
    */
    pub fn acquire_locks(&self) {
        for i in 0..K_NUM_BUCKET {
            self.bucket[i].get_lock();
        }
    }
    pub fn release_locks(&self) {
        for i in 0..K_NUM_BUCKET {
            self.bucket[i].release_lock();
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
                    Err(TableError::TableFull)
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
    This insert function is very similar to the traditional insert function.
    The only difference is we are trying to shift the values from a split bucket to its neighbor
     */
    pub fn insert_4_split(
        &mut self,
        key: &Key<T>,
        value: &ValueT,
        key_hash: usize,
        meta_hash: u8,
    ) -> Result<i32, TableError> {
        let bucket_index = bucket_index(key_hash, K_FINGER_BITS, BUCKET_MASK);
        let buckets_ptr = self.bucket.as_mut_ptr();
        unsafe {
            let target = &mut *buckets_ptr.add(bucket_index);

            let neighbor = &mut *buckets_ptr.add((bucket_index + 1) & BUCKET_MASK);
            let insert_bucket;
            let mut probe = false;
            if get_count(target.bitmap) <= get_count(neighbor.bitmap) {
                insert_bucket = target;
            } else {
                probe = true;
                insert_bucket = neighbor;
            }
            insert_bucket.get_lock();
            if get_count(insert_bucket.bitmap) < K_NUM_PAIR_PER_BUCKET {
                // Case where we can store the new element
                match insert_bucket.insert(key.clone(), value.clone(), meta_hash, probe) {
                    Ok(_) => {
                        insert_bucket.release_lock();
                        Ok(1)
                    }
                    Err(_) => {
                        println!("Error occurred while inserting a new element inside insert4split function");
                        Err(TableError::Internal)
                    }
                }
            } else {
                // Case where the target and neighbors are filled
                let next_neighbor = &mut *buckets_ptr.add((bucket_index + 2) & BUCKET_MASK);
                if !next_neighbor.try_get_lock() {
                    insert_bucket.release_lock();
                    return Err(TableError::UnableToAcquireLock(
                        "Unable to acquire the lock for next neighbor".to_string(),
                    ));
                }
                let displacement_res = Self::next_displace(
                    insert_bucket,
                    next_neighbor,
                    key.clone(),
                    value.clone(),
                    meta_hash,
                );
                next_neighbor.release_lock();
                if displacement_res {
                    // inserted in the neighboring bucket by displacement
                    insert_bucket.release_lock();
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
                    insert_bucket.release_lock();
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
                    insert_bucket.release_lock();
                    return Ok(3);
                }
                // Trying to insert in stash_bucket

                let mut stash_buckets: Vec<&mut Bucket<T>> = vec![];
                for i in 0..K_STASH_BUCKET {
                    stash_buckets.push(&mut *buckets_ptr.add(K_NUM_BUCKET + i));
                }
                target.get_lock();
                neighbor.get_lock();
                let stash_insert_res = stash_insert(
                    stash_buckets,
                    target,
                    neighbor,
                    key.clone(),
                    value.clone(),
                    meta_hash,
                );
                target.release_lock();
                neighbor.release_lock();
                if stash_insert_res {
                    Ok(4)
                } else {
                    Err(TableError::TableFull)
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
    /**
    This assumes the locks for all the buckets of this table are acquired
    1. Increments the pattern for auditing the change
    2. Creates a new table to divide the existing table to 2 parts.
    3. Rehash each entry in all the buckets to find the new positions in new table
    4.
    */
    pub fn split(&mut self, origin_key_hash: usize) -> Result<Table<T>, SplitError> {
        let new_pattern = (self.pattern << 1) + 1;
        let old_pattern = self.pattern << 1;
        self.state = Arc::from(TableState::Splitting);
        let mut next_table: Table<T> = Table::new(new_pattern);
        next_table.local_depth = self.local_depth + 1;
        next_table.state = Arc::from(TableState::Splitting);

        // Getting the lock of the first bucket to make sure the new table does not get split in between
        next_table.bucket[0].get_lock();

        let key_hash;
        let mut invalid_buckets: Vec<u32> = vec![];
        for i in 0..K_NUM_BUCKET {
            let current_bucket = &mut self.bucket[i];
            let mask = get_bitmap(current_bucket.bitmap);
            let mut invalid_mask = 0;
            for j in 0..K_NUM_PAIR_PER_BUCKET {
                if check_bit_32(mask, j) {
                    let current_pair: Pair<T> = current_bucket.pairs[j].unwrap();
                    if current_pair.key.is_pointer {
                        key_hash = calculate_hash(&current_pair.key);
                    } else {
                        key_hash = calculate_hash(&current_pair.key.key);
                    }
                    // FIXME: Verify if this is working as needed
                    if (key_hash >> (8 * size_of::<usize>() - self.local_depth - 1)) == new_pattern
                    {
                        invalid_mask = invalid_mask | (1 << j);
                        match next_table.insert_4_split(
                            &current_pair.key,
                            &current_pair.value,
                            key_hash,
                            current_bucket.finger_array[j],
                        ) {
                            Ok(_) => {}
                            Err(_) => {
                                println!(
                                    "Some error occurred while splitting {:?} in pair {:?}",
                                    current_bucket, current_pair.value
                                );
                                return Err(SplitError::InternalError(format!(
                                    "Some error occurred while splitting {:?} in pair {:?}",
                                    current_bucket, current_pair.value
                                )));
                            }
                        }
                    }
                }
            }
            invalid_buckets.append(invalid_mask);
        }

        // Splitting the values stored in Stash Buckets
        for i in K_NUM_BUCKET..K_NUM_BUCKET + K_STASH_BUCKET {
            let curr_stash_bucket = &mut self.bucket[i];
            let mask = get_bitmap(curr_stash_bucket);
            let mut invalid_mask = 0;
            for j in 0..K_NUM_PAIR_PER_BUCKET {
                if check_bit_32(mask, j) {
                    let current_pair: Pair<T> = curr_stash_bucket.pairs[j].unwrap();
                    if current_pair.key.is_pointer {
                        key_hash = calculate_hash(&current_pair.key);
                    } else {
                        key_hash = calculate_hash(&current_pair.key.key);
                    }
                    // FIXME: Verify if this is working as needed
                    if (key_hash >> (8 * size_of::<usize>() - self.local_depth - 1)) == new_pattern
                    {
                        invalid_mask = invalid_mask | (1 << j);
                        match next_table.insert_4_split(
                            &current_pair.key,
                            &current_pair.value,
                            key_hash,
                            curr_stash_bucket.finger_array[j],
                        ) {
                            Ok(_) => {
                                let bucket_ix = bucket_index(key_hash, K_FINGER_BITS, BUCKET_MASK);
                                let buckets_ptr = self.bucket.as_mut_ptr();
                                unsafe {
                                    let target = &mut *buckets_ptr.add(bucket_ix);
                                    let neighbor =
                                        &mut *buckets_ptr.add((bucket_ix + 1) & BUCKET_MASK);
                                    target.get_lock();
                                    neighbor.get_lock();
                                    target.unset_indicator(
                                        curr_stash_bucket.finger_array[j],
                                        neighbor,
                                        i,
                                    );
                                    target.release_lock();
                                    neighbor.release_lock();
                                }
                            }
                            Err(_) => {
                                println!(
                                    "Some error occurred while splitting {:?} in pair {:?}",
                                    curr_stash_bucket, current_pair.value
                                );
                                return Err(SplitError::InternalError(format!(
                                    "Some error occurred while splitting {:?} in pair {:?}",
                                    curr_stash_bucket, current_pair.value
                                )));
                            }
                        }
                    }
                }
            }
            invalid_buckets.append(invalid_mask);
        }
        // Invalidating the entries in target
        for i in 0..K_NUM_BUCKET + K_STASH_BUCKET {
            let current_bucket = &mut self.bucket[i];
            current_bucket.get_lock();
            current_bucket.bitmap = current_bucket.bitmap
                & (!(invalid_buckets[i] << 18))
                & (!(invalid_buckets[i] << 4));
            current_bucket.bitmap -= invalid_buckets[i].count_ones();
            current_bucket.release_lock();
        }
        next_table.pattern = new_pattern;
        Ok(next_table)
    }
}
pub fn bucket_index(hash: usize, finger_bits: usize, bucket_mask: usize) -> usize {
    // We do the finger_bits right shift because we use that last 8 bits for finger-print.
    (hash >> finger_bits) & bucket_mask
}

mod tests {
    use crate::extendable_hashing::bucket::meta_hash;
    use crate::extendable_hashing::table::Table;
    use crate::extendable_hashing::{K_MASK, K_NUM_BUCKET, K_STASH_BUCKET};
    use crate::utils::hashing::calculate_hash;
    use crate::utils::pair::Key;
    use std::collections::HashSet;
    use std::io;
    use std::io::Write;
    use std::time::SystemTime;

    #[test]
    pub fn test_new_table() {
        let table = Table::<i32>::new(0);
        assert_eq!(table.bucket.len(), K_NUM_BUCKET + K_STASH_BUCKET);
        assert_eq!(table.local_depth, 0);
        assert_eq!(table.pattern, 0);
        assert_eq!(table.number, 0);
    }
    #[test]
    pub fn test_acquire_locks() {
        let mut table = Table::<i32>::new(0);
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
        let mut table = Table::<i32>::new(0);
        let key = Key::new(&10);
        let value = String::from("Hello World");
        let hash = calculate_hash(&key.key);
        unsafe {
            let res = table.insert(key, value.into_bytes(), hash, meta_hash(hash));
            assert_eq!(res.unwrap(), 0);
        }
    }

    #[test]
    pub fn test_insert_for_all_buckets() {
        let mut table = Table::<i32>::new(0);
        let value = String::from("Hello World");
        let mut target_bucket = 0;
        let mut neighbor_bucket = 0;
        let mut next_neighbor_bucket = 0;
        let mut prev_neighbor_bucket = 0;
        let mut stash_bucket = 0;
        let mut failed_count = 0;
        let start_time = SystemTime::now();
        for i in 13000..14500 {
            let key = Key::new(&i);
            let hash = calculate_hash(&key.key);
            let meta_hash = (hash & K_MASK) as u8;
            let value = value.clone();
            // let bucket_index = bucket_index(hash, K_FINGER_BITS, BUCKET_MASK);
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
        let mut table = Table::<i32>::new(0);
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
            let key = Key::new(&i);
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
                }
            }
            if res.is_ok() {
                inserted.insert(i);
            }
        }
        for i in 13000..14500 {
            let key = Key::new(&i);
            let hash = calculate_hash(&key.key);
            let meta_hash = (hash & K_MASK) as u8;
            if inserted.contains(&i) {
                assert!(table.search(&key, hash, meta_hash).is_some());
            } else {
                assert!(table.search(&key, hash, meta_hash).is_none());
            }
        }
    }

    #[test]
    pub fn test_delete_for_all_buckets() {
        let mut table = Table::<i32>::new(0);
        let value = String::from("Hello World");
        let mut inserted = Vec::new();
        for i in 13000..14500 {
            let key = Key::new(&i);
            let hash = calculate_hash(&key.key);
            let meta_hash = (hash & K_MASK) as u8;
            let value = value.clone();
            let res = table.insert(key, value.into_bytes(), hash, meta_hash);
            match &res {
                Ok(_) => {
                    inserted.push(i);
                }
                Err(_) => {}
            }
        }
        for i in 0..(inserted.len() / 2) {
            let key = Key::new(&inserted[i]);
            let hash = calculate_hash(&key.key);
            let meta_hash = (hash & K_MASK) as u8;
            assert!(table.delete(&key, hash, meta_hash).is_ok());
            assert!(table.search(&key, hash, meta_hash).is_none());
        }
        for i in (inserted.len() / 2)..inserted.len() {
            let key = Key::new(&inserted[i]);
            let hash = calculate_hash(&key.key);
            let meta_hash = (hash & K_MASK) as u8;
            assert!(table.search(&key, hash, meta_hash).is_some());
        }
    }
}
