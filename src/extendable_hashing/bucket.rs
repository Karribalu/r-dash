use crate::extendable_hashing::{K_MASK, K_STASH_BUCKET};
use crate::hash::ValueT;
use crate::utils::pair::{Key, Pair};
use crate::utils::var_compare;
use std::error::Error;
use std::fmt::Debug;
use std::ops::BitAnd;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use std::sync::{atomic, Arc};
use thiserror::Error;

pub const K_NUM_PAIR_PER_BUCKET: u32 = 14;
const COUNT_MASK: u32 = (1 << 4) - 1;
const OVERFLOW_BITMAP_MASK: u8 = (1 << 4) - 1;
const OVERFLOW_SET: u8 = 1 << 4;
const STASH_BUCKET: u8 = 2;
const STASH_MASK: usize = (1 << STASH_BUCKET.ilog2()) - 1;
const ALLOC_MASK: usize = (1 << K_NUM_PAIR_PER_BUCKET) - 1;
const LOCK_SET: u32 = 1 << 31;
const LOCK_MASK: u32 = (1 << 31) - 1;
#[derive(Debug, Clone)]
pub struct Bucket<T: PartialEq> {
    pub pairs: Vec<Option<Pair<T>>>,
    pub unused: [u8; 2],
    pub overflow_count: u8,
    pub overflow_member: u8,
    pub overflow_index: u8,
    pub overflow_bitmap: u8,
    pub finger_array: [u8; 18], /*only use the first 14 bytes, can be accelerated by SSE instruction,0-13 for finger, 14-17 for overflowed*/
    pub bitmap: u32,            // allocation bitmap + pointer bitmap + counter
    pub version_lock: Arc<AtomicU32>,
}
/**
for Bitmap: 32 bits
0000 0000 1110 00 00 0000 0000 0000 0101
First 14 bits are for allocating the buckets
Next 4 is for stash buckets
Next 10 is for pointers
The last 4 bits determines the number of slots filled in the bucket,
In the above example 5 slots are filled

*/

impl<T: Debug + Clone + PartialEq> Bucket<T> {
    pub fn new() -> Self {
        Bucket {
            pairs: vec![None; 18],
            unused: [0, 0],
            overflow_count: 0,
            overflow_member: 0,
            overflow_index: 0,
            overflow_bitmap: 0,
            finger_array: [0; 18],
            bitmap: 0,
            version_lock: Arc::new(AtomicU32::new(0)),
        }
    }
    /**
        It will wait till the executor is able to get the lock.
        Ensure there are no threads which are acquired but not released.
    */
    pub fn get_lock(&self) {
        let mut old_value: u32;
        let mut new_value: u32;
        loop {
            loop {
                // We check if the lock is acquired or not, If not we store the local old_value in un locked state
                old_value = self.version_lock.load(atomic::Ordering::Acquire);
                if old_value & LOCK_SET == 0 {
                    old_value &= LOCK_MASK;
                    let hello = old_value;
                    break;
                }
            }
            // We lock the new_value to set the lock in the next step
            new_value = old_value | LOCK_SET;

            // We check if the version lock is the old_value, If not there is another thread which acquired the lock,
            // If the lock value is same as old_value, We can change the lock to new_value which is the locked state.
            if self
                .version_lock
                .compare_exchange(old_value, new_value, Acquire, Acquire)
                .is_ok()
            {
                break;
            }
        }
    }
    pub fn release_lock(&self) {
        let version_lock = self.version_lock.load(atomic::Ordering::Acquire);
        self.version_lock
            .store(version_lock + 1 - LOCK_SET, Release);
    }
    pub fn reset_lock(&self) {
        self.version_lock.store(0, SeqCst);
    }
    /**
        Doesn't block the thread till it acquire the lock, Tries to get the lock in the first attempt and returns true if it succeeds else false
    */
    pub fn try_get_lock(&self) -> bool {
        let v = self.version_lock.load(Acquire);
        if v & LOCK_SET != 0 {
            return false;
        }
        let old_value = v & LOCK_MASK;
        let new_value = old_value | LOCK_SET;
        self.version_lock
            .compare_exchange(old_value, new_value, Acquire, Acquire)
            .is_ok()
    }
    pub fn find_empty_slot(&self) -> i32 {
        if get_count(self.bitmap) == K_NUM_PAIR_PER_BUCKET {
            return -1;
        }
        let mask = !get_bitmap(self.bitmap);
        mask.trailing_zeros() as i32
    }
    pub fn is_lock(&self) -> bool {
        self.version_lock.load(Acquire) != 0
    }

    /*true indicates overflow, needs extra check in the stash*/
    pub fn test_overflow(&self) -> bool {
        self.overflow_count > 0
    }

    pub fn test_stash_check(&self) -> bool {
        self.overflow_bitmap & OVERFLOW_SET > 0
    }

    pub fn clear_stash_check(&mut self) {
        self.overflow_bitmap = self.overflow_bitmap & !OVERFLOW_SET
    }

    /**
    This function is used to look for open slots in stash_buckets of the probing bucket
    If there is an available slot then uses it or else it does the exact search in the neighbor bucket
    */
    pub fn set_indicator(&mut self, meta_hash: u8, neighbor: &mut Bucket<T>, pos: u8) {
        let mut mask: u8 = self.overflow_bitmap & OVERFLOW_BITMAP_MASK;
        mask = !mask;
        let mut index: u8 = mask.trailing_zeros() as u8;

        if index < 4 {
            // Means a slot is free in the probing bucket
            self.finger_array[(K_NUM_PAIR_PER_BUCKET + index as u32) as usize] = meta_hash;
            self.overflow_bitmap = (1 << index) | self.overflow_bitmap;
            self.overflow_index =
                self.overflow_index & (!(3 << (index * 2))) | (pos << (index * 2));
        } else {
            // Looking for the free slot in neighboring bucket
            mask = neighbor.overflow_bitmap & OVERFLOW_BITMAP_MASK;
            mask = !mask;
            index = mask.trailing_zeros() as u8;
            if index < 4 {
                neighbor.finger_array[(K_NUM_PAIR_PER_BUCKET + index as u32) as usize] = meta_hash;
                neighbor.overflow_bitmap = (1 << index) | neighbor.overflow_bitmap;
                neighbor.overflow_index =
                    neighbor.overflow_index & (!(3 << (index * 2))) | (pos << (index * 2));
                // Overflow member is only used to track that if there are some overflowed members in neighboring buckets
                neighbor.overflow_member = (1 << index) | neighbor.overflow_member;
            } else {
                self.overflow_count += 1;
            }
        }
    }

    pub fn unset_indicator(
        &mut self,
        meta_hash: u8,
        neighbor: &mut Bucket<T>,
        key: Key<T>,
        pos: u64,
    ) {
        // TODO: Verify it it is u64 or u8
        let mut clear_success = false;
        let mask1 = self.overflow_bitmap & OVERFLOW_BITMAP_MASK;
        for i in 0..4 {
            // First looking for the match in the probing bucket
            if check_bit(mask1, i)
                && (self.finger_array[(14 + i) as usize] == meta_hash)
                && ((1 << i) & self.overflow_member == 0)
                && (((self.overflow_index >> (2 * i)) as usize & STASH_MASK) == pos as usize)
            {
                self.overflow_bitmap = self.overflow_bitmap & !(1 << i);
                self.overflow_index = self.overflow_index & (!(3 << (i * 2)));
                assert_eq!((self.overflow_index >> (i * 2)) as usize & STASH_MASK, 0);
                clear_success = true;
                break;
            }
        }

        let mask2 = neighbor.overflow_bitmap & OVERFLOW_BITMAP_MASK;
        if !clear_success {
            // If the match is not found then we look for neighboring bucket
            for i in 0..4 {
                if check_bit(mask2, i)
                    && (neighbor.finger_array[(14 + i) as usize] == meta_hash)
                    && ((1 << i) & neighbor.overflow_member == 0)
                    && (((neighbor.overflow_index >> (2 * i)) as usize & STASH_MASK)
                        == pos as usize)
                {
                    neighbor.overflow_bitmap = neighbor.overflow_bitmap & !(1 << i);
                    neighbor.overflow_index = neighbor.overflow_index & (!(3 << (i * 2)));
                    neighbor.overflow_member = neighbor.overflow_member & !(1 << i);
                    assert_eq!(
                        (neighbor.overflow_index >> (i * 2)) as usize & STASH_MASK,
                        0
                    );
                    clear_success = true;
                    break;
                }
            }
        }
        if !clear_success {
            self.overflow_count -= 1;
        }
        // If we don't find it in both the buckets
        // We check if the overflow count is 0 and empty the stash if yes

        let mask1 = self.overflow_bitmap & OVERFLOW_BITMAP_MASK;
        let mask2 = neighbor.overflow_bitmap & OVERFLOW_BITMAP_MASK;
        if mask1 & !self.overflow_member == 0
            && self.overflow_count == 0
            && mask2 & neighbor.overflow_member == 0
        {
            self.clear_stash_check();
        }
    }

    pub fn set_hash(&mut self, index: i32, meta_hash: u8, probe: bool) {
        self.finger_array[index as usize] = meta_hash;
        let mut new_bitmap = self.bitmap | (1 << (index + 18));
        if probe {
            // Meaning the value is being hosted but not owned by the bucket
            new_bitmap = new_bitmap | (1 << (index + 4));
        }
        new_bitmap += 1; // Increasing the count of occupied slots i.e. last 4 bits
        self.bitmap = new_bitmap;
    }
    pub fn unset_hash(&mut self, index: u32) {
        let mut new_bitmap = self.bitmap & !(1 << (index + 18)) & (!(1 << (index + 4)));
        assert!(get_count(self.bitmap) <= K_NUM_PAIR_PER_BUCKET);
        assert!(get_count(self.bitmap) > 0);
        new_bitmap -= 1;
        self.bitmap = new_bitmap;
    }
    pub fn insert(
        &mut self,
        key: Key<T>,
        value: ValueT,
        meta_hash: u8,
        probe: bool,
    ) -> Result<i32, BucketError> {
        let slot = self.find_empty_slot();
        assert!(slot < K_NUM_PAIR_PER_BUCKET as i32);
        if slot == -1 {
            println!("Cannot find the empty slot, for key {:?}", key);
            return Err(BucketError::BucketFull);
        }
        self.pairs[slot as usize] = Some(Pair::new(key, value));
        self.set_hash(slot, meta_hash, probe);
        Ok(slot)
    }
    pub fn check_and_get(
        &self,
        meta_hash: u8,
        key: &Key<T>,
        probe: bool,
        value: &mut ValueT,
    ) -> bool {
        let mut mask: u32 = 0;
        // TODO: We can replace this loop with SIMD instruction
        for (i, &finger) in self.finger_array.iter().enumerate() {
            if finger == meta_hash {
                // Setting the corresponding bit which matched with the hash;
                mask |= 1 << i;
            }
        }
        if probe {
            // Meaning We are looking the key in the probing (neighbor) bucket
            mask = mask & get_bitmap(self.bitmap) & get_member(self.bitmap);
        } else {
            mask = mask & get_bitmap(self.bitmap) & !get_member(self.bitmap);
        }

        if mask == 0 {
            // No match found
            return false;
        }
        if key.is_pointer {
            // Variable length key
            for i in 0..14 {
                if check_bit_32(mask, i as u32) {
                    let ex_key = &self.pairs[i].clone().unwrap().key;
                    if var_compare(
                        &key.pointed_key,
                        key.length,
                        &ex_key.pointed_key,
                        ex_key.length,
                    ) {
                        *value = self.pairs[i].clone().unwrap().value;
                        return true;
                    }
                }
            }
        } else {
            // Fixed length keys
            for i in (0..14).step_by(4) {
                let iu = i as usize;
                if check_bit_32(mask, i) && self.pairs[iu].clone().unwrap().key.key == key.key {
                    *value = self.pairs[iu].clone().unwrap().value;
                    return true;
                }
                if check_bit_32(mask, i + 1)
                    && self.pairs[iu + 1].clone().unwrap().key.key == key.key
                {
                    *value = self.pairs[iu + 1].clone().unwrap().value;
                    return true;
                }
                if check_bit_32(mask, i + 2)
                    && self.pairs[iu + 2].clone().unwrap().key.key == key.key
                {
                    *value = self.pairs[iu + 2].clone().unwrap().value;
                    return true;
                }
                if check_bit_32(mask, i + 3)
                    && self.pairs[iu + 3].clone().unwrap().key.key == key.key
                {
                    *value = self.pairs[iu + 3].clone().unwrap().value;
                    return true;
                }
            }
            if check_bit_32(mask, 12) && self.pairs[12].clone().unwrap().key.key == key.key {
                *value = self.pairs[12].clone().unwrap().value;
                return true;
            }
            if check_bit_32(mask, 13) && self.pairs[13].clone().unwrap().key.key == key.key {
                *value = self.pairs[13].clone().unwrap().value;
                return true;
            }
        }
        false
    }
    pub(crate) fn insert_displace(
        &mut self,
        key: Key<T>,
        value: ValueT,
        meta_hash: u8,
        slot: i32,
        probe: bool,
    ) {
        self.pairs[slot as usize] = Some(Pair::new(key, value));
        self.set_hash(slot, meta_hash, probe);
    }
    pub fn delete(&mut self, key: Key<T>, meta_hash: u8, probe: bool) -> Result<(), BucketError> {
        /*do the simd and check the key, then do the delete operation*/
        let mut mask: u32 = 0;
        // TODO: Can be replaced by a simd operation
        for (i, &finger) in self.finger_array.iter().enumerate() {
            if finger == meta_hash {
                // Setting the corresponding bit which matched with the hash;
                mask |= 1 << i;
            }
        }
        if probe {
            mask = mask & get_bitmap(self.bitmap) & get_member(self.bitmap);
        } else {
            mask = mask & get_bitmap(self.bitmap) & !get_member(self.bitmap);
        }
        if key.is_pointer {
            if mask != 0 {
                for i in (0..14).step_by(4) {
                    let iu = i as usize;
                    if check_bit_32(mask, i)
                        && self.pairs[iu].clone().unwrap().key.pointed_key == key.pointed_key
                    {
                        self.unset_hash(i);
                        self.pairs[iu] = None;
                        return Ok(());
                    }
                    if check_bit_32(mask, i + 1)
                        && self.pairs[iu + 1].clone().unwrap().key.pointed_key == key.pointed_key
                    {
                        self.unset_hash(i);
                        self.pairs[iu] = None;
                        return Ok(());
                    }
                    if check_bit_32(mask, i + 2)
                        && self.pairs[iu + 2].clone().unwrap().key.pointed_key == key.pointed_key
                    {
                        self.unset_hash(i);
                        self.pairs[iu] = None;
                        return Ok(());
                    }
                    if check_bit_32(mask, i + 3)
                        && self.pairs[iu + 3].clone().unwrap().key.pointed_key == key.pointed_key
                    {
                        self.unset_hash(i);
                        self.pairs[iu] = None;
                        return Ok(());
                    }
                }
                if check_bit_32(mask, 12)
                    && self.pairs[12].clone().unwrap().key.pointed_key == key.pointed_key
                {
                    self.unset_hash(12);
                    self.pairs[12] = None;
                    return Ok(());
                }
                if check_bit_32(mask, 13)
                    && self.pairs[13].clone().unwrap().key.pointed_key == key.pointed_key
                {
                    self.unset_hash(13);
                    self.pairs[13] = None;
                    return Ok(());
                }
            }
        } else {
            for i in (0..12).step_by(4) {
                let iu = i as usize;
                if check_bit_32(mask, i) && self.pairs[iu].clone().unwrap().key.key == key.key {
                    self.unset_hash(i);
                    self.pairs[iu] = None;
                    return Ok(());
                }
                if check_bit_32(mask, i + 1)
                    && self.pairs[iu + 1].clone().unwrap().key.key == key.key
                {
                    self.unset_hash(i + 1);
                    self.pairs[iu + 1] = None;
                    return Ok(());
                }
                if check_bit_32(mask, i + 2)
                    && self.pairs[iu + 2].clone().unwrap().key.key == key.key
                {
                    self.unset_hash(i + 1);
                    self.pairs[iu + 1] = None;
                    return Ok(());
                }
                if check_bit_32(mask, i + 3)
                    && self.pairs[iu + 3].clone().unwrap().key.key == key.key
                {
                    self.unset_hash(i + 1);
                    self.pairs[iu + 1] = None;
                    return Ok(());
                }
            }
            if check_bit_32(mask, 12) && self.pairs[12].clone().unwrap().key.key == key.key {
                self.unset_hash(12);
                self.pairs[12] = None;
                return Ok(());
            }
            if check_bit_32(mask, 13) && self.pairs[13].clone().unwrap().key.key == key.key {
                self.unset_hash(13);
                self.pairs[13] = None;
                return Ok(());
            }
        }
        Err(BucketError::ItemDoesntExist)
    }
    pub fn unique_check(
        &self,
        meta_hash: u8,
        key: &Key<T>,
        neighbor: &Bucket<T>,
        stash: &[Bucket<T>],
    ) -> bool {
        let mut value: ValueT = vec![];
        // We are only looking for the neighboring buckets
        if self.check_and_get(meta_hash, &key, false, &mut value)
            || neighbor.check_and_get(meta_hash, &key, true, &mut value)
        {
            return false;
        }
        if self.test_stash_check() {
            let mut test_stash = false;
            if self.test_overflow() {
                // Overflow is there we have to check in stash buckets
                test_stash = true;
            } else {
                // Check in the overflow buckets and decide if we have to check in the stash buckets
                let mask = self.overflow_bitmap & OVERFLOW_BITMAP_MASK;
                if mask != 0 {
                    for i in 0..4usize {
                        if check_bit(mask, i as u32)
                            && self.finger_array[14 + i] == meta_hash
                            && ((1 << i) & self.overflow_member) == 0
                        {
                            test_stash = true;
                            break;
                        }
                    }
                }
                if !test_stash {
                    let mask = neighbor.overflow_bitmap & OVERFLOW_BITMAP_MASK;
                    if mask != 0 {
                        for i in 0..4usize {
                            if check_bit(mask, i as u32)
                                && neighbor.finger_array[14 + i] == meta_hash
                                && ((1 << i) & neighbor.overflow_member) == 0
                            {
                                test_stash = true;
                                break;
                            }
                        }
                    }
                }
            }
            if test_stash {
                for i in 0..K_STASH_BUCKET {
                    let curr_bucket = &stash[i];
                    if curr_bucket.check_and_get(meta_hash, &key, false, &mut value) {
                        return false;
                    }
                }
            }
        }
        true
    }

    pub fn reset_overflow_fp(&mut self) {
        self.overflow_bitmap = 0;
        self.overflow_index = 0;
        self.overflow_member = 0;
        self.overflow_count = 0;
        self.clear_stash_check()
    }
    pub fn find_org_displacement(&self) -> i32 {
        let mask = get_inverse_member(self.bitmap);
        if mask == 0 {
            return -1;
        }
        mask.trailing_zeros() as i32
    }

    pub fn find_probe_displacement(&self) -> i32 {
        let mask = get_member(self.bitmap);
        if mask == 0 {
            return -1;
        }
        mask.trailing_zeros() as i32
    }
}
#[derive(Debug, Error)]
pub enum BucketError {
    #[error("The bucket is full")]
    BucketFull,
    #[error("Internal Error")]
    Internal,
    #[error("Item does not exist")]
    ItemDoesntExist,
}

/**
0000 0000 0001 1111 0000 0000 0000 0101 & 0000 0000 0000 0000 0000 0000 0000 1111
it returns 5 as the count
*/
pub fn get_count(var: u32) -> u32 {
    var & COUNT_MASK
}

/**
We remove last 4 bits which are for count
and 14 bits before that which are for pointers
*/
pub fn get_bitmap(var: u32) -> u32 {
    var >> 18
}
// It returns the overflowed bucket space
pub fn get_member(var: u32) -> u32 {
    (var >> 4) & ALLOC_MASK as u32
}

pub fn check_bit(var: u8, pos: u32) -> bool {
    var & (1 << pos) > 0
}

pub fn check_bit_32(var: u32, pos: u32) -> bool {
    var & (1 << pos) > 0
}
// It returns the empty overflow bucket space
pub fn get_inverse_member(var: u32) -> u32 {
    !(var >> 4) & ALLOC_MASK as u32
}
pub fn meta_hash(var: usize) -> u8 {
    (var & K_MASK) as u8
}
pub fn stash_insert<T: Debug + Clone + PartialEq>(
    stash_buckets: Vec<&mut Bucket<T>>,
    target: &mut Bucket<T>,
    neighbor: &mut Bucket<T>,
    key: Key<T>,
    value: ValueT,
    meta_hash: u8,
) -> bool {
    let mut index = 0;
    for stash_bucket in stash_buckets {
        if get_count(stash_bucket.bitmap) < K_NUM_PAIR_PER_BUCKET {
            return match stash_bucket.insert(key, value, meta_hash, false) {
                Ok(_) => {
                    target.set_indicator(meta_hash, neighbor, index);
                    println!("Added the pair to the stash bucket");
                    true
                }
                Err(e) => {
                    println!("Some error occurred while inserting element to stash buckets");
                    false
                }
            };
        }
        index += 1;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extendable_hashing::K_MASK;
    use crate::utils::hashing::calculate_hash;
    use std::ops::AddAssign;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn test_locking_with_multiple_thread() {
        let bucket: Arc<Bucket<i32>> = Arc::new(Bucket::new());
        let mut handles = vec![];
        let num_of_threads = 1000;
        let total_dur = Instant::now();
        for i in 0..num_of_threads {
            let mut cloned = Arc::clone(&bucket);
            let handle = thread::spawn(move || {
                let start = Instant::now();
                cloned.get_lock();
                let elapsed = start.elapsed();
                // println!("Thread {} got lock {:?}", i,elapsed);
                cloned.release_lock();
                elapsed
            });
            handles.push(handle);
        }
        println!("Thread creation took{:?}", total_dur.elapsed());
        let mut duration = Duration::new(0, 0);
        let total_dur = Instant::now();
        for handle in handles {
            let dur = handle.join().unwrap();
            duration.add_assign(dur);
        }
        println!("Thread execution took {:?}", total_dur.elapsed());
        println!(
            " average {} nanos {} micro seconds {} seconds",
            duration.as_nanos() as f64 / num_of_threads as f64,
            duration.as_micros(),
            duration.as_secs()
        );
    }
    #[test]
    fn test_bucket_insertion_with_fixed_keys() {
        let mut bucket: Bucket<i32> = Bucket::new();
        let mut success = 0;
        let mut ans = vec![];
        for i in 10000..10020 {
            let mut string = String::from(format!("let hash = calculate_hash(&key) {}", i));
            let value: ValueT = string.clone().into_bytes();

            let hash = calculate_hash(&i);
            let key = Key::new(i);
            let response = bucket.insert(key, value, meta_hash(hash), true);
            match response {
                Ok(_) => {
                    success += 1;
                    ans.push(i);
                }
                Err(err) => {
                    println!("{:?}", err);
                }
            }
        }
        ans.push(500);
        for key_str in &ans {
            let mut vector = vec![];
            let cloned_key = key_str.clone();
            let hash = calculate_hash(key_str);
            let key = Key::new(*key_str);
            let start = Instant::now();
            // Calculate the elapsed time
            if bucket.check_and_get(hash as u8, &key, false, &mut vector) {
                println!("found the key {:?}", vector);
            } else {
                println!("Didn't found the key {}", cloned_key);
            }
            let duration = start.elapsed();
            println!("it took so time {:?}", duration);
        }
        let hash = calculate_hash(&ans[5]);
        let key: Key<i32> = Key::new(ans[5]);
        let delete = bucket.delete(key, hash as u8, false);
        match delete {
            Ok(_) => {
                println!("found the key to delete {:?}", ans);
                let mut vector = vec![];
                let key = Key::new(ans[5]);
                let start = Instant::now();
                // Calculate the elapsed time

                if bucket.check_and_get(meta_hash(hash), &key, false, &mut vector) {
                    println!("found the key {:?}", vector);
                } else {
                    println!("Didn't found the key {}", ans[5]);
                }
                let duration = start.elapsed();
                println!("it took so time {:?}", duration);
            }
            Err(err) => {
                println!("{:?}", err);
            }
        }
        let key: Key<i32> = Key::new(ans[5]);
        let delete = bucket.delete(key, meta_hash(hash), false);
        match delete {
            Ok(_) => {
                println!("found the key to delete {:?}", ans);
            }
            Err(err) => {
                println!("{:?}", err.to_string());
            }
        }
    }
    #[test]
    fn test_bucket_insertion_with_variable_keys() {
        let mut bucket: Bucket<String> = Bucket::new();
        let mut success = 0;
        let mut ans: Vec<String> = vec![];
        for i in 10000..10020 {
            let key = String::from(format!("let hash = calculate_hash(&key) {}", i));
            let mut string = String::from(format!("let hash = calculate_hash(&key) {}", i));
            let value: ValueT = string.clone().into_bytes();

            let hash = calculate_hash(&key);
            let key = Key::new(key);
            let response = bucket.insert(key, value, meta_hash(hash), true);
            match response {
                Ok(slot) => {
                    let key = String::from(format!("let hash = calculate_hash(&key) {}", i));
                    println!("The key {} is inserted at {}", key, slot);
                    success += 1;
                    ans.push(key);
                }
                Err(_) => {}
            }
        }
        ans.push(String::from(format!(
            "let hash = calculate_hash(&key) {}",
            500
        )));
        for key_str in ans {
            let mut vector = vec![];
            let cloned_key = key_str.clone();
            let hash = calculate_hash(&key_str);
            let key = Key::new(key_str);
            let start = Instant::now();
            // Calculate the elapsed time

            if bucket.check_and_get(meta_hash(hash), &key, false, &mut vector) {
                println!("found the key {:?}", vector);
            } else {
                println!("Didn't found the key {}", cloned_key);
            }
            let duration = start.elapsed();
            println!("it took so time {:?}", duration);
        }
    }
}
