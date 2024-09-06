use std::error::Error;
use std::fmt::{write, Debug, Display, Formatter};
use crate::hash::ValueT;
use crate::utils::pair::{Key, Pair};
use std::ops::BitAnd;
use thiserror::Error;
use crate::utils::var_compare;
// use crate::utils::sse_cmp8;

const K_NUM_PAIR_PER_BUCKET: u32 = 14;
const COUNT_MASK: u32 = (1 << 4) - 1;
const OVERFLOW_BITMAP_MASK: u8 = (1 << 4) - 1;
const OVERFLOW_SET: u8 = 1 << 4;
const STASH_BUCKET: u8 = 2;
const STASH_MASK: usize = (1 << STASH_BUCKET.ilog2()) - 1;
const ALLOC_MASK: usize = 1 << 4 - 1;
#[derive(Debug, Clone)]

pub struct Bucket<T: PartialEq> {
    pub pairs: Vec<Option<Pair<T>>>,
    pub unused: [u8; 2],
    overflow_count: u8,
    overflow_member: u8,
    overflow_index: u8,
    overflow_bitmap: u8,
    finger_array: [u8; 18], /*only use the first 14 bytes, can be accelerated by SSE instruction,0-13 for finger, 14-17 for overflowed*/
    bitmap: u32,            // allocation bitmap + pointer bitmap + counter
    version_lock: u32,
}
/**
for Bitmap: 32 bits
0000 0000 1110 00 10 0000 0000 0000 0101
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
            version_lock: 0,
        }
    }
    pub fn find_empty_slot(&self) -> i32 {
        if get_count(self.bitmap) == K_NUM_PAIR_PER_BUCKET {
            return -1;
        }
        let mask = !get_bitmap(self.bitmap);
        println!("{}", mask.trailing_zeros());
        mask.trailing_zeros() as i32
    }

    pub fn test_overflow(&self) -> bool {
        self.overflow_count > 0
    }

    pub fn test_stack_check(&self) -> bool {
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
        let mut mask: u8 = self.overflow_bitmap.bitand(OVERFLOW_BITMAP_MASK);
        mask = !mask;
        let mut index: u8 = mask.trailing_zeros() as u8;

        if index < 4 {
            // Means a slot is free in the probing bucket
            self.finger_array[(14 + index) as usize] = meta_hash;
            self.overflow_bitmap = (1 << index) | self.overflow_bitmap;
            self.overflow_index =
                self.overflow_index.bitand(!(3 << (index * 2))) | (pos << (index * 2));
        } else {
            // Looking for the free slot in neighboring bucket
            mask = neighbor.overflow_bitmap.bitand(OVERFLOW_BITMAP_MASK);
            mask = !mask;
            index = mask.trailing_zeros() as u8;
            if index < 4 {
                neighbor.finger_array[(14 + index) as usize] = meta_hash;
                neighbor.overflow_bitmap = (1 << index) | neighbor.overflow_bitmap;
                neighbor.overflow_index =
                    neighbor.overflow_index.bitand(!(3 << (index * 2))) | (pos << (index * 2));
                // Overflow member is only used to track that if there are some overflowed members in neighboring buckets
                neighbor.overflow_member = (1 << index) | neighbor.overflow_member;
            } else {
                self.overflow_count += 1;
            }
        }
    }

    pub fn unset_indicator(&mut self, meta_hash: u8, neighbor: &mut Bucket<T>, key: Key<T>, pos: u64) {
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
            // Meaning the hash is being inserted the 14 slots not the stash slots
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
    pub fn insert(&mut self, key: Key<T>, value: ValueT, meta_hash: u8, probe: bool) -> Result<i32, BucketError> {
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
    pub fn check_and_get(&self, meta_hash: u8, key: Key<T>, probe: bool, value: &mut ValueT) -> bool {
        let mut mask: u32 = 0;
        // TODO: We can replace this loop with SIMD instruction
        for(i, &finger) in self.finger_array.iter().enumerate() {
            if finger == meta_hash{
                // Setting the corresponding bit which matched with the hash;
                mask |= 1 << i;
            }
        }
        if probe {
            // Meaning We are looking the key in the probing bucket
            mask = (mask & get_bitmap(self.bitmap) & get_member(self.bitmap));
        }else{
            mask = (mask & get_bitmap(self.bitmap) & !get_member(self.bitmap));
        }

        if mask == 0{
            // No match found
            return false;
        }
        if key.is_pointer {
            // Variable length key
            for i in 0.. 14{
                if check_bit_32(mask, i as u32){
                    let ex_key = &self.pairs[i].clone().unwrap().key;
                    if var_compare(&key.pointed_key, key.length, &ex_key.pointed_key, ex_key.length){
                        *value = self.pairs[i].clone().unwrap().value;
                        return true;
                    }
                }

            }
        }else{
            // Fixed length keys
            for i in (0..14).step_by(4){
                let iu = i as usize;
                if check_bit_32(mask, i) &&
                    self.pairs[iu].clone().unwrap().key.key == key.key{
                    *value = self.pairs[iu].clone().unwrap().value;
                    return true;
                }
                if check_bit_32(mask, i + 1) &&
                    self.pairs[iu + 1].clone().unwrap().key.key == key.key{
                    *value = self.pairs[iu + 1].clone().unwrap().value;
                    return true;
                }
                if check_bit_32(mask, i + 2) &&
                    self.pairs[iu + 2].clone().unwrap().key.key == key.key{
                    *value = self.pairs[iu + 2].clone().unwrap().value;
                    return true;
                }
                if check_bit_32(mask, i + 3) &&
                    self.pairs[iu + 3].clone().unwrap().key.key == key.key{
                    *value = self.pairs[iu + 3].clone().unwrap().value;
                    return true;
                }
            }
            if check_bit_32(mask, 12) &&
                self.pairs[12].clone().unwrap().key.key == key.key{
                *value = self.pairs[12].clone().unwrap().value;
                return true;
            }
            if check_bit_32(mask, 13) &&
                self.pairs[13].clone().unwrap().key.key == key.key{
                *value = self.pairs[13].clone().unwrap().value;
                return true;
            }
        }
        false
    }
    pub fn insert_displace(&mut self, key: Key<T>, value: ValueT, meta_hash: u8, slot: i32, probe: bool){
        self.pairs[slot as usize] = Some(Pair::new(key, value));
        self.set_hash(slot, meta_hash, probe);
    }
    pub fn delete_with_key_pointer(&mut self, key: *mut u8,meta_hash: u8, probe: bool) -> i32{
        unsafe {
            println!("Pointer is called {:?}", key);
            // self.delete(*key,meta_hash, probe)
            // *key = "hello ball".parse().unwrap();
            10
        }
    }
    pub fn delete(&mut self, key: Key<T>, meta_hash: u8, probe: bool) -> Result<(), BucketError> {
        /*do the simd and check the key, then do the delete operation*/
        let mut mask: u32 = 0;
        // sse_cmp8(&self.finger_array, meta_hash);
        for(i, &finger) in self.finger_array.iter().enumerate() {
            if finger == meta_hash{
                // Setting the corresponding bit which matched with the hash;
                mask |= 1 << i;
            }
        }
        if probe {
            mask = (mask & get_bitmap(self.bitmap) & get_member(self.bitmap));
        }else{
            mask = (mask & get_bitmap(self.bitmap) & !get_member(self.bitmap));
        }
        if key.is_pointer{
            if mask != 0{
                for i in (0..14).step_by(4){
                    let iu = i as usize;
                    if check_bit_32(mask, i) &&
                        self.pairs[iu].clone().unwrap().key.pointed_key == key.pointed_key{
                        self.unset_hash(i);
                        self.pairs[iu] = None;
                        return Ok(());
                    }
                    if check_bit_32(mask, i + 1) &&
                        self.pairs[iu + 1].clone().unwrap().key.pointed_key == key.pointed_key{
                        self.unset_hash(i);
                        self.pairs[iu] = None;
                        return Ok(());
                    }
                    if check_bit_32(mask, i + 2) &&
                        self.pairs[iu + 2].clone().unwrap().key.pointed_key == key.pointed_key{
                        self.unset_hash(i);
                        self.pairs[iu] = None;
                        return Ok(());
                    }
                    if check_bit_32(mask, i + 3) &&
                        self.pairs[iu + 3].clone().unwrap().key.pointed_key == key.pointed_key{
                        self.unset_hash(i);
                        self.pairs[iu] = None;
                        return Ok(());
                    }
                }
                if check_bit_32(mask, 12) &&
                    self.pairs[12].clone().unwrap().key.pointed_key == key.pointed_key{
                    self.unset_hash(12);
                    self.pairs[12] = None;
                    return Ok(());
                }
                if check_bit_32(mask, 13) &&
                    self.pairs[13].clone().unwrap().key.pointed_key == key.pointed_key{
                    self.unset_hash(13);
                    self.pairs[13] = None;
                    return Ok(());
                }
            }
        }
        else{
            for i in (0..12).step_by(4){
                let iu = i as usize;
                if check_bit_32(mask, i) &&
                    self.pairs[iu].clone().unwrap().key.key == key.key{
                    self.unset_hash(i);
                    self.pairs[iu] = None;
                    return Ok(());
                }
                if check_bit_32(mask, i + 1) &&
                    self.pairs[iu + 1].clone().unwrap().key.key == key.key{
                    self.unset_hash(i + 1);
                    self.pairs[iu + 1] = None;
                    return Ok(());
                }
                if check_bit_32(mask, i + 2) &&
                    self.pairs[iu + 2].clone().unwrap().key.key == key.key{
                    self.unset_hash(i + 1);
                    self.pairs[iu + 1] = None;
                    return Ok(());
                }
                if check_bit_32(mask, i + 3) &&
                    self.pairs[iu + 3].clone().unwrap().key.key == key.key{
                    self.unset_hash(i + 1);
                    self.pairs[iu + 1] = None;
                    return Ok(());
                }
            }
            if check_bit_32(mask, 12) &&
                self.pairs[12].clone().unwrap().key.key == key.key{
                self.unset_hash(12);
                self.pairs[12] = None;
                return Ok(());
            }
            if check_bit_32(mask, 13) &&
                self.pairs[13].clone().unwrap().key.key == key.key{
                self.unset_hash(13);
                self.pairs[13] = None;
                return Ok(());
            }
        }
        Err(BucketError::ItemDoesntExist)
    }
    pub fn reset_lock(&mut self){
        self.version_lock = 0;
    }

    pub fn reset_overflow_fp(&mut self){
        self.overflow_bitmap = 0;
        self.overflow_index = 0;
        self.overflow_member = 0;
        self.overflow_count = 0;
        self.clear_stash_check()
    }

}
#[derive(Debug, Error)]
pub enum BucketError{
    #[error("The bucket is full")]
    BucketFull,
    #[error("Internal Error")]
    Internal,
    #[error("Item does not exist")]
    ItemDoesntExist,
}

// impl Display for BucketError {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         match self {
//             BucketError::BucketFull => write!(
//                 f,
//                 "The bucket is full",
//             ),
//             BucketError::Internal => write!(
//                 f,
//                 "Internal Error"
//             )
//         }
//     }
// }

/**
0000 0000 1110 0010 0000 0000 0000 0101 & 0000 0000 0000 0000 0000 0000 0000 1111
it returns 5 as the count
*/
fn get_count(var: u32) -> u32 {
    var.bitand(COUNT_MASK)
}

/**
We remove last 4 bits which are for count
and 14 bits before that which are for pointers
*/
fn get_bitmap(var: u32) -> u32 {
    var >> 18
}
fn get_member(var: u32) -> u32{
    var & ALLOC_MASK as u32
}

fn check_bit(var: u8, pos: u32) -> bool {
    var & (1 << pos) > 0
}

fn check_bit_32(var: u32, pos: u32) -> bool {
    var & (1 << pos) > 0
}
