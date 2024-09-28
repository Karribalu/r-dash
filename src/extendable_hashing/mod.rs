pub mod bucket;
mod directory;
mod table;

use crate::extendable_hashing::directory::Directory;
use crate::hash::Hash;
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
// pub struct ExtendableHashing<T> {
//     clean: bool,
//     crash_version: u64,
//     lock: AtomicI32, // the MSB is the lock bit; remaining bits are used as the counter
//     // dir: Directory // Yet to be implemented
//
// }
// impl<T> Hash<T> for ExtendableHashing<T> {
//     fn new() {
//         todo!()
//     }
//
//     fn insert(key: T, value: Box<[u8]>) {
//         todo!()
//     }
//
//     fn delete(key: T) {
//         todo!()
//     }
//
//     fn get(key: T, buff: &mut [u8]) {
//         todo!()
//     }
// }
// impl<T> ExtendableHashing<T>{
//     fn shut_down(&mut self){
//         self.clean = true;
//         // Persist after that
//     }
// }
