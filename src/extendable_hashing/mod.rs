pub mod bucket;
mod directory;
mod table;

use crate::extendable_hashing::directory::Directory;
use crate::hash::Hash;
use std::sync::atomic::AtomicI32;
pub const K_NUM_BUCKET: usize = 64;
pub const K_STASH_BUCKET: usize = 4;

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
