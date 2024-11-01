use crate::extendable_hashing::table::Table;
use std::collections::HashMap;
use std::fmt::Debug;

pub struct Directory<T: PartialEq + Debug + Clone> {
    pub segments: Vec<Table<T>>, // Yet to be created
    pub global_depth: usize,
    pub version: usize,
    pub depth_count: usize,
}

impl<T: PartialEq + Debug + Clone> Directory<T> {
    pub fn new(capacity: usize, version: usize) -> Self {
        let mut segments = Vec::with_capacity(capacity);
        for i in 0..capacity {
            segments.push(Table::new(i));
        }
        Directory {
            segments,
            global_depth: capacity.ilog2() as usize,
            version,
            depth_count: capacity,
        }
    }
}
