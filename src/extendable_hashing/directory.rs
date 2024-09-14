use crate::extendable_hashing::table::Table;
use std::fmt::Debug;

pub struct Directory<T: PartialEq + Debug + Clone> {
    pub x: Vec<Table<T>>, // Yet to be created
    pub global_depth: usize,
    pub version: usize,
    pub depth_count: usize,
}

impl<T: PartialEq + Debug + Clone> Directory<T> {
    fn new(&mut self, capacity: usize, version: usize) -> Self {
        Directory {
            x: vec![],
            global_depth: capacity.ilog2() as usize,
            version,
            depth_count: 0,
        }
    }
}
