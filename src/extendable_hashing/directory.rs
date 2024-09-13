use crate::extendable_hashing::table::Table;

pub struct Directory<T> {
    pub x: Vec<Table<T>>, // Yet to be created
    pub global_depth: usize,
    pub version: usize,
    pub depth_count: usize,
}

impl<T> Directory<T> {
    fn new(&mut self, capacity: usize, version: usize) -> Self {
        Directory {
            x: vec![],
            global_depth: capacity.ilog2() as usize,
            version,
            depth_count: 0,
        }
    }
}
