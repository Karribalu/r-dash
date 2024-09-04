pub struct Directory {
    // x: Box::new: (vec![Table<T>]), // Yet to be created
    global_depth: usize,
    version: usize,
    depth_count: usize,
}
//
// impl<T> Directory<T>{
//     fn new(&mut self, capacity: usize, version: usize) -> Self {
//         Directory{
//             // table: ,
//             global_depth: capacity.ilog2() as usize,
//             version,
//             depth_count: 0,
//         }
//     }
// }
