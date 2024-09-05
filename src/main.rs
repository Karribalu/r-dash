use crate::extendable_hashing::bucket::Bucket;
use crate::hash::ValueT;
use crate::utils::hashing::calculate_hash;
use crate::utils::pair::Pair;

mod extendable_hashing;
mod hash;
mod utils;

fn main() {
    // println!("Hello, world!");
    // let mut bucket: Bucket<String> = Bucket::new();
    // println!("hey {:?}", bucket.find_empty_slot());
    // let mut a = String::from("hello");
    // bucket.delete(a.clone(), 100, true);
    // bucket.delete_with_key_pointer(a.as_mut_ptr(), 200, true);
    // unsafe { println!("{:#?}", a.as_ptr()); }
    // let address: &i64 = &0x600003730000;
    // println!("{}", *address);
    // let mut string = String::from("some_value");
    // let value: ValueT = unsafe { string.as_bytes_mut() };
    // let pair: Pair<f32> = Pair::new(77.0, value);
    // println!("Pair is {:?}", pair);

    let mut bucket: Bucket<String> = Bucket::new();
    let mut success = 0;
    let mut ans: Vec<String> = vec![];
    for i in 10000..100020{
        let key = String::from(format!("let hash = calculate_hash(&key) {}", i));
        let mut string = String::from(format!("let hash = calculate_hash(&key) {}", i));
        let value: ValueT = string.clone().into_bytes();

        let hash = calculate_hash(&key);
        let response = bucket.insert(key, value, hash, true);
        if response == 0 {
            success += 1;
            ans.push(String::from(format!("let hash = calculate_hash(&key) {}", i)));
        }

    }
    println!("Successfully insertions are {} {:?}", success, ans);

}
