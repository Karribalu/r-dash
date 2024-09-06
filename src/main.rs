#![feature(let_chains)]

use std::time::Instant;
use crate::extendable_hashing::bucket::{Bucket, BucketError};
use crate::hash::ValueT;
use crate::utils::hashing::calculate_hash;
use crate::utils::pair::{Key, Pair};

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


    // println!("Successfully insertions are {} {:?}", success, ans);
    fixed_keys();
}
fn variable_keys(){
    let mut bucket: Bucket<String> = Bucket::new();
    let mut success = 0;
    let mut ans: Vec<String> = vec![];
    for i in 10000..10020{
        let key = String::from(format!("let hash = calculate_hash(&key) {}", i));
        let mut string = String::from(format!("let hash = calculate_hash(&key) {}", i));
        let value: ValueT = string.clone().into_bytes();

        let hash = calculate_hash(&key);
        let key = Key::new(key);
        let response = bucket.insert(key, value, hash, true);
        match response{
            Ok(slot) => {
                let key = String::from(format!("let hash = calculate_hash(&key) {}", i));
                println!("The key {} is inserted at {}", key, slot);
                success += 1;
                ans.push(key);
            }
            Err(_) => {

            }
        }
    }
    ans.push(String::from(format!("let hash = calculate_hash(&key) {}", 500)));
    for key_str in ans{
        let mut vector = vec![];
        let cloned_key = key_str.clone();
        let hash = calculate_hash(&key_str);
        let key = Key::new(key_str);
        let start = Instant::now();
        // Calculate the elapsed time

        if bucket.check_and_get(hash, key, false, &mut vector){
            println!("found the key {:?}", vector);
        }else{
            println!("Didn't found the key {}", cloned_key);
        }
        let duration = start.elapsed();
        println!("it took so time {:?}", duration);
    }
}
fn fixed_keys(){
    let mut bucket: Bucket<i32> = Bucket::new();
    let mut success = 0;
    let mut ans = vec![];
    for i in 10000..10020{
        let mut string = String::from(format!("let hash = calculate_hash(&key) {}", i));
        let value: ValueT = string.clone().into_bytes();

        let hash = calculate_hash(&i);
        let key = Key::new(i);
        let response = bucket.insert(key, value, hash, true);
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
    for key_str in &ans{
        let mut vector = vec![];
        let cloned_key = key_str.clone();
        let hash = calculate_hash(key_str);
        let key = Key::new(*key_str);
        let start = Instant::now();
        // Calculate the elapsed time

        if bucket.check_and_get(hash, key, false, &mut vector){
            println!("found the key {:?}", vector);
        }else{
            println!("Didn't found the key {}", cloned_key);
        }
        let duration = start.elapsed();
        println!("it took so time {:?}", duration);
    }
    let hash = calculate_hash(&ans[5]);
    let key: Key<i32> = Key::new(ans[5]);
    println!("{:?}", bucket);
    let delete = bucket.delete(key, hash, false);
    match delete {
        Ok(_) => {
            println!("found the key to delete {:?}", ans);
            let mut vector = vec![];
            let key =  Key::new(ans[5]);
            let start = Instant::now();
            // Calculate the elapsed time

            if bucket.check_and_get(hash, key, false, &mut vector){
                println!("found the key {:?}", vector);
            }else{
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
    let delete = bucket.delete(key, hash, false);
    match delete {
        Ok(_) => {
            println!("found the key to delete {:?}", ans);
        }
        Err(err) => {
            println!("{:?}", err.to_string());
        }
    }
}
