// #![feature(portable_simd)]
// use std::simd::cmp::SimdPartialEq;
// use std::simd::Simd;
//
pub mod pair;
pub mod hashing;
//
// pub fn sse_cmp8(src: &[u8; 18], key: u8) -> i32 {
//     // Load the key into all elements of a __m128i vector
//     let key_data = Simd::splat(key);
//
//     // Load the data from the src pointer into a __m128i vector
//     let seg_data = Simd::from_slice(src);
//
//     // Compare the elements of seg_data and key_data
//     let cmp_result = seg_data.simd_eq(key_data);
//
//     // Convert the cmp_result to an integer mask
//     cmp_result.to_bitmask() as i32
// }


pub fn var_compare(key_1: &Vec<u8>, len1: u32, key_2: &Vec<u8>, len2: u32) -> bool{
    if len1 != len2{
        return false;
    }
    let matching = key_1.iter().zip(key_2.iter()).filter(|&(a, b)| a == b).count();
    matching == key_1.len() && matching == key_1.len()
}