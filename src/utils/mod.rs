// #![feature(portable_simd)]
// use std::simd::cmp::SimdPartialEq;
// use std::simd::Simd;
//
pub mod pair;
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