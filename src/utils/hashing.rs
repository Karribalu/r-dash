use std::hash::{DefaultHasher, Hash, Hasher};
pub fn calculate_hash<T: Hash>(t: &T) -> usize {
    let mut hasher = DefaultHasher::new(); // Create a new DefaultHasher
    t.hash(&mut hasher); // Hash the value
    hasher.finish() as usize // Get the resulting hash
}
