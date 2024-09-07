use crate::hash::ValueT;
#[derive(Debug, Clone, PartialEq)]
pub struct Key<T: PartialEq> {
    pub key: T,
    pub is_pointer: bool,
    pub length: u32,
    pub pointed_key: Vec<u8>,
}
impl<T: PartialEq> Key<T> {
    pub fn new(key: T) -> Self {
        Key {
            key,
            is_pointer: false,
            length: 0,
            pointed_key: vec![],
        }
    }
}
#[derive(Debug)]
pub struct Pair<T: PartialEq> {
    pub key: Key<T>,
    pub value: ValueT,
}
impl<T: Clone + PartialEq> Clone for Pair<T> {
    fn clone(&self) -> Self {
        // Manually handle the data if necessary; for example, create a new buffer
        let new_buffer = self.value.to_vec(); // or other logic
        Pair {
            key: self.key.clone(),
            value: new_buffer,
        }
    }
}

impl<T: PartialEq> Pair<T> {
    pub fn new(key: Key<T>, value: ValueT) -> Self {
        Pair { key, value }
    }
}
