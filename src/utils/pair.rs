use crate::hash::ValueT;
#[derive(Debug)]
pub struct Key<T>{
    pub key: T,
    pub is_pointer: bool,
    pub length: u32,
    pub pointed_key: Vec<u8>
}
impl<T> Key<T> {
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
pub struct Pair<'a, T> {
    pub key: Key<T>,
    pub value: ValueT<'a>,
}

impl<'a, T> Pair<'a ,T> {
    pub fn new(key: T, value: ValueT<'a>) -> Self {
        let key = Key::new(key);
        Pair { key, value}
    }
}
