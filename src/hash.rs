pub type ValueT = Vec<u8>;
pub trait Hash<T> {
    fn new();
    fn insert(key: T, value: [u8]);
    fn delete(key: T);
    fn get(key: T, buff: &mut [u8]);
}
