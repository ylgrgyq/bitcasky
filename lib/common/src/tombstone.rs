pub const TOMBSTONE_VALUE: &str = "bitcask_tombstone";

pub fn is_tombstone(val: &[u8]) -> bool {
    val.eq(TOMBSTONE_VALUE.as_bytes())
}
