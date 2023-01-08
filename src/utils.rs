pub const TOMBSTONE_VALUE: &str = "bitcask_tombstone";

pub fn is_tombstone(val: &String) -> bool {
    val.eq(TOMBSTONE_VALUE)
}
