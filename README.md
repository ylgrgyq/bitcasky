# Bitcasky

Bitcasky is a Rust implementation of the Bitcask key-value store. It is an ACID-compliant, append-only key-value store that provides high write throughput. It is optimized for write-heavy workloads, and is commonly used for applications such as log storage and time-series data.

## Features

- Append-only storage for durability and consistency
- Memory-mapped files for efficient I/O
- Key-value storage with O(1) read and write performance
- Store expirable values

## Usage

### Basic usage

To use Bitcasky, simply add it to your `Cargo.toml` file:

```rust
[dependencies]
bitcasky = "0.1.0"
```

Then, in your Rust code, import the `bitcasky` crate and start using the key-value store:

```rust
use bitcasky::Bitcasky;

fn main() {
    let mut db = Bitcasky::open("/path/to/db", BitcaskyOptions::default()).unwrap()

    db.put("key", "value").unwrap();

    assert!(db.has("key").unwrap());

    let value = db.get("key").unwrap().unwrap();

    println!("{:?}", value);
}
```

### Store expirable value

```rust
db.put_with_ttl("key", "value", Duration::from_secs(60)).unwrap();

// 60 seconds later
assert!(db.get("key").unwrap().is_none());
```

### Delete some value or the entire database

```rust
db.put("key1", "value1").unwrap();
db.put("key2", "value2").unwrap();

// delete some value
db.delete("key1").unwrap();
assert!(db.get("key1").unwrap().is_none());

// drop database
db.drop().unwrap();
assert!(db.get("key2").unwrap().is_none());
```

### Iterate database

Iterate all keys.

```rust
// iterate and print all keys in database
bc.foreach_key(|k| println!("{}", String::from_utf8_lossy(k))).unwrap();

// fold all keys by concatenate them
let ret = bc.fold_key(
        |k, accumulator: Option<String>| match accumulator {
            // concatenate new key to folded key string
            Some(folded_k) => Ok(Some(folded_k + &String::from_utf8_lossy(k))),
            
            // if we have not fold anything, use this new key as folded key
            None => Ok(Some(String::from_utf8_lossy(k).into())),
        },
        // init accumulator
        None,
    )
    .unwrap();
assert!(ret.is_some());
println!("{}", ret.unwrap());
```

Iterate all keys and values.

```rust
// iterate and print all keys and values in database
bc.foreach(|k, v| {
    println!(
        "key: {}, value: {}",
        String::from_utf8_lossy(k),
        String::from_utf8_lossy(v)
    )
})
.unwrap();

// fold all keys and values by concatenate them
let ret = bc
    .fold(
        |k, v, accumulator: Option<String>| match accumulator {
            // concatenate new key and value to folded values
            Some(folded_vals) => Ok(Some(format!(
                "{} key: {}, val: {};",
                folded_vals,
                String::from_utf8_lossy(k),
                String::from_utf8_lossy(v)
            ))),

            // if we have not fold anything, use this new key and value as folded values
            None => Ok(Some(format!(
                "key: {}, val: {};",
                String::from_utf8_lossy(k),
                String::from_utf8_lossy(v)
            ))),
        },
        // init accumulator
        None,
    )
    .unwrap();
assert!(ret.is_some());
println!("{}", ret.unwrap());
```

### Sync strategy

By choosing a sync strategy, you can configure the durability of writes by specifying when to synchronize data to disk.

The following sync strategies are available:

* None — lets the operating system manage syncing writes
* OSync — uses the O_SYNC flag, which forces syncs on every write
* Time interval — sync at specified intervals (default: 60 secs)

For example, create a Bitcasky database which sync on every 35 secs as follows:

```rust
let db = Bitcasky::open(
        "/path/to/db", 
        BitcaskyOptions::default().sync_strategy(SyncStrategy::Interval(Duration::from_secs(35)))
    ).unwrap();
```

### Maintainance

Bitcasky need to call merge periodically to 

The merge process traverses data files and reclaims space by eliminating out-of-date of deleted key/value pairs, writing only the current key/value pairs to a new set of files within the directory.

```rust

```

### Telemetry data