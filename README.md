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

```
[dependencies]
bitcasky = "0.1.0"

```

Then, in your Rust code, import the `bitcasky` crate and start using the key-value store:

```
use bitcasky::Bitcasky;

fn main() {
    let mut db = Bitcasky::open("/path/to/db", BitcaskyOptions::default()).unwrap()

    db.put("key", "value").unwrap();
<<<<<<< Updated upstream
    let value = db.get("key").unwrap();
=======

    assert!(db.has("key").unwrap());

    let value = db.get("key").unwrap().unwrap();
>>>>>>> Stashed changes

    println!("{:?}", value);
}
```

### Store expirable value

```
db.put_with_ttl("key", "value", Duration::from_secs(60)).unwrap();

<<<<<<< Updated upstream
fn main() {
    let mut db = Bitcasky::open("/path/to/db", BitcaskyOptions::default()).unwrap()

    db.put_with_ttl("key", "value", Duration::from_secs(60)).unwrap();
    let value = db.get("key").unwrap();

    println!("{:?}", value);
}
=======
// 60 seconds later
assert!(db.get("key").unwrap().is_none());
>>>>>>> Stashed changes
```

### Delete some value or the entire database

```
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

```
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


