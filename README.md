# bitcask

# Bitcask-rs

Bitcask-rs is a Rust implementation of the Bitcask key-value store. Bitcask is an ACID-compliant, append-only key-value store that provides high write throughput. It is optimized for write-heavy workloads, and is commonly used for applications such as log storage and time-series data.

## Features

- Append-only storage for durability and consistency
- Memory-mapped files for efficient I/O
- Key-value storage with O(1) read and write performance

## Usage

To use Bitcask, simply add it to your `Cargo.toml` file:

```
[dependencies]
bitcask = "0.1.0"

```

Then, in your Rust code, import the `bitcask` crate and start using the key-value store:

```
use bitcask::Bitcask;

fn main() {
    let mut db = Bitcask::open("/path/to/db").unwrap();

    db.put(b"key", b"value").unwrap();
    let value = db.get(b"key").unwrap();

    println!("{:?}", value);
}

```

For more information on how to use Bitcask-rs, please see the [documentation](https://docs.rs/bitcask-rs/0.1.0/bitcask/).
