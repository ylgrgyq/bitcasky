# bitcask

# **Operational Transformations as an algorithm for automatic conflict resolution**

# List of Papers about Operational Transformation

- Operational Transformation in Real-Time Group Editors: Issues, Algorithms, and Achievements
- Concurrency Control in Groupware Systems
- A Survey of Collaborative Editing Systems
- Transformational adaptation to support collaborative writing
- Integrating Synchronous and Asynchronous Collaboration with Bifocal Users and Groupware
- A generic operational transformation framework for real-time group editors
- Probabilistic Operational Transformation for Real-Time Collaborative Editing
- A review of conflict detection and resolution approaches in collaborative editing
- A taxonomy of operational transformation
- A Transformation-Based Optimistic Concurrency Control Method for Collaborative Editing Systems
- Operational Transformation in Distributed Systems
- A Formal Model for Collaborative Editing
- A Commutative Replicated Data Type for Cooperative Editing
- Operational Transformation in Object-Oriented Collaborative Systems

# Bitcask-rs

Bitcask-rs is a Rust implementation of the Bitcask key-value store. Bitcask is an ACID-compliant, append-only key-value store that provides high write throughput. It is optimized for write-heavy workloads, and is commonly used for applications such as log storage and time-series data.

## Features

- Append-only storage for durability and consistency
- Memory-mapped files for efficient I/O
- Key-value storage with O(1) read and write performance
- Automatic compaction to reduce disk usage and improve read performance

## Usage

To use Bitcask-rs, simply add it to your `Cargo.toml` file:

```
[dependencies]
bitcask-rs = "0.1.0"

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
