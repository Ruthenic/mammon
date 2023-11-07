# mammon
simple, bad, blob storage api

## examples
```rust
pub fn main() {
    let store = Store::new("store_dir");

    store.store("foo", vec![1, 2, 3]).unwrap();

    println!("{:?}", store.retrieve("foo").unwrap()); // vec![1, 2, 3]
}
```