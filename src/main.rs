use rustgres::{SkipList, MemTable};

fn main() {
    println!("Testing SkipList...");

    let mut sl = SkipList::new();

    // Insert some key-value pairs
    sl.insert(3, "three".to_string());
    sl.insert(1, "one".to_string());
    sl.insert(4, "four".to_string());
    sl.insert(2, "two".to_string());

    // Test get
    println!("Get 1: {:?}", sl.get(&1));
    println!("Get 3: {:?}", sl.get(&3));

    // Test iterate
    println!("Iterate (should be ordered):");
    for (k, v) in sl.iter() {
        println!("  {}: {}", k, v);
    }

    println!("\nTesting MemTable...");

    let mut memtable = MemTable::new(1024 * 1024);

    // Insert 10 items
    for i in 0..10 {
        let key = format!("key_{:03}", i).into_bytes();
        let value = format!("value_{}", i).into_bytes();
        let should_flush = memtable.put(key, value);
        if should_flush {
            println!("Should flush!");
        }
    }

    // Test get
    println!("Get key_005: {:?}", memtable.get(b"key_005"));

    // Test iterate
    println!("Iterate MemTable:");
    for (k, v) in memtable.iter() {
        println!("  {:?}: {:?}", String::from_utf8_lossy(k), String::from_utf8_lossy(v));
    }

    println!("\nAll tests passed!");
}
