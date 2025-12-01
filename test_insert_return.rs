use rustgres::SkipList;

fn main() {
    let mut sl = SkipList::new();
    sl.insert(1, "one".to_string());
    
    // This line should cause a borrow checker error
    let val = sl.insert(1, "ONE".to_string());
    
    // Because we're trying to use the returned reference while the mutable borrow from insert is active
    println!("Insert returned: {:?}", val);
}
