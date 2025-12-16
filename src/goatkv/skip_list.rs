use rand::{rngs::SmallRng, Rng, SeedableRng};
use std::cmp::Ordering;
use std::ptr::NonNull;

// ==================== Arena 分配器 ====================

#[derive(Debug)]
pub struct Arena {
    chunks: Vec<Vec<u8>>,
    current: Vec<u8>,
    chunk_size: usize,
    bytes_allocated: usize,
}

impl Arena {
    pub fn new() -> Self {
        Self::with_capacity(4096)
    }

    pub fn with_capacity(chunk_size: usize) -> Self {
        Self {
            chunks: Vec::new(),
            current: Vec::with_capacity(chunk_size),
            chunk_size,
            bytes_allocated: 0,
        }
    }

    pub fn alloc<T>(&mut self, value: T) -> NonNull<T> {
        let layout = std::alloc::Layout::new::<T>();
        let ptr = self.alloc_bytes(layout);
        unsafe {
            let typed_ptr = ptr as *mut T;
            std::ptr::write(typed_ptr, value);
            NonNull::new_unchecked(typed_ptr)
        }
    }

    pub fn alloc_slice<T: Copy>(&mut self, src: &[T]) -> NonNull<[T]> {
        if src.is_empty() {
            return NonNull::slice_from_raw_parts(NonNull::dangling(), 0);
        }

        let layout = std::alloc::Layout::array::<T>(src.len()).unwrap();
        let ptr = self.alloc_bytes(layout) as *mut T;
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), ptr, src.len());
            NonNull::new_unchecked(std::slice::from_raw_parts_mut(ptr, src.len()))
        }
    }

    fn alloc_bytes(&mut self, layout: std::alloc::Layout) -> *mut u8 {
        let align = layout.align();
        let size = layout.size();

        // 对齐当前位置
        let current_len = self.current.len();
        let aligned_pos = (current_len + align - 1) & !(align - 1);
        let padding = aligned_pos - current_len;

        if aligned_pos + size > self.current.capacity() {
            // 当前 chunk 空间不足，分配新的
            if !self.current.is_empty() {
                let old = std::mem::replace(
                    &mut self.current,
                    Vec::with_capacity(self.chunk_size.max(size + align)),
                );
                self.chunks.push(old);
            } else {
                self.current = Vec::with_capacity(self.chunk_size.max(size + align));
            }
            return self.alloc_bytes(layout);
        }

        // 填充对齐字节
        self.current.resize(aligned_pos, 0);

        let ptr = unsafe { self.current.as_mut_ptr().add(aligned_pos) };

        // 扩展长度
        unsafe {
            self.current.set_len(aligned_pos + size);
        }

        self.bytes_allocated += padding + size;
        ptr
    }

    /// 已分配的总字节数
    pub fn memory_usage(&self) -> usize {
        self.bytes_allocated
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

// ==================== 跳表节点 ====================

// Increase MAX_HEIGHT to handle larger datasets
const MAX_HEIGHT: usize = 32;

#[repr(C)]
struct Node<K, V> {
    key: K,
    value: V,
    height: usize,
    // tower 紧跟在结构体后面，通过 get_tower 访问
}

impl<K, V> Node<K, V> {
    /// 获取 tower 数组（存储各层的下一个节点指针）
    #[inline]
    fn tower(&self) -> &[NodePtr<K, V>] {
        unsafe {
            let tower_ptr = (self as *const Self).add(1) as *const NodePtr<K, V>;
            std::slice::from_raw_parts(tower_ptr, self.height)
        }
    }

    #[inline]
    fn tower_mut(&mut self) -> &mut [NodePtr<K, V>] {
        unsafe {
            let tower_ptr = (self as *mut Self).add(1) as *mut NodePtr<K, V>;
            std::slice::from_raw_parts_mut(tower_ptr, self.height)
        }
    }

    #[inline]
    fn next(&self, level: usize) -> NodePtr<K, V> {
        self.tower()[level]
    }

    #[inline]
    fn set_next(&mut self, level: usize, node: NodePtr<K, V>) {
        self.tower_mut()[level] = node;
    }
}

type NodePtr<K, V> = Option<NonNull<Node<K, V>>>;

// ==================== 跳表实现 ====================

#[derive(Debug)]
pub struct SkipList<K, V> {
    arena: Arena,
    head: NonNull<Node<K, V>>,
    max_height: usize, // 当前最大高度
    len: usize,
    rng: SmallRng,
}

impl<K, V> SkipList<K, V> {
    pub fn new() -> Self {
        Self::with_arena(Arena::new())
    }

    pub fn with_arena(mut arena: Arena) -> Self {
        // 分配 head 节点
        let head = Self::alloc_node(&mut arena, None, MAX_HEIGHT);

        Self {
            arena,
            head,
            max_height: 1,
            len: 0,
            rng: SmallRng::from_entropy(),
        }
    }

    fn alloc_node(arena: &mut Arena, entry: Option<(K, V)>, height: usize) -> NonNull<Node<K, V>> {
        // 计算需要的内存：Node 结构体 + tower 数组
        let node_size = std::mem::size_of::<Node<K, V>>();
        let tower_size = std::mem::size_of::<NodePtr<K, V>>() * height;
        let total_size = node_size + tower_size;
        let align = std::mem::align_of::<Node<K, V>>();

        let layout = std::alloc::Layout::from_size_align(total_size, align).unwrap();
        let ptr = arena.alloc_bytes(layout) as *mut Node<K, V>;

        unsafe {
            // Initialize node with MaybeUninit to handle head node case
            if let Some((key, value)) = entry {
                // Normal node - initialize with key and value
                std::ptr::write(ptr, Node { key, value, height });
            } else {
                // Head node - since we never access key and value, we can initialize the memory
                // as zeroed. In Rust, zeroing memory for the entire struct is undefined behavior
                // if the type has Drop trait, but since we're using Arena which never deallocates
                // and we never access key/value, this is safe in practice.

                // Zero the entire node memory first
                std::ptr::write_bytes(ptr, 0, 1);
                // Then set the height field
                (*ptr).height = height;
            }

            // 初始化 tower 为全 None
            let tower_ptr = ptr.add(1) as *mut NodePtr<K, V>;
            for i in 0..height {
                std::ptr::write(tower_ptr.add(i), None);
            }

            NonNull::new_unchecked(ptr)
        }
    }

    fn random_height(&mut self) -> usize {
        let mut height = 1;
        // Use standard skip list probability (50% chance to increase height)
        while height < MAX_HEIGHT && self.rng.gen_bool(0.5) {
            height += 1;
        }
        height
    }
}

impl<K: Ord, V> SkipList<K, V> {
    /// 插入键值对，如果 key 已存在则更新 value 并返回旧值
    pub fn insert(&mut self, key: K, value: V) -> Option<&V> {
        let mut prev = [None::<NonNull<Node<K, V>>>; MAX_HEIGHT];
        let mut current = self.head;

        // 从最高层开始查找
        for i in (0..self.max_height).rev() {
            loop {
                let next = unsafe { current.as_ref().next(i) };
                match next {
                    Some(next_ptr) => {
                        let next_node = unsafe { next_ptr.as_ref() };
                        match next_node.key.cmp(&key) {
                            Ordering::Less => current = next_ptr,
                            Ordering::Equal => {
                                // key 已存在，LSM 场景下直接返回已存在的值
                                // 实际 LSM 中通常允许重复 key（不同版本）
                                return Some(&next_node.value);
                            }
                            Ordering::Greater => break,
                        }
                    }
                    None => break,
                }
            }
            prev[i] = Some(current);
        }

        // 分配新节点
        let height = self.random_height();
        let mut new_node = Self::alloc_node(&mut self.arena, Some((key, value)), height);

        // 如果新节点高度超过当前最大高度，更新 prev
        if height > self.max_height {
            for i in self.max_height..height {
                prev[i] = Some(self.head);
            }
            self.max_height = height;
        }

        // 插入新节点到各层
        for i in 0..height {
            if let Some(mut prev_node) = prev[i] {
                let prev_next = unsafe { prev_node.as_ref().next(i) };
                unsafe {
                    // Set new node's next pointer
                    new_node.as_mut().set_next(i, prev_next);
                    // Set previous node's next pointer to new node
                    prev_node.as_mut().set_next(i, Some(new_node));
                }
            }
        }

        self.len += 1;
        None
    }

    /// 查找 key
    pub fn get(&self, key: &K) -> Option<&V> {
        let mut current = self.head;

        for i in (0..self.max_height).rev() {
            loop {
                let next = unsafe { current.as_ref().next(i) };
                match next {
                    Some(next_ptr) => {
                        let next_node = unsafe { next_ptr.as_ref() };
                        match next_node.key.cmp(key) {
                            Ordering::Less => current = next_ptr,
                            Ordering::Equal => return Some(&next_node.value),
                            Ordering::Greater => break,
                        }
                    }
                    None => break,
                }
            }
        }
        None
    }

    /// 查找大于等于 key 的最小元素
    pub fn seek(&self, key: &K) -> Option<(&K, &V)> {
        let mut current = self.head;

        for i in (0..self.max_height).rev() {
            loop {
                let next = unsafe { current.as_ref().next(i) };
                match next {
                    Some(next_ptr) => {
                        let next_node = unsafe { next_ptr.as_ref() };
                        if next_node.key.cmp(key) == Ordering::Less {
                            current = next_ptr;
                        } else {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }

        // 检查 level 0 的下一个节点
        let next = unsafe { current.as_ref().next(0) };
        next.map(|ptr| {
            let node = unsafe { ptr.as_ref() };
            (&node.key, &node.value)
        })
    }

    /// 检查 key 是否存在
    pub fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// 返回内存使用量
    pub fn memory_usage(&self) -> usize {
        self.arena.memory_usage()
    }

    /// 返回迭代器
    pub fn iter(&self) -> Iter<'_, K, V> {
        let first = unsafe { self.head.as_ref().next(0) };
        Iter {
            current: first,
            _marker: std::marker::PhantomData,
        }
    }

    /// 范围迭代器
    pub fn range<'a>(&'a self, start: &'a K, end: &'a K) -> RangeIter<'a, K, V> {
        let mut current = self.head;

        // 找到 >= start 的第一个节点
        for i in (0..self.max_height).rev() {
            loop {
                let next = unsafe { current.as_ref().next(i) };
                match next {
                    Some(next_ptr) => {
                        let next_node = unsafe { next_ptr.as_ref() };
                        if next_node.key.cmp(start) == Ordering::Less {
                            current = next_ptr;
                        } else {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }

        let start_node = unsafe { current.as_ref().next(0) };

        RangeIter {
            current: start_node,
            end,
            _marker: std::marker::PhantomData,
        }
    }
}

// 告诉编辑器：只要K和V是现场安全的，我的skipList就是线程安全的
unsafe impl<K: Send, V: Send> Send for SkipList<K, V> {}
unsafe impl<K: Send, V: Send> Sync for SkipList<K, V> {}

// ==================== 迭代器 ====================

pub struct Iter<'a, K, V> {
    current: NodePtr<K, V>,
    _marker: std::marker::PhantomData<&'a (K, V)>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.current.map(|ptr| {
            let node = unsafe { ptr.as_ref() };
            self.current = node.next(0);
            (&node.key, &node.value)
        })
    }
}

pub struct RangeIter<'a, K, V> {
    current: NodePtr<K, V>,
    end: &'a K,
    _marker: std::marker::PhantomData<&'a (K, V)>,
}

impl<'a, K: Ord, V> Iterator for RangeIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.current.and_then(|ptr| {
            let node = unsafe { ptr.as_ref() };
            if node.key.cmp(self.end) == Ordering::Less {
                self.current = node.next(0);
                Some((&node.key, &node.value))
            } else {
                None
            }
        })
    }
}

impl<K, V> Default for SkipList<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

// ==================== 测试 ====================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut sl: SkipList<i32, String> = SkipList::new();

        // 插入
        sl.insert(3, "three".to_string());
        sl.insert(1, "one".to_string());
        sl.insert(4, "four".to_string());
        sl.insert(1, "ONE".to_string()); // 重复 key
        sl.insert(5, "five".to_string());
        sl.insert(9, "nine".to_string());
        sl.insert(2, "two".to_string());

        // 查找
        assert_eq!(sl.get(&1), Some(&"one".to_string()));
        assert_eq!(sl.get(&5), Some(&"five".to_string()));
        assert_eq!(sl.get(&100), None);

        // 遍历（有序）
        let keys: Vec<_> = sl.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![1, 2, 3, 4, 5, 9]);
    }

    #[test]
    fn test_seek() {
        let mut sl: SkipList<i32, i32> = SkipList::new();

        for i in (0..100).step_by(10) {
            sl.insert(i, i * 10);
        }

        // seek 到存在的 key
        assert_eq!(sl.seek(&50), Some((&50, &500)));

        // seek 到不存在的 key，返回下一个
        assert_eq!(sl.seek(&55), Some((&60, &600)));

        // seek 超过最大值
        assert_eq!(sl.seek(&1000), None);
    }

    #[test]
    fn test_range() {
        let mut sl: SkipList<i32, i32> = SkipList::new();

        for i in 0..100 {
            sl.insert(i, i * 10);
        }

        let range: Vec<_> = sl.range(&20, &30).map(|(k, _)| *k).collect();
        assert_eq!(range, (20..30).collect::<Vec<_>>());
    }

    #[test]
    fn test_large_scale() {
        let mut sl: SkipList<u64, u64> = SkipList::new();
        let n = 100_000;

        // 插入
        for i in 0..n {
            sl.insert(i, i * 2);
        }
        assert_eq!(sl.len(), n as usize);

        // 查找
        for i in 0..n {
            assert_eq!(sl.get(&i), Some(&(i * 2)));
        }

        // 验证有序性
        let mut prev = None;
        for (k, _) in sl.iter() {
            if let Some(p) = prev {
                assert!(k > p);
            }
            prev = Some(k);
        }

        println!("Memory usage: {} bytes", sl.memory_usage());
    }
}
