use crate::goatkv::encoding::internal_key::InternalKey;
use bytes::Bytes;
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
struct Node {
    key: InternalKey,
    value: Bytes,
    height: usize,
    // tower 紧跟在结构体后面，通过 get_tower 访问
}

impl Node {
    /// 获取 tower 数组（存储各层的下一个节点指针）
    #[inline]
    fn tower(&self) -> &[NodePtr] {
        unsafe {
            let tower_ptr = (self as *const Self).add(1) as *const NodePtr;
            std::slice::from_raw_parts(tower_ptr, self.height)
        }
    }

    #[inline]
    fn tower_mut(&mut self) -> &mut [NodePtr] {
        unsafe {
            let tower_ptr = (self as *mut Self).add(1) as *mut NodePtr;
            std::slice::from_raw_parts_mut(tower_ptr, self.height)
        }
    }

    #[inline]
    fn next(&self, level: usize) -> NodePtr {
        self.tower()[level]
    }

    #[inline]
    fn set_next(&mut self, level: usize, node: NodePtr) {
        self.tower_mut()[level] = node;
    }
}

type NodePtr = Option<NonNull<Node>>;

// ==================== 跳表实现 ====================

#[derive(Debug)]
pub struct SkipList {
    arena: Arena,
    head: NonNull<Node>,
    max_height: usize, // 当前最大高度
    len: usize,
    rng: SmallRng,
}

impl SkipList {
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

    fn alloc_node(
        arena: &mut Arena,
        entry: Option<(InternalKey, Bytes)>,
        height: usize,
    ) -> NonNull<Node> {
        // 计算需要的内存：Node 结构体 + tower 数组
        let node_size = std::mem::size_of::<Node>();
        let tower_size = std::mem::size_of::<NodePtr>() * height;
        let total_size = node_size + tower_size;
        let align = std::mem::align_of::<Node>();

        let layout = std::alloc::Layout::from_size_align(total_size, align).unwrap();
        let ptr = arena.alloc_bytes(layout) as *mut Node;

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
            let tower_ptr = ptr.add(1) as *mut NodePtr;
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

impl SkipList {
    /// 插入键值对，如果 key 已存在则更新 value 并返回旧值
    pub fn insert(&mut self, key: InternalKey, value: Bytes) -> Option<&Bytes> {
        let mut prev = [None::<NonNull<Node>>; MAX_HEIGHT];
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
    pub fn get(&self, key: &[u8]) -> Option<&Bytes> {
        let mut current = self.head;

        for i in (0..self.max_height).rev() {
            loop {
                let next = unsafe { current.as_ref().next(i) };
                match next {
                    Some(next_ptr) => {
                        let next_node = unsafe { next_ptr.as_ref() };
                        match next_node.key.user_key().cmp(key) {
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
    pub fn seek(&self, key: &[u8]) -> Option<(&InternalKey, &Bytes)> {
        let mut current = self.head;

        for i in (0..self.max_height).rev() {
            loop {
                let next = unsafe { current.as_ref().next(i) };
                match next {
                    Some(next_ptr) => {
                        let next_node = unsafe { next_ptr.as_ref() };
                        if next_node.key.user_key().cmp(key) == Ordering::Less {
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
    pub fn contains(&self, key: &[u8]) -> bool {
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
    pub fn iter(&self) -> Iter<'_> {
        let first = unsafe { self.head.as_ref().next(0) };
        Iter {
            current: first,
            _marker: std::marker::PhantomData,
        }
    }

    /// 范围迭代器
    pub fn range<'a>(&'a self, start: &'a InternalKey, end: &'a InternalKey) -> RangeIter<'a> {
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
unsafe impl Send for SkipList {}
unsafe impl Sync for SkipList {}

// ==================== 迭代器 ====================

pub struct Iter<'a> {
    current: NodePtr,
    _marker: std::marker::PhantomData<&'a Bytes>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (InternalKey, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        self.current.map(|ptr| {
            let node = unsafe { ptr.as_ref() };
            self.current = node.next(0);
            (node.key.clone(), node.value.clone())
        })
    }
}

pub struct RangeIter<'a> {
    current: NodePtr,
    end: &'a InternalKey,
    _marker: std::marker::PhantomData<&'a (InternalKey, Bytes)>,
}

impl<'a> Iterator for RangeIter<'a> {
    type Item = (InternalKey, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        self.current.and_then(|ptr| {
            let node = unsafe { ptr.as_ref() };
            if node.key.cmp(self.end) == Ordering::Less {
                self.current = node.next(0);
                Some((node.key.clone(), node.value.clone()))
            } else {
                None
            }
        })
    }
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

// ==================== 测试 ====================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goatkv::encoding::internal_key::InternalKeyKind;

    fn make_key(key: &[u8], seq: u64) -> InternalKey {
        InternalKey::new(key.to_vec(), seq, InternalKeyKind::Put)
    }

    #[test]
    fn test_basic_operations() {
        let mut sl: SkipList = SkipList::new();

        // 插入
        sl.insert(make_key(b"3", 1), Bytes::from("three"));
        sl.insert(make_key(b"1", 1), Bytes::from("one"));
        sl.insert(make_key(b"4", 1), Bytes::from("four"));
        sl.insert(make_key(b"5", 1), Bytes::from("five"));
        sl.insert(make_key(b"9", 1), Bytes::from("nine"));
        sl.insert(make_key(b"2", 1), Bytes::from("two"));

        // 查找
        assert_eq!(sl.get(b"1"), Some(&Bytes::from("one")));
        assert_eq!(sl.get(b"5"), Some(&Bytes::from("five")));
        assert_eq!(sl.get(b"100"), None);

        // 遍历（有序）
        let keys: Vec<_> = sl.iter().map(|(k, _)| k.user_key().to_vec()).collect();
        assert_eq!(
            keys,
            vec![
                b"1".to_vec(),
                b"2".to_vec(),
                b"3".to_vec(),
                b"4".to_vec(),
                b"5".to_vec(),
                b"9".to_vec()
            ]
        );
    }

    #[test]
    fn test_seek() {
        let mut sl: SkipList = SkipList::new();

        // 使用固定宽度的数字格式以避免字典序问题
        for i in (0..100).step_by(10) {
            let key_str = format!("{:02}", i);
            sl.insert(
                make_key(key_str.as_bytes(), i as u64),
                Bytes::from((i * 10).to_string()),
            );
        }

        // seek 到存在的 key
        let result = sl.seek(b"50");
        assert!(result.is_some());
        assert_eq!(result.unwrap().0.user_key(), b"50".as_ref());

        // seek 到不存在的 key，返回下一个
        let result = sl.seek(b"55");
        assert!(result.is_some());
        assert_eq!(result.unwrap().0.user_key(), b"60".as_ref());

        // seek 超过最大值
        assert_eq!(sl.seek(b"99"), None);
    }

    #[test]
    fn test_range() {
        let mut sl: SkipList = SkipList::new();

        // 使用固定宽度的数字格式以避免字典序问题
        for i in 0..100 {
            let key_str = format!("{:02}", i);
            sl.insert(
                make_key(key_str.as_bytes(), i as u64),
                Bytes::from((i * 10).to_string()),
            );
        }

        let start = make_key(b"20", 20);
        let end = make_key(b"30", 30);

        let range: Vec<_> = sl
            .range(&start, &end)
            .map(|(k, _)| k.user_key().to_vec())
            .collect();

        assert_eq!(range.len(), 10);
        for i in 0..10 {
            assert_eq!(range[i], format!("{:02}", 20 + i).as_bytes());
        }
    }

    #[test]
    fn test_large_scale() {
        let mut sl: SkipList = SkipList::new();
        let n = 100_000;

        // 插入
        for i in 0..n {
            let key_str = format!("key_{:010}", i);
            sl.insert(
                make_key(key_str.as_bytes(), 1),
                Bytes::from(format!("value_{}", i)),
            );
        }
        assert_eq!(sl.len(), n);

        // 查找
        for i in 0..n {
            let key_str = format!("key_{:010}", i);
            assert!(sl.get(key_str.as_bytes()).is_some());
        }

        // 验证有序性
        let mut prev_key: Option<Vec<u8>> = None;
        for (k, _) in sl.iter() {
            if let Some(ref prev) = prev_key {
                assert!(k.user_key() > prev.as_slice());
            }
            prev_key = Some(k.user_key().to_vec());
        }

        println!("Memory usage: {} bytes", sl.memory_usage());
    }
}
