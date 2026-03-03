use bytes::Bytes;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ptr::NonNull;

use super::arena::Arena;
use super::iter::{Iter, RangeIter};
use super::node::{Node, NodePtr};
use super::UserKey;

// Increase MAX_HEIGHT to handle larger datasets
const MAX_HEIGHT: usize = 32;

#[derive(Debug)]
pub struct SkipList<K>
where
    K: UserKey,
{
    arena: Arena,
    head: NonNull<Node<K>>,
    max_height: usize,
    len: usize,
    rng: SmallRng,
    _phantom: PhantomData<K>,
}

impl<K> SkipList<K>
where
    K: UserKey,
{
    pub fn new() -> Self {
        Self::with_arena(Arena::new())
    }

    pub fn with_arena(mut arena: Arena) -> Self {
        let head = Self::alloc_node(&mut arena, None, MAX_HEIGHT);
        Self {
            arena,
            head,
            max_height: 1,
            len: 0,
            rng: SmallRng::from_entropy(),
            _phantom: PhantomData,
        }
    }

    fn alloc_node(arena: &mut Arena, entry: Option<(K, Bytes)>, height: usize) -> NonNull<Node<K>> {
        let node_size = std::mem::size_of::<Node<K>>();
        let tower_size = std::mem::size_of::<NodePtr<K>>() * height;
        let total_size = node_size + tower_size;
        let align = std::mem::align_of::<Node<K>>();

        let layout = std::alloc::Layout::from_size_align(total_size, align).unwrap();
        let ptr = arena.alloc_bytes(layout) as *mut Node<K>;

        unsafe {
            if let Some((key, value)) = entry {
                std::ptr::write(ptr, Node { key, value, height });
            } else {
                std::ptr::write_bytes(ptr, 0, 1);
                (*ptr).height = height;
            }

            let tower_ptr = ptr.add(1) as *mut NodePtr<K>;
            for i in 0..height {
                std::ptr::write(tower_ptr.add(i), None);
            }

            NonNull::new_unchecked(ptr)
        }
    }

    fn random_height(&mut self) -> usize {
        let mut height = 1;
        while height < MAX_HEIGHT && self.rng.gen_bool(0.5) {
            height += 1;
        }
        height
    }

    /// 插入键值对，如果 key 已存在则返回已存在的值（不覆盖）
    pub fn insert(&mut self, key: K, value: Bytes) -> Option<&Bytes> {
        let (mut prev, existing) = self.find_predecessors(&key);
        if let Some(ptr) = existing {
            let node = unsafe { ptr.as_ref() };
            return Some(&node.value);
        }

        let height = self.random_height();
        let mut new_node = Self::alloc_node(&mut self.arena, Some((key, value)), height);
        self.extend_height(&mut prev, height);
        self.link_new_node(&mut new_node, &prev, height);
        self.len += 1;
        None
    }

    /// 查找 key
    pub fn get(&self, key: &[u8]) -> Option<&Bytes> {
        self.find_equal(key)
            .map(|ptr| unsafe { &ptr.as_ref().value })
    }

    /// 查找大于等于 key 的最小元素
    pub fn seek(&self, key: &[u8]) -> Option<(&K, &Bytes)> {
        self.find_ge(key).map(|ptr| unsafe {
            let node = ptr.as_ref();
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
    pub fn iter(&self) -> Iter<'_, K> {
        let first = unsafe { self.head.as_ref().next(0) };
        Iter {
            current: first,
            _marker: PhantomData,
        }
    }

    /// 范围迭代器
    pub fn range<'a>(&'a self, start: &'a K, end: &'a K) -> RangeIter<'a, K> {
        let start_node = self.find_ge(start.user_key());
        RangeIter {
            current: start_node,
            end,
            _marker: PhantomData,
        }
    }

    fn find_predecessors(&self, key: &K) -> ([NodePtr<K>; MAX_HEIGHT], Option<NonNull<Node<K>>>) {
        let mut prev = [None::<NonNull<Node<K>>>; MAX_HEIGHT];
        let mut current = self.head;

        for i in (0..self.max_height).rev() {
            loop {
                let next = unsafe { current.as_ref().next(i) };
                match next {
                    Some(next_ptr) => {
                        let next_node = unsafe { next_ptr.as_ref() };
                        match next_node.key.cmp(key) {
                            Ordering::Less => current = next_ptr,
                            Ordering::Equal => return (prev, Some(next_ptr)),
                            Ordering::Greater => break,
                        }
                    }
                    None => break,
                }
            }
            prev[i] = Some(current);
        }

        (prev, None)
    }

    fn extend_height(&mut self, prev: &mut [NodePtr<K>; MAX_HEIGHT], height: usize) {
        if height <= self.max_height {
            return;
        }
        for prev_item in prev.iter_mut().take(height).skip(self.max_height) {
            *prev_item = Some(self.head);
        }
        self.max_height = height;
    }

    fn link_new_node(
        &self,
        new_node: &mut NonNull<Node<K>>,
        prev: &[NodePtr<K>; MAX_HEIGHT],
        height: usize,
    ) {
        for (i, prev_item) in prev.iter().enumerate().take(height) {
            if let Some(mut prev_node) = prev_item {
                let prev_next = unsafe { prev_node.as_ref().next(i) };
                unsafe {
                    new_node.as_mut().set_next(i, prev_next);
                    prev_node.as_mut().set_next(i, Some(*new_node));
                }
            }
        }
    }

    fn find_equal(&self, key: &[u8]) -> Option<NonNull<Node<K>>> {
        let mut current = self.head;

        for i in (0..self.max_height).rev() {
            loop {
                let next = unsafe { current.as_ref().next(i) };
                match next {
                    Some(next_ptr) => {
                        let next_node = unsafe { next_ptr.as_ref() };
                        match next_node.key.user_key().cmp(key) {
                            Ordering::Less => current = next_ptr,
                            Ordering::Equal => return Some(next_ptr),
                            Ordering::Greater => break,
                        }
                    }
                    None => break,
                }
            }
        }
        None
    }

    fn find_ge(&self, key: &[u8]) -> Option<NonNull<Node<K>>> {
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

        unsafe { current.as_ref().next(0) }
    }
}

unsafe impl<K> Send for SkipList<K> where K: UserKey + Send {}
unsafe impl<K> Sync for SkipList<K> where K: UserKey + Sync {}

impl<K> Default for SkipList<K>
where
    K: UserKey,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K> Drop for SkipList<K>
where
    K: UserKey,
{
    fn drop(&mut self) {
        // Safety:
        // - All nodes are allocated from `arena` and stay valid until `arena` drops.
        // - We walk level-0 forward pointers exactly once and drop each data node.
        // - The head node stores uninitialized key/value fields by design, so it must
        //   never be dropped as `Node<K>`.
        let mut current = unsafe { self.head.as_ref().next(0) };
        while let Some(node_ptr) = current {
            unsafe {
                let raw = node_ptr.as_ptr();
                current = (*raw).next(0);
                std::ptr::drop_in_place(raw);
            }
        }
    }
}
