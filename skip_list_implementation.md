# 跳表（Skip List）实现原理详解

## 1. 跳表概述

跳表是一种高效的动态数据结构，它通过在链表基础上增加多级索引，实现了平均 O(log n) 时间复杂度的插入、删除和查找操作。跳表的核心思想是通过空间换取时间，通过建立多层次的"快捷方式"来加速链表的访问。

## 2. 核心数据结构

### 2.1 Node 结构体

```rust
#[repr(C)]
struct Node<K, V> {
    key: K,
    value: V,
    height: usize,
    // tower 紧跟在结构体后面，通过 get_tower 访问
}
```

- **key**: 节点的键值，用于排序和查找
- **value**: 节点存储的值
- **height**: 节点的高度（即塔的层数）
- **tower**: 塔结构，存储各层的下一个节点指针，通过 `tower()` 和 `tower_mut()` 方法访问

### 2.2 Node 方法

```rust
impl<K, V> Node<K, V> {
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

    fn next(&self, level: usize) -> NodePtr<K, V> {
        self.tower()[level]
    }

    fn set_next(&mut self, level: usize, node: NodePtr<K, V>) {
        self.tower_mut()[level] = node;
    }
}
```

- **tower()/tower_mut()**: 使用指针运算获取塔数组的引用，这是一种内存高效的实现方式
- **next()**: 获取指定层的下一个节点指针
- **set_next()**: 设置指定层的下一个节点指针

### 2.3 SkipList 结构体

```rust
pub struct SkipList<K, V> {
    arena: Arena,
    head: NonNull<Node<K, V>>,
    max_height: usize, // 当前最大高度
    len: usize,
    rng: ThreadRng,
}

const MAX_HEIGHT: usize = 32;
type NodePtr<K, V> = Option<NonNull<Node<K, V>>>;
```

- **arena**: 内存分配器，用于高效分配节点内存
- **head**: 跳表的头节点，高度为 MAX_HEIGHT
- **max_height**: 跳表当前的最大高度
- **len**: 跳表中的节点数量
- **rng**: 随机数生成器，用于决定新节点的高度
- **MAX_HEIGHT**: 跳表允许的最大高度，这里设置为 32
- **NodePtr**: 节点指针类型，使用 `Option<NonNull<Node<K, V>>>` 表示

## 3. 核心实现机制

### 3.1 节点分配

跳表使用 `alloc_node` 方法在内存池中分配节点：

```rust
fn alloc_node(arena: &mut Arena, entry: Option<(K, V)>, height: usize) -> NonNull<Node<K, V>> {
    // 计算需要的内存：Node 结构体 + tower 数组
    let node_size = std::mem::size_of::<Node<K, V>>();
    let tower_size = std::mem::size_of::<NodePtr<K, V>>() * height;
    let total_size = node_size + tower_size;
    let align = std::mem::align_of::<Node<K, V>>();

    let layout = std::alloc::Layout::from_size_align(total_size, align).unwrap();
    let ptr = arena.alloc_bytes(layout) as *mut Node<K, V>;

    unsafe {
        if let Some((key, value)) = entry {
            // 普通节点 - 初始化键和值
            std::ptr::write(ptr, Node { key, value, height });
        } else {
            // 头节点 - 初始化高度
            std::ptr::write_bytes(ptr, 0, 1);
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
```

节点内存布局：
```
┌─────────────────────────────────────────────────────────┐
│ Node Struct:                                             │
│   - key: K                                               │
│   - value: V                                             │
│   - height: usize                                        │
├─────────────────────────────────────────────────────────┤
│ Tower Array:                                             │
│   - [0]: NodePtr<K, V>                                   │
│   - [1]: NodePtr<K, V>                                   │
│   - ...                                                 │
│   - [height-1]: NodePtr<K, V>                           │
└─────────────────────────────────────────────────────────┘
```

### 3.2 随机高度生成

跳表的性能依赖于节点高度的随机化。这里使用了标准的跳表概率算法，50% 概率增加高度，直到达到 MAX_HEIGHT：

```rust
fn random_height(&mut self) -> usize {
    let mut height = 1;
    // 使用标准跳表概率 (50% 机会增加高度)
    while height < MAX_HEIGHT && self.rng.gen_bool(0.5) {
        height += 1;
    }
    height
}
```

### 3.3 插入操作

插入操作是跳表的核心，主要步骤：

1. **查找插入位置**：从最高层开始查找，记录每一层需要插入的前一个节点
2. **创建新节点**：随机生成节点高度，并在内存池中分配节点
3. **更新跳表高度**：如果新节点高度超过当前最大高度，更新 max_height
4. **插入节点**：在各层插入新节点，更新前后节点的指针

```rust
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
                            // key 已存在，直接返回已存在的值
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
                // 设置新节点的 next 指针
                new_node.as_mut().set_next(i, prev_next);
                // 设置前一个节点的 next 指针指向新节点
                prev_node.as_mut().set_next(i, Some(new_node));
            }
        }
    }

    self.len += 1;
    None
}
```

### 3.4 查找操作

查找操作从最高层开始，逐步向下层移动，直到找到目标节点或确定不存在：

```rust
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
```

### 3.5 迭代器

跳表实现了两种迭代器：
- **Iter**: 遍历所有节点
- **RangeIter**: 遍历指定范围的节点

```rust
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
```

迭代器只需要遍历第 0 层即可，因为第 0 层包含了所有节点。

## 4. 性能分析

### 4.1 时间复杂度

- **插入**: 平均 O(log n)，最坏 O(n)
- **查找**: 平均 O(log n)，最坏 O(n)
- **删除**: 平均 O(log n)，最坏 O(n)
- **迭代**: O(n)

### 4.2 空间复杂度

跳表的空间复杂度为 O(n)，平均每个节点的高度为 2，因此实际空间开销约为 2n。

## 5. 应用场景

这个跳表实现是作为 LSM-Tree 的 MemTable 设计的，适合：

1. **需要高效插入、删除和查找的数据场景**
2. **LSM-Tree 中的内存表**
3. **需要有序迭代的数据结构**

## 6. 实现特点

1. **内存高效**：使用 Arena 内存池分配节点，减少内存碎片
2. **缓存友好**：节点和塔结构连续存储在内存中
3. **类型安全**：使用 Rust 的类型系统确保安全
4. **高效迭代**：O(n) 时间复杂度的迭代器
5. **支持范围查询**：实现了 range 方法

## 7. 代码优化点

- **Arena 内存分配**：提高了内存分配效率
- **#[inline]** 注解：关键方法使用内联，提高性能
- **指针运算访问塔**：直接访问内存，避免了额外的指针开销
- **头节点复用**：头节点高度为 MAX_HEIGHT，避免了动态调整头节点高度的开销

## 8. 总结

这个跳表实现是一个高效、内存友好的动态数据结构，通过多级索引实现了平均 O(log n) 的时间复杂度。它特别适合作为 LSM-Tree 的 MemTable，提供快速的插入、查找和范围查询功能。