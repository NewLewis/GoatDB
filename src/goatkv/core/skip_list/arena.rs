use std::ptr::NonNull;

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
            NonNull::new_unchecked(std::ptr::slice_from_raw_parts_mut(ptr, src.len()))
        }
    }

    pub(crate) fn alloc_bytes(&mut self, layout: std::alloc::Layout) -> *mut u8 {
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
