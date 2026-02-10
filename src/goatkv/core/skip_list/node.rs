use bytes::Bytes;
use std::ptr::NonNull;

use super::UserKey;

#[repr(C)]
pub(crate) struct Node<K>
where
    K: UserKey,
{
    pub(crate) key: K,
    pub(crate) value: Bytes,
    pub(crate) height: usize,
    // tower 紧跟在结构体后面，通过 get_tower 访问
}

impl<K> Node<K>
where
    K: UserKey,
{
    /// 获取 tower 数组（存储各层的下一个节点指针）
    #[inline]
    pub(crate) fn tower(&self) -> &[NodePtr<K>] {
        unsafe {
            let tower_ptr = (self as *const Self).add(1) as *const NodePtr<K>;
            std::slice::from_raw_parts(tower_ptr, self.height)
        }
    }

    #[inline]
    pub(crate) fn tower_mut(&mut self) -> &mut [NodePtr<K>] {
        unsafe {
            let tower_ptr = (self as *mut Self).add(1) as *mut NodePtr<K>;
            std::slice::from_raw_parts_mut(tower_ptr, self.height)
        }
    }

    #[inline]
    pub(crate) fn next(&self, level: usize) -> NodePtr<K> {
        self.tower()[level]
    }

    #[inline]
    pub(crate) fn set_next(&mut self, level: usize, node: NodePtr<K>) {
        self.tower_mut()[level] = node;
    }
}

pub(crate) type NodePtr<K> = Option<NonNull<Node<K>>>;
