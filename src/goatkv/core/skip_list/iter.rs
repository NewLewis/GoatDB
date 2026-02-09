use bytes::Bytes;
use std::marker::PhantomData;

use super::node::NodePtr;
use super::UserKey;

pub struct Iter<'a, K>
where
    K: UserKey,
{
    pub(crate) current: NodePtr<K>,
    pub(crate) _marker: PhantomData<&'a K>,
}

impl<'a, K> Iterator for Iter<'a, K>
where
    K: UserKey,
{
    type Item = (K, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        self.current.map(|ptr| unsafe {
            let node = ptr.as_ref();
            self.current = node.next(0);
            (node.key.clone(), node.value.clone())
        })
    }
}

pub struct RangeIter<'a, K>
where
    K: UserKey,
{
    pub(crate) current: NodePtr<K>,
    pub(crate) end: &'a K,
    pub(crate) _marker: PhantomData<&'a K>,
}

impl<'a, K> Iterator for RangeIter<'a, K>
where
    K: UserKey,
{
    type Item = (K, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        self.current.and_then(|ptr| unsafe {
            let node = ptr.as_ref();
            if node.key.cmp(self.end) == std::cmp::Ordering::Less {
                self.current = node.next(0);
                Some((node.key.clone(), node.value.clone()))
            } else {
                None
            }
        })
    }
}
