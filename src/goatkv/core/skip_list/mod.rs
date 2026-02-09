mod arena;
mod iter;
mod list;
mod node;

#[cfg(test)]
mod tests;

pub use arena::Arena;
pub use iter::{Iter, RangeIter};
pub use list::SkipList;

/// 定义用户键比较操作的 trait
/// 泛型跳表需要这个 trait 来比较键
pub trait UserKey: Ord + Clone {
    /// 获取用户键的字节表示
    fn user_key(&self) -> &[u8];
}
