# GoatKV Unsafe Audit Checklist (SM6-01)

## Scope

- `src/goatkv/core/skip_list/arena.rs`
- `src/goatkv/core/skip_list/node.rs`
- `src/goatkv/core/skip_list/list.rs`
- `src/goatkv/core/skip_list/iter.rs`

## Core Invariants

1. Arena allocations are stable: pointers returned by `Arena` never move until `Arena` is dropped.
2. Skip list head node is special: `key/value` are intentionally uninitialized and must never be dropped/read.
3. Node tower layout is contiguous after `Node<K>` with length exactly `height`.
4. Skip list links are only rewired via `link_new_node` under `&mut self`, preserving per-level order.
5. Iterator/read traversals only dereference pointers originating from in-list `next(level)` links.

## Unsafe Site Map

- `arena.rs::alloc`
  - Assumes `alloc_bytes(Layout::new::<T>())` returns aligned writable memory for `T`.
  - Writes exactly one initialized `T` into uninitialized arena region.
- `arena.rs::alloc_slice`
  - Assumes destination has contiguous capacity for `src.len()` elements.
  - Requires `T: Copy` and non-overlap for `copy_nonoverlapping`.
- `arena.rs::alloc_bytes`
  - `as_mut_ptr().add(aligned_pos)` must stay within current chunk capacity.
  - `set_len(aligned_pos + size)` only after capacity guard.
- `node.rs::tower/tower_mut`
  - Reinterprets trailing memory as `NodePtr<K>` slice of length `height`.
  - Depends on `alloc_node` allocating `Node + height * NodePtr`.
- `list.rs::alloc_node`
  - Initializes data/head node and all tower entries before publication.
  - Head node uninitialized fields are never observed as valid user data.
- `list.rs` pointer traversals (`insert/get/seek/find_*`)
  - `NonNull<Node<K>>` comes from established links and is dereferenced read-only.
  - Traversal only follows `next(level)` pointers.
- `list.rs::link_new_node`
  - Mutates predecessor/new node links under exclusive `&mut self` insertion flow.
  - New node is not externally visible before link completion.
- `list.rs::Drop`
  - Walks level-0 once, drops each data node exactly once, skips head node drop.
- `iter.rs::Iter/RangeIter::next`
  - Dereferences current pointer, clones payload, then advances along level-0 link.
- `list.rs::unsafe impl Send/Sync for SkipList`
  - Valid because storage is arena-backed and pointer mutation is restricted to `&mut self`.
  - Shared read traversal requires `K: Sync`; transfer across threads requires `K: Send`.

## Review Checklist

- [ ] New unsafe blocks include a `Safety:` comment with pointer origin, lifetime, and aliasing constraints.
- [ ] Head node special-case invariant is preserved (no drop/read of uninitialized key/value).
- [ ] Any change to node memory layout updates both allocation and tower access logic.
- [ ] Pointer rewiring still happens only in insertion path under exclusive mutable access.
- [ ] `Send/Sync` assumptions still hold after any interior mutability or ownership model changes.
- [ ] Skip list tests pass after modifications.

## Validation Commands

- `cargo test --lib goatkv::core::skip_list::tests`
- `cargo test --lib`
