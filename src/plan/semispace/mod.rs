//! Plan: semispace

pub(super) mod gc_work;
pub(crate) mod global;
pub(super) mod mutator;

pub use self::global::SemiSpace;
pub use self::global::SS_CONSTRAINTS;
