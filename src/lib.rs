#![allow(trivial_casts)]
#![allow(trivial_numeric_casts)]

pub use crate::core::{
    Cursor, CursorIter, CursorKeyRangeIter, CursorValue, Database, DbFlags, DbHandle, EnvBuilder,
    EnvCreateFlags, EnvFlags, Environment, MdbError, MdbValue, ReadonlyTransaction, Transaction,
};
pub use libc::c_int;
pub use liblmdb_sys::{mdb_filehandle_t, MDB_envinfo, MDB_stat, MDB_val};
pub use traits::{FromMdbValue, ToMdbValue};

pub mod core;
pub mod traits;
mod utils;

#[cfg(test)]
mod tests;
