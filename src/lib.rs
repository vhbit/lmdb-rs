#![allow(trivial_casts)]
#![allow(trivial_numeric_casts)]

extern crate libc;

#[macro_use] extern crate bitflags;
#[macro_use] extern crate log;

extern crate liblmdb_sys as ffi;

pub use libc::c_int;
pub use ffi::{mdb_filehandle_t, MDB_stat, MDB_envinfo, MDB_val};
pub use core::{EnvBuilder, Environment, EnvFlags, EnvCreateFlags};
pub use core::{Database, DbFlags, DbHandle};
pub use core::{Transaction, ReadonlyTransaction, MdbError, MdbValue};
pub use core::{Cursor, CursorValue, CursorIter, CursorKeyRangeIter};
pub use traits::{FromMdbValue, ToMdbValue};

pub mod core;
pub mod traits;
mod utils;

#[cfg(test)]
mod tests;
