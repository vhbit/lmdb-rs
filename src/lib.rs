#![feature(phase)]
#![feature(globs)]
#![feature(macro_rules)]
#![feature(unsafe_destructor)]
#![feature(unboxed_closures)]

extern crate libc;
#[phase(plugin, link)] extern crate log;

extern crate "liblmdb-sys" as ffi;

#[stable]
pub use ffi::{mdb_filehandle_t, MDB_stat, MDB_envinfo};
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
