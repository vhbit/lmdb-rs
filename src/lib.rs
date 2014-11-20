#![crate_name = "lmdb"]
#![crate_type = "rlib"]

#![feature(phase)]
#![feature(globs)]
#![feature(macro_rules)]
#![feature(unsafe_destructor)]
#![feature(if_let)]

#[phase(plugin, link)] extern crate log;
extern crate libc;
extern crate sync;

#[cfg(test)]
extern crate rustrt;

extern crate "liblmdb-sys" as ffi;

#[stable]
pub use ffi::{mdb_filehandle_t, MDB_stat, MDB_envinfo};
pub use core::{EnvBuilder, Environment, EnvFlags, EnvCreateFlags};
pub use core::{Database, DbFlags, DbHandle};
pub use core::{Transaction, ReadonlyTransaction, MdbError, MdbValue};
pub use core::{Cursor, CursorValue, CursorIter, CursorKeyRangeIter};
pub use traits::{FromMdbValue, ToMdbValue};

pub mod traits;
mod utils;
pub mod core;

#[cfg(test)]
mod tests;
