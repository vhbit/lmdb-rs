#![crate_name = "lmdb"]
#![crate_type = "rlib"]

#![feature(phase)]
#![feature(globs)]
#![feature(macro_rules)]
#![feature(unsafe_destructor)]

#[phase(plugin, link)] extern crate log;
extern crate libc;

#[cfg(test)]
extern crate debug;

pub use ffi::consts as consts;
pub use ffi::types::{mdb_mode_t, mdb_filehandle_t, MDB_stat, MDB_envinfo};
pub use base::{Environment, Database, Transaction, ReadonlyTransaction, MdbError};
pub use base::{Cursor, CursorValue, CursorIter, CursorKeyRangeIter};
pub use base::errors;

mod traits;
mod utils;
mod ffi;
pub mod base;
