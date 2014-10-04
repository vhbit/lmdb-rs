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
extern crate debug;

pub use ffi::types::{mdb_filehandle_t, MDB_stat, MDB_envinfo};
pub use core::{EnvBuilder, Environment, EnvFlags, EnvCreateFlags, Database, DbFlags};
pub use core::{Transaction, ReadonlyTransaction, MdbError};
pub use core::{Cursor, CursorValue, CursorIter, CursorKeyRangeIter};
pub use core::errors;
pub use traits::{FromMdbValue, ToMdbValue};

pub mod traits;
mod utils;
mod ffi;
pub mod core;

#[cfg(test)]
mod tests;
