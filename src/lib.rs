#![feature(unsafe_destructor)]
#![feature(unboxed_closures)]
#![feature(libc)]
#![feature(core)]
#![feature(std_misc)]
#![feature(path)]
#![feature(io)]
#![feature(collections)]
#![feature(hash)]
#![allow(unused_attributes)]

extern crate libc;

#[macro_use] extern crate bitflags;
#[macro_use] extern crate log;

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
