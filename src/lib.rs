#![crate_id = "github.com/vhbit/lmdb-rs#lmdb:0.2"]
#![crate_type = "rlib"]

#![feature(phase)]
#![feature(globs)]
#![feature(macro_rules)]
#![feature(unsafe_destructor)]

#[phase(plugin, link)] extern crate log;
extern crate libc;

#[cfg(test)]
extern crate debug;

pub use consts = ffi::consts;
pub use ffi::types::{mdb_mode_t, mdb_filehandle_t, MDB_stat, MDB_envinfo};

mod traits;
mod utils;
mod ffi;
pub mod base;
