#![crate_id = "github.com/vhbit/lmdb-rs#lmdb:0.1"]
#![crate_type = "rlib"]

#![feature(phase)]
#![feature(globs)]
#![feature(macro_rules)]

#[phase(syntax, link)] extern crate log;
extern crate libc;

#[cfg(test)]
extern crate debug;

pub use consts = mdb::consts;
pub use mdb::types::{mdb_mode_t, mdb_filehandle_t, MDB_stat, MDB_envinfo};

mod traits;
mod utils;
mod mdb;
pub mod base;
