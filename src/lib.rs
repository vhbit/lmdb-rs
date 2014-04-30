#![crate_id = "lmdb-rs"]
#![crate_type = "rlib"]
#![crate_type = "staticlib"] // for now

#![feature(phase)]
#![feature(globs)]
#![feature(macro_rules)]

#[phase(syntax, link)] extern crate log;
extern crate libc;

pub use consts = mdb::consts;
pub use mdb::types::{mdb_mode_t, mdb_filehandle_t, MDB_stat, MDB_envinfo};

mod traits;
mod utils;
mod mdb;
pub mod base;
pub mod ext2;
