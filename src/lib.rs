#![crate_id = "lmdb-rs"]
#![crate_type = "rlib"]
#![crate_type = "staticlib"] // for now

#![feature(phase)]
#![feature(globs)]

#[phase(syntax, link)] extern crate log;
extern crate libc;

mod traits;
mod utils;
mod mdb;
pub mod base;
