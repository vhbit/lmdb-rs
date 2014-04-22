#![feature(globs)]
#![crate_id = "lmdb-rs"]
#![crate_type = "rlib"]
#![crate_type = "staticlib"] // for now

extern crate libc;

pub mod mdb;
