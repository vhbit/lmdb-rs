#![crate_id = "lmdb-rs"]
#![crate_type = "rlib"]
#![crate_type = "staticlib"] // for now

#![feature(phase)]
#![feature(globs)]

#[phase(syntax, link)] extern crate log;
extern crate libc;

pub mod mdb;
