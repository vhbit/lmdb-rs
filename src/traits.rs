//! Conversion of data structures to and from MDB_val
//!
//! Since MDB_val is valid through whole transaction, it is kind of safe
//! to keep plain data, i.e. to keep raw pointers and transmute them back
//! and forward into corresponding data structures to avoid any unnecessary
//! copying.
//!
//! `MdbValue` is a simple wrapper with bounded lifetime which should help
//! keep it sane, i.e. provide compile errors when data retrieved outlives
//! transaction.
//!
//! It would be extremely helpful to create `compile-fail` tests to ensure
//! this, but unfortunately there is no way yet.
//!
//! Note that due to https://github.com/rust-lang/rust/issues/17322 it is
//! impossible to convert `&'a str` and `&'a [u8]` for now


use std::{mod, mem, string};

use core::MdbValue;
use ffi::types::MDB_val;

/// `ToMdbValue` is supposed to convert a value to a memory
/// slice which `lmdb` uses to prevent multiple copying data
/// multiple times. May be unsafe.
#[experimental]
pub trait ToMdbValue {
    fn to_mdb_value<'a>(&'a self) -> MdbValue<'a>;
}

/// `FromMdbValue` is supposed to reconstruct a value from
/// memory slice. It allows to use zero copy where it is
/// required.
#[experimental]
pub trait FromMdbValue<'a, T:'a> {
    fn from_mdb_value(value: &'a MdbValue<'a>) -> T;
}

impl ToMdbValue for Vec<u8> {
    fn to_mdb_value<'a>(&'a self) -> MdbValue<'a> {
        unsafe {
            MdbValue::new(std::mem::transmute(self.as_ptr()), self.len())
        }
    }
}

impl<'a> FromMdbValue<'a, Vec<u8>> for Vec<u8> {
    fn from_mdb_value<'a>(value: &'a MdbValue<'a>) -> Vec<u8> {
        unsafe {
            std::vec::raw::from_buf(std::mem::transmute(value.get_ref()),
                                    value.get_size() as uint)
        }
    }
}

impl ToMdbValue for String {
    fn to_mdb_value<'a>(&'a self) -> MdbValue<'a> {
        unsafe {
            let t = self.as_slice();
            MdbValue::new(std::mem::transmute(t.as_ptr()), t.len())
        }
    }
}

// Conversion from `&'a str` and `&'a [u8]` is broken due:
// https://github.com/rust-lang/rust/issues/17322
impl<'a> ToMdbValue for &'a str {
    fn to_mdb_value<'a>(&'a self) -> MdbValue<'a> {
        unsafe {
            MdbValue::new(mem::transmute(self.as_ptr()),
                          self.len())
        }
    }
}

/*
impl<'a> ToMdbValue<'a> for &'a [u8] {
    fn to_mdb_value(&'a self) -> MdbValue<'a> {
        unsafe {
            MdbValue::new(std::mem::transmute(self.as_ptr()),
                          self.len())
        }
    }
}
*/

impl ToMdbValue for MDB_val {
    fn to_mdb_value<'a>(&'a self) -> MdbValue<'a> {
        unsafe {
            MdbValue::new((*self).mv_data, (*self).mv_size as uint)
        }
    }
}

impl<'a> ToMdbValue for MdbValue<'a> {
    fn to_mdb_value<'a>(&'a self) -> MdbValue<'a> {
        *self
    }
}


impl<'a> FromMdbValue<'a, String> for String {
    fn from_mdb_value(value: &'a MdbValue<'a>) -> String {
        unsafe {
            string::raw::from_buf_len(std::mem::transmute(value.get_ref()),
                                      value.get_size()).to_string()
        }
    }
}

impl<'a> FromMdbValue<'a, ()> for () {
    fn from_mdb_value(_: &'a MdbValue<'a>) {
    }
}

impl<'a> FromMdbValue<'a, &'a str> for &'a str {
    fn from_mdb_value(value: &'a MdbValue<'a>) -> &'a str {
        unsafe {
            std::mem::transmute(std::raw::Slice {
                data: value.get_ref(),
                len: value.get_size(),
            })
        }
    }
}

impl<'a> FromMdbValue<'a, &'a [u8]> for &'a [u8] {
    fn from_mdb_value<'a>(value: &'a MdbValue<'a>) -> &'a [u8] {
        unsafe {
            std::mem::transmute(std::raw::Slice {
                data: value.get_ref(),
                len: value.get_size(),
            })
        }
    }
}

macro_rules! mdb_for_primitive {
    ($t:ty) => (
        impl ToMdbValue for $t {
            fn to_mdb_value<'a>(&'a self) -> MdbValue<'a> {
                MdbValue::new_from_sized(self)
            }
        }

        impl<'a> FromMdbValue<'a, $t> for $t {
            fn from_mdb_value<'a>(value: &'a MdbValue<'a>) -> $t {
                unsafe {
                    let t: *const $t = mem::transmute(value.get_ref());
                    *t
                }
            }
        }

        )
}

mdb_for_primitive!(u8)
mdb_for_primitive!(i8)
mdb_for_primitive!(u16)
mdb_for_primitive!(i16)
mdb_for_primitive!(u32)
mdb_for_primitive!(i32)
mdb_for_primitive!(u64)
mdb_for_primitive!(i64)
mdb_for_primitive!(f32)
mdb_for_primitive!(f64)
mdb_for_primitive!(bool)
