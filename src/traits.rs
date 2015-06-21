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


use std::{self, mem, slice};

pub trait AsByteSlice {
    fn as_byte_slice<'a>(&'a self) -> &'a [u8];
}

/// `FromMdbValue` is supposed to reconstruct a value from
/// memory slice. It allows to use zero copy where it is
/// required.
pub trait FromBytes {
    fn from_bytes(value: &[u8]) -> Self;
}

impl FromBytes for String {
    fn from_bytes(value: &[u8]) -> String {
        let data: Vec<u8> = value.to_owned();
        String::from_utf8(data).unwrap()
    }
}

impl FromBytes for Vec<u8> {
    fn from_bytes(value: &[u8]) -> Vec<u8> {
        value.to_owned()
    }
}

impl FromBytes for () {
    fn from_bytes(_: &[u8]) {
    }
}

impl<'b> FromBytes for &'b str {
    fn from_bytes(value: &[u8]) -> &'b str {
        unsafe {
            mem::transmute(slice::from_raw_parts(value.as_ptr(), value.len()))
        }
    }
}

impl<'b> FromBytes for &'b [u8] {
    fn from_bytes(value: &[u8]) -> &'b [u8] {
        unsafe {
            slice::from_raw_parts(value.as_ptr(), value.len())
        }
    }
}

macro_rules! mdb_for_primitive {
    ($t:ty) => (
        impl AsByteSlice for $t {
            fn as_byte_slice<'a>(&'a self) -> &'a [u8] {
                unsafe {std::slice::from_raw_parts(mem::transmute(self), mem::size_of::<$t>())}
            }
        }

        impl FromBytes for $t {
            fn from_bytes(value: &[u8]) -> $t {
                unsafe {
                    let t: *mut $t = mem::transmute(value.as_ptr());
                    *t
                }
            }
        }

        )
}

macro_rules! mdb_for_int {
    ($t:ty, $e:expr) => (
        impl AsByteSlice for $t {
            fn as_byte_slice<'a>(&'a self) -> &'a [u8] {
                unsafe {std::slice::from_raw_parts(mem::transmute(self), mem::size_of::<$t>())}
            }
        }

        impl FromBytes for $t {
            fn from_bytes(value: &[u8]) -> $t {
                unsafe {
                    let t: *mut $t = mem::transmute(value.as_ptr());
                    *t
                }
            }
        }
        )
}


mdb_for_primitive!(u8);
mdb_for_primitive!(i8);
mdb_for_primitive!(f32);
mdb_for_primitive!(f64);
mdb_for_primitive!(bool);

mdb_for_int!(u16, u16::from_le);
mdb_for_int!(i16, i16::from_le);
mdb_for_int!(u32, u32::from_le);
mdb_for_int!(i32, i32::from_le);
mdb_for_int!(u64, u64::from_le);
mdb_for_int!(i64, i64::from_le);
