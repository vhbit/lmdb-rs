use libc::{c_void};
use std;
use std::string;
use libc::size_t;
use ffi::types::MDB_val;

#[deriving(Clone)]
pub struct MdbValue<'a> {
    pub value: MDB_val
}

impl<'a> MdbValue<'a> {
    fn new(data: *const c_void, len: uint) -> MdbValue<'a> {
        MdbValue {
            value: MDB_val {
                mv_data: data,
                mv_size: len as size_t
            }
        }
    }
}

pub trait ToMdbValue<'a> {
    fn to_mdb_value(&'a self) -> MdbValue<'a>;
}

pub trait FromMdbValue<'a> {
    fn from_mdb_value(value: &'a MdbValue<'a>) -> Self;
}

impl<'a> ToMdbValue<'a> for Vec<u8> {
    fn to_mdb_value(&'a self) -> MdbValue<'a> {
        unsafe {
            MdbValue::new(std::mem::transmute(self.as_ptr()), self.len())
        }
    }
}

impl<'a> FromMdbValue<'a> for Vec<u8> {
    fn from_mdb_value(value: &'a MdbValue<'a>) -> Vec<u8> {
        unsafe {
            std::vec::raw::from_buf(std::mem::transmute(value.value.mv_data), value.value.mv_size as uint)
        }
    }
}

impl<'a> ToMdbValue<'a> for String {
    fn to_mdb_value(&'a self) -> MdbValue<'a> {
        unsafe {
            let t = self.as_slice();
            MdbValue::new(std::mem::transmute(t.as_ptr()), t.len())
        }
    }
}

impl<'a> ToMdbValue<'a> for bool {
    fn to_mdb_value(&'a self) -> MdbValue<'a> {
        unsafe {
            MdbValue::new(std::mem::transmute(self), std::mem::size_of::<bool>())
        }
    }
}

/*
impl<'a> ToMdbValue<'a> for &'a str {
    fn to_mdb_value<'a>(&'a self) -> MdbValue<'a> {
        unsafe {
            MdbValue::new(std::mem::transmute(self.as_ptr()),
                self.len())
        }
    }
}
*/

impl<'a> ToMdbValue<'a> for MDB_val {
    fn to_mdb_value(&'a self) -> MdbValue<'a> {
        MdbValue::new((*self).mv_data, (*self).mv_size as uint)
    }
}

impl<'a> ToMdbValue<'a> for MdbValue<'a> {
    fn to_mdb_value(&'a self) -> MdbValue<'a> {
        *self
    }
}


impl<'a> FromMdbValue<'a> for String {
    fn from_mdb_value(value: &'a MdbValue<'a>) -> String {
        unsafe {
            string::raw::from_buf_len(std::mem::transmute(value.value.mv_data),
                                      value.value.mv_size as uint).to_string()
        }
    }
}

impl<'a> FromMdbValue<'a> for () {
    fn from_mdb_value(_: &'a MdbValue<'a>) {
    }
}

impl<'a> FromMdbValue<'a> for &'a str {
    fn from_mdb_value(value: &'a MdbValue<'a>) -> &'a str {
        unsafe {
            std::mem::transmute(std::raw::Slice {
                data: value.value.mv_data,
                len: value.value.mv_size as uint,
            })
        }
    }
}
