use std;
use std::string;
use libc::size_t;
use ffi::types::MDB_val;

pub trait ToMdbValue {
    fn to_mdb_value(&self) -> MDB_val;
}

pub trait FromMdbValue {
    fn from_mdb_value(value: &MDB_val) -> Self;
}

impl ToMdbValue for Vec<u8> {
    fn to_mdb_value(&self) -> MDB_val {
        unsafe {
            MDB_val {
                mv_data: std::mem::transmute(self.as_ptr()),
                mv_size: self.len() as size_t
            }
        }
    }
}

impl FromMdbValue for Vec<u8> {
    fn from_mdb_value(value: &MDB_val) -> Vec<u8> {
        unsafe {
            std::vec::raw::from_buf(std::mem::transmute(value.mv_data), value.mv_size as uint)
        }
    }
}

impl ToMdbValue for String {
    fn to_mdb_value(&self) -> MDB_val {
        unsafe {
            let t = self.as_slice();
            MDB_val {
                mv_data: std::mem::transmute(t.as_ptr()),
                mv_size: t.len() as size_t
            }
        }
    }
}

/*
impl<'a> ToMdbValue for &'a str {
    fn to_mdb_value<'a>(&'a self) -> MDB_val {
        unsafe {
            MDB_val {
                mv_data: std::mem::transmute(self.as_ptr()),
                mv_size: self.len() as size_t
            }
        }
    }
}
*/

impl ToMdbValue for MDB_val {
    fn to_mdb_value(&self) -> MDB_val {
        MDB_val {
            mv_data: (*self).mv_data,
            mv_size: (*self).mv_size
        }
    }
}

impl FromMdbValue for String {
    fn from_mdb_value(value: &MDB_val) -> String {
        unsafe {
            string::raw::from_buf_len(std::mem::transmute(value.mv_data), 
                                      value.mv_size as uint).to_string()
        }
    }
}

impl FromMdbValue for () {
    fn from_mdb_value(_: &MDB_val) {
    }
}


/*
impl<'a> FromMdbValue for &'a str {
    fn from_mdb_value(value: &MDB_val) -> &'a str {
        unsafe {
            std::mem::transmute(std::raw::Slice {
                data: value.mv_data,
                len: value.mv_size as uint,
            })
        }
    }
}
*/
