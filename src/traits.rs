use std;
use libc::size_t;
use mdb::types::MDB_val;

pub trait MDBIncomingValue {
    fn to_mdb_value(&self) -> MDB_val;
}

pub trait MDBOutgoingValue {
    fn from_mdb_value(value: &MDB_val) -> Self;
}

impl MDBIncomingValue for Vec<u8> {
    fn to_mdb_value(&self) -> MDB_val {
        unsafe {
            MDB_val {
                mv_data: std::mem::transmute(self.as_ptr()),
                mv_size: self.len() as size_t
            }
        }
    }
}

impl MDBOutgoingValue for Vec<u8> {
    fn from_mdb_value(value: &MDB_val) -> Vec<u8> {
        unsafe {
            std::vec::raw::from_buf(std::mem::transmute(value.mv_data), value.mv_size as uint)
        }
    }
}

impl MDBIncomingValue for Box<String> {
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

impl MDBIncomingValue for String {
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

impl<'a> MDBIncomingValue for &'a str {
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

impl MDBIncomingValue for MDB_val {
    fn to_mdb_value(&self) -> MDB_val {
        MDB_val {
            mv_data: (*self).mv_data,
            mv_size: (*self).mv_size
        }
    }
}

impl MDBOutgoingValue for Box<String> {
    fn from_mdb_value(value: &MDB_val) -> Box<String> {
        unsafe {
            box std::str::raw::from_buf_len(std::mem::transmute(value.mv_data), value.mv_size as uint)
        }
    }
}

impl MDBOutgoingValue for String {
    fn from_mdb_value(value: &MDB_val) -> String {
        unsafe {
            std::str::raw::from_buf_len(std::mem::transmute(value.mv_data), value.mv_size as uint).to_string()
        }
    }
}

impl MDBOutgoingValue for () {
    fn from_mdb_value(_: &MDB_val) {
    }
}

pub trait StateError {
    fn new_state_error(msg: Box<String>) -> Self;
}
