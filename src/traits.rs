use std;
use libc::size_t;
use mdb::types::MDB_val;

pub trait MDBIncomingValue {
    fn to_mdb_value(&self) -> MDB_val;
}

pub trait MDBOutgoingValue {
    fn from_mdb_value(value: &MDB_val) -> Self;
}

impl MDBIncomingValue for ~[u8] {
    fn to_mdb_value(&self) -> MDB_val {
        unsafe {
            MDB_val {
                mv_data: std::cast::transmute(self.as_ptr()),
                mv_size: self.len() as size_t
            }
        }
    }
}

impl MDBOutgoingValue for ~[u8] {
    fn from_mdb_value(value: &MDB_val) -> ~[u8] {
        unsafe {
            std::slice::raw::from_buf_raw(value.mv_data as *u8, value.mv_size as uint)
        }
    }
}

impl MDBIncomingValue for ~str {
    fn to_mdb_value(&self) -> MDB_val {
        unsafe {
            let t = self.as_slice();
            MDB_val {
                mv_data: std::cast::transmute(t.as_ptr()),
                mv_size: t.len() as size_t
            }
        }
    }
}

impl MDBOutgoingValue for ~str {
    fn from_mdb_value(value: &MDB_val) -> ~str {
        unsafe {
            std::str::raw::from_buf_len(std::cast::transmute(value.mv_data), value.mv_size as uint)
        }
    }
}

impl MDBOutgoingValue for () {
    fn from_mdb_value(_: &MDB_val) {
    }
}


pub trait StateError {
    fn new_state_error(msg: ~str) -> Self;
}