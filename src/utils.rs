use libc::c_int;
use std::ffi::{CStr};
use std::mem;
use super::{FromMdbValue, MdbValue};
use ffi::MDB_val;
use ffi::mdb_strerror;

pub fn error_msg(code: c_int) -> String {
    unsafe {
        String::from_utf8(CStr::from_ptr(mdb_strerror(code)).to_bytes().to_vec()).unwrap()
    }
}

pub extern "C" fn sort<T:FromMdbValue+Ord>(lhs_val: *const MDB_val, rhs_val: *const MDB_val) -> c_int {
    let lhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(lhs_val)});
    let rhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(rhs_val)});

    let order: i8 = unsafe { mem::transmute(lhs.cmp(&rhs)) };
    order as c_int
}

pub extern "C" fn sort_reverse<T:FromMdbValue+Ord>(lhs_val: *const MDB_val, rhs_val: *const MDB_val) -> c_int {
    let lhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(lhs_val)});
    let rhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(rhs_val)});
    let order: i8 = unsafe { mem::transmute(lhs.cmp(&rhs).reverse()) };
    order as c_int
}
