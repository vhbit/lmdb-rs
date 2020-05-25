use libc::c_int;
use std::ffi::{CStr};
use std::mem;
use super::{FromMdbValue, MdbValue};
use ffi::MDB_val;
use ffi::mdb_strerror;
use std::cmp::Ordering;

pub fn error_msg(code: c_int) -> String {
    unsafe {
        String::from_utf8(CStr::from_ptr(mdb_strerror(code)).to_bytes().to_vec()).unwrap()
    }
}

#[inline(always)]
fn order_to_c_int(ord: Ordering) -> c_int {
    let order: i8 = unsafe { mem::transmute(ord) };
    order as c_int
}

pub extern "C" fn sort<T:FromMdbValue+Ord>(lhs_val: *const MDB_val, rhs_val: *const MDB_val) -> c_int {
    let lhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(lhs_val)});
    let rhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(rhs_val)});
    order_to_c_int(lhs.cmp(&rhs))
}

pub extern "C" fn sort_reverse<T:FromMdbValue+Ord>(lhs_val: *const MDB_val, rhs_val: *const MDB_val) -> c_int {
    let lhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(lhs_val)});
    let rhs = T::from_mdb_value(&unsafe{MdbValue::from_raw(rhs_val)});
    order_to_c_int(lhs.cmp(&rhs).reverse())
}

#[test]
fn test_order() {
    assert!(order_to_c_int(Ordering::Less) < 0);
    assert!(order_to_c_int(Ordering::Equal) == 0);
    assert!(order_to_c_int(Ordering::Greater) > 0);
}
