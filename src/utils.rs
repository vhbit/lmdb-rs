use libc::c_int;
use liblmdb_sys::mdb_strerror;
use std::ffi::CStr;

pub fn error_msg(code: c_int) -> String {
    unsafe { String::from_utf8(CStr::from_ptr(mdb_strerror(code)).to_bytes().to_vec()).unwrap() }
}
