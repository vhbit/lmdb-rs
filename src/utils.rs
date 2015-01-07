use libc::c_int;
use std::ffi::c_str_to_bytes;

use ffi::mdb_strerror;

pub fn error_msg(code: c_int) -> String {
    unsafe {
        String::from_utf8(c_str_to_bytes(&mdb_strerror(code)).to_vec()).unwrap()
    }
}
