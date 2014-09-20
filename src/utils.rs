use std::string;
use libc::c_int;
use ffi::funcs::mdb_strerror;

pub fn error_msg(code: c_int) -> String {
    unsafe {
        string::raw::from_buf(mdb_strerror(code) as *const u8)
    }
}
