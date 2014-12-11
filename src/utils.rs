use libc::c_int;
use ffi::mdb_strerror;

pub fn error_msg(code: c_int) -> String {
    unsafe {
        String::from_raw_buf(mdb_strerror(code) as *const u8)
    }
}
