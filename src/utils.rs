use std::string;
use libc::c_int;
use ffi::consts::MDB_SUCCESS;
use ffi::funcs::mdb_strerror;
use base::{MdbResult, MdbError};

pub fn error_msg(code: c_int) -> String {
    unsafe {
        string::raw::from_buf(mdb_strerror(code) as *const u8)
    }
}

#[inline]
pub fn lift<U>(code: c_int, res: || -> U) -> MdbResult<U> {
    match code {
        MDB_SUCCESS => Ok(res() ),
        _ => Err(MdbError::new_with_code(code))
    }
}

#[inline]
pub fn lift_noret(code: c_int) -> MdbResult<()> {
    match code {
        MDB_SUCCESS => Ok(()),
        _ => Err(MdbError::new_with_code(code))
    }
}
