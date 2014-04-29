use std::str;
use libc::c_int;
use mdb::consts::MDB_SUCCESS;
use mdb::funcs::mdb_strerror;
use base::{MDBResult, MDBError};

pub fn error_msg(code: c_int) -> ~str {
    unsafe {
        str::raw::from_c_str(mdb_strerror(code))
    }
}

#[inline]
pub fn lift<U>(code: c_int, res: || -> U) -> MDBResult<U> {
    match code {
        MDB_SUCCESS => Ok(res() ),
        _ => Err(MDBError::new_with_code(code))
    }
}

#[inline]
pub fn lift_noret(code: c_int) -> MDBResult<()> {
    match code {
        MDB_SUCCESS => Ok(()),
        _ => Err(MDBError::new_with_code(code))
    }
}
