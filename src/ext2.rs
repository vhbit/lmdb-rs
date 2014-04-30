use base::{Environment, Database, NativeTransaction, Cursor};
use base::{InnerEnv, InnerDb, InnerTxn, InnerCursor};
use base::{MDBResult};
use traits::{MDBIncomingValue, MDBOutgoingValue};

use libc::{c_uint, c_int, size_t};
use mdb::types::{mdb_filehandle_t, MDB_stat, MDB_envinfo};
use mdb::consts::*;

use std;

macro_rules! impl_inner(
    ($struct_name:ident, $trait_name:ident, $inner_name: ident) => (
        impl $trait_name for $struct_name {
            fn inner<'a>(&'a self) -> &'a $inner_name {
                &self.inner
            }

            fn inner_mut<'a>(&'a mut self) -> &'a mut $inner_name {
                &mut self.inner
            }

            fn new_with(inner: $inner_name) -> $struct_name {
                $struct_name {
                    inner: inner
                }
            }
        }
    )
)

macro_rules! mixin(
    ($t:ident, $tt:ident) => (
            impl $tt for $t {}
        )
)

macro_rules! wrap_into(
    ($container:ident, $t:ident) => (
        pub struct $container {
            inner: $t
        }
    )
)

macro_rules! ObjectFacet(
    ($container:ident, $wrapper:ident, $inner:ident, $($t:ident),*) => (
        wrap_into!($container, $inner)
        impl_inner!($container, $wrapper, $inner)
        $(mixin!($container, $t))*
    )
)

ObjectFacet!(ReadonlyEnv, InnerEnv, Environment, ROEnv)
ObjectFacet!(ReadwriteEnv, InnerEnv, Environment, RWEnv, ROEnv)

// DBs
ObjectFacet!(ReadonlyDB, InnerDb, Database, RODB, AnyDB)
ObjectFacet!(ReadwriteDB, InnerDb, Database, RWDB, AnyDB)

ObjectFacet!(ReadonlyDupDB, InnerDb, Database, RODupDB, AnyDupDB)
ObjectFacet!(ReadwriteDupDB, InnerDb, Database, RWDupDB, AnyDupDB)

/*
ObjectFacet!(ReadonlyDupFixedDB, InnerDb, Database, RODupFixedDB, AnyDupFixedDB)
ObjectFacet!(ReadwriteDupFixedDB, InnerDb, Database, RWDupFixedDB, AnyDupFixedDB)
*/

// Txns
ObjectFacet!(ReadonlyTxn, InnerTxn, NativeTransaction, ROTxn, AnyTxn)
ObjectFacet!(ReadwriteTxn, InnerTxn, NativeTransaction, RWTxn, AnyTxn)

// Cursors
ObjectFacet!(ReadonlyCursor, InnerCursor, Cursor, ROCursor)
ObjectFacet!(ReadwriteCursor, InnerCursor, Cursor, RWCursor, ROCursor)


ObjectFacet!(ReadonlyDupCursor, InnerCursor, Cursor, RODupCursor)
ObjectFacet!(ReadwriteDupCursor, InnerCursor, Cursor, RWDupCursor, RODupCursor)

/*
ObjectFacet!(ReadonlyDupFixedCursor, InnerCursor, Cursor, RODupFixedCursor)
ObjectFacet!(ReadwriteDupFixedCursor, InnerCursor, Cursor, RWDupFixedCursor, RODupFixedCursor)
*/

pub trait ROEnv: InnerEnv {
    fn stat(&self) -> MDBResult<MDB_stat> {
        self.inner().stat()
    }

    fn info(&self) -> MDBResult<MDB_envinfo> {
        self.inner().info()
    }

    fn set_flags(&mut self, flags: c_uint, turn_on: bool) -> MDBResult<()> {
        self.inner().set_flags(flags, turn_on)
    }

    fn get_flags(&self) -> MDBResult<c_uint> {
        self.inner().get_flags()
    }

    /// Returns a copy of database path, if it was opened successfully
    fn get_path(&self) -> Option<~Path> {
        self.inner().get_path()
    }

    fn set_mapsize(&mut self, size: size_t) -> MDBResult<()> {
        self.inner().set_mapsize(size)
    }

    fn set_maxreaders(&mut self, max_readers: c_uint) -> MDBResult<()> {
        self.inner().set_maxreaders(max_readers)
    }

    fn get_maxreaders(&self) -> MDBResult<c_uint> {
        self.inner().get_maxreaders()
    }

    /// Sets number of max DBs open. Should be called before open.
    fn set_maxdbs(&mut self, max_dbs: c_uint) -> MDBResult<()> {
        self.inner().set_maxdbs(max_dbs)
    }

    fn get_maxkeysize(&self) -> c_int {
        self.inner().get_maxkeysize()
    }

    /// Creates a backup copy in specified file descriptor
    fn copy_to_fd(&self, fd: mdb_filehandle_t) -> MDBResult<()> {
        self.inner().copy_to_fd(fd)
    }

    /// Gets file descriptor of this environment
    fn get_fd(&self) -> MDBResult<mdb_filehandle_t> {
        self.inner().get_fd()
    }

    /// Creates a backup copy in specified path
    // FIXME: check who is responsible for creating path: callee or caller
    fn copy_to_path(&self, path: &Path) -> MDBResult<()> {
        self.inner().copy_to_path(path)
    }

    fn new_ro_transaction<T: InnerTxn>(&self, parent: Option<&T>) -> MDBResult<ReadonlyTxn> {
        self.inner().create_transaction(parent.map(|t| t.inner()), MDB_RDONLY)
            .and_then(|t| Ok(InnerTxn::new_with(t)))
    }

    /// Returns or creates database with name
    ///
    /// Note: set_maxdbis should be called before
    fn get_or_create_ro_db<T: RODB>(&mut self, name: &str, flags: c_uint) -> MDBResult<T> {
        self.inner().get_or_create_db(name, flags)
    }

    /// Returns default database
    fn get_default_ro_db<T: RODB>(&mut self, flags: c_uint) -> MDBResult<T> {
        self.inner().get_default_db(flags)
    }
}

pub trait RWEnv: ROEnv {
    /// Sync environment to disk
    fn sync(&mut self, force: bool) -> MDBResult<()> {
        self.inner().sync(force)
    }    

    /// Returns or creates database with name
    ///
    /// Note: set_maxdbis should be called before
    fn get_or_create_rw_db<T: RWDB>(&mut self, name: &str, flags: c_uint) -> MDBResult<T> {
        self.inner().get_or_create_db(name, flags)
    }

    /// Returns default database
    fn get_default_rw_db<T: RWDB>(&mut self, flags: c_uint) -> MDBResult<T> {
        self.inner().get_default_db(flags)
    }

    fn new_rw_transaction<T: RWTxn>(&self, parent: Option<&T>) -> MDBResult<ReadwriteTxn> {
        self.inner().create_transaction(parent.map(|t| t.inner()), 0)
            .and_then(|txn| Ok(InnerTxn::new_with(txn)))
    }    
}

pub trait AnyDB: InnerDb {
    fn get<V: MDBOutgoingValue>(&self, txn: &InnerTxn, key: &MDBIncomingValue) -> MDBResult<V> {
        txn.inner().get(self.inner(), key)
    }
    
    fn new_ro_cursor<T: InnerTxn>(&self, txn: &T) -> MDBResult<ReadonlyCursor> {
        self.inner().new_cursor(txn) 
    }    
}

pub trait RODB: AnyDB {
}

pub trait RWDB: AnyDB {
    /// Set (with overwrite)
    fn set(&mut self, txn: &InnerTxn, key: &MDBIncomingValue, value: &MDBIncomingValue) -> MDBResult<()> {        
        txn.inner().set(self.inner(), key, value)
    }

    // FIXME: provide additional flag
    fn set_nooverwrite(&mut self, txn: &InnerTxn, key: &MDBIncomingValue, value: &MDBIncomingValue) -> MDBResult<()> {
        txn.inner().set(self.inner(), key, value)
    }

    fn del<T: MDBIncomingValue>(&mut self, txn: &InnerTxn, key: &MDBIncomingValue) -> MDBResult<()> {
        txn.inner().del(self.inner(), key)
    }

    fn new_rw_cursor<T: InnerTxn>(&self, txn: &T) -> MDBResult<ReadwriteCursor> {
        self.inner().new_cursor(txn)
    }
}

pub trait AnyDupDB: InnerDb {
    /// Returns first available item for key
    /// For accessing others cursor should be used
    // FIXME: provide lightweight API for accessing other items
    fn get_first<V: MDBOutgoingValue>(&self, txn: &InnerTxn, key: &MDBIncomingValue) -> MDBResult<V> {
        txn.inner().get(self.inner(), key)
    }
    
    fn new_ro_cursor<T: InnerTxn>(&self, txn: &T) -> MDBResult<ReadonlyDupCursor> {
        self.inner().new_cursor(txn) 
    }    
}

pub trait RODupDB: AnyDupDB {
}

pub trait RWDupDB: AnyDupDB {
    fn append(&mut self, txn: &InnerTxn, key: &MDBIncomingValue, value: &MDBIncomingValue) -> MDBResult<()> {        
        txn.inner().set(self.inner(), key, value)
    }

    // FIXME: provide additional flag
    fn set_nooverwrite(&mut self, txn: &InnerTxn, key: &MDBIncomingValue, value: &MDBIncomingValue) -> MDBResult<()> {
        txn.inner().set(self.inner(), key, value)
    }

    fn del_all<T: MDBIncomingValue>(&mut self, txn: &InnerTxn, key: &MDBIncomingValue) -> MDBResult<()> {
        txn.inner().del(self.inner(), key)
    }

    fn del_exact<T: MDBIncomingValue>(&mut self, txn: &InnerTxn, key: &MDBIncomingValue, value: &MDBIncomingValue) -> MDBResult<()> {
        txn.inner().del_exact_value(self.inner(), key, value)
    }

    fn new_rw_cursor<T: RWTxn>(&mut self, txn: &T) -> MDBResult<ReadwriteDupCursor> {
        self.inner().new_cursor(txn)
    }
}

/*
pub trait AnyDupFixedDB: InnerDb {
    /// Returns first available item for key
    /// For accessing others cursor should be used
    // FIXME: provide lightweight API for accessing other items
    fn get_first<V: MDBOutgoingValue>(&self, txn: &InnerTxn, key: &MDBIncomingValue) -> MDBResult<V> {
        txn.inner().get(self.inner(), key)
    }
    
    fn new_ro_cursor<T: InnerTxn>(&self, txn: &T) -> MDBResult<ReadonlyDupFixedCursor> {
        self.inner().new_cursor(txn) 
    }    
}

pub trait RODupFixedDB: AnyDupFixedDB {
}

pub trait RWDupFixedDB: AnyDupFixedDB {

}
*/

pub trait AnyTxn: InnerTxn {
    fn new_ro_txn(&self, flags: c_uint) -> MDBResult<ReadonlyTxn> {
        self.inner().create_child(flags)
            .and_then(|txn| Ok(InnerTxn::new_with(txn)))
    }

    fn abort(&mut self) {
        self.inner_mut().abort()
    }    
}

pub trait ROTxn: AnyTxn {
    fn renew(&mut self) -> MDBResult<()> {
        self.inner_mut().renew()
    }

    fn reset(&mut self) {
        self.inner_mut().reset()
    }
}

pub trait RWTxn: AnyTxn {
    fn commit(&mut self) -> MDBResult<()> {
        self.inner_mut().commit()
    }

    fn new_rw_txn(&mut self, flags: c_uint) -> MDBResult<ReadwriteTxn> {
        self.inner().create_child(flags)
            .and_then(|txn| Ok(InnerTxn::new_with(txn)))
    }
}

trait ROCursor: InnerCursor {

}

trait RWCursor: ROCursor {

}

trait RODupCursor: InnerCursor {

}

trait RWDupCursor: RODupCursor {

}

trait RODupFixedCursor: InnerCursor {

}

trait RWDupFixedCursor: RODupFixedCursor {

}

#[cfg(test)]

pub mod test {
    use super::*;
    use base::*;

    #[test]
    fn test1() {
        let env = Environment::new().unwrap();
        let mut renv: ReadonlyEnv = InnerEnv::new_with(env);
        let db: ReadonlyDB = renv.get_default_ro_db(0).unwrap();        
    }
}

/*
impl ReadonlyEnv {
    pub fn new() -> ReadonlyEnv {
        ReadonlyEnv {
            inner: Env {
                a: 3
            }
        }
    }
}

impl ReadwriteEnv {
    pub fn new() -> ReadwriteEnv {
        ReadwriteEnv {
            inner: Env {
                a: 4
            }
        }
    }
}
*/

/*
fn main() {
    let ro = ReadonlyEnv::new();    
    let rw = ReadwriteEnv::new();

    let a = ro.test2::<int>();
    let b = rw.test2::<uint>();
    let c = rw.test1::<int>();

    println!("Ro test2: {}, RW test2: {}, RW test1: {}", a, b, c);
}
*/

