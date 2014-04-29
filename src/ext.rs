use base;
use base::{MDBResult};
use base::{_DB, _Cursor, _Txn, FlagsFor, TypeKeeper, _TypeKeeper};
use libc::c_uint;
use mdb::consts::*;

trait _RW {
}

trait _RO {
}

pub struct ReadonlyEnv {
    native: base::Environment,
}

impl ReadonlyEnv {
    pub fn get_or_create_ro_db<C: _Cursor, T: _DB<C>+_RO>(&mut self, name: &str) -> MDBResult<T> {
        self.native.get_or_create_db(name, 0)
    }

    pub fn get_ro_default_db<C: _Cursor, T: _DB<C>+_RO>(&mut self) -> MDBResult<T> {
        self.native.get_default_db(0)
    }
}


pub struct Env {
    native: base::Environment,
}

impl Env {
    pub fn get_or_create_rw_db<C: _Cursor, T: _DB<C>+_RW>(&mut self, name: &str) -> MDBResult<T> {
        self.native.get_or_create_db(name, 0)
    }

    pub fn get_rw_default_db<C: _Cursor, T: _DB<C>+_RW>(&mut self) -> MDBResult<T> {
        self.native.get_default_db(0)
    }

    pub fn get_or_create_ro_db<C: _Cursor, T: _DB<C>+_RO>(&mut self, name: &str) -> MDBResult<T> {
        self.native.get_or_create_db(name, 0)
    }

    pub fn get_ro_default_db<C: _Cursor, T: _DB<C>+_RO>(&mut self) -> MDBResult<T> {
        self.native.get_default_db(0)
    }
}

pub struct RODB<T> {
    native: base::Database,
}

impl<C: _Cursor + _RO> _DB<C> for RODB<C> {
    fn new_with_native(db: base::Database) -> RODB<C> {
        RODB {
            native: db,
        }
    }

    #[inline]
    fn inner<'a>(&'a mut self) -> &'a base::Database {
        &self.native
    }

    /*
    pub fn get<K, V>(&mut self, txn: &_RO, key: &K) -> MDBResult<V> {
        self.native.get(txn, key)
    }
    */
}

impl<T: _Cursor + FlagsFor> FlagsFor for RODB<T> {
    fn flags(_: TypeKeeper<RODB<T>>) -> c_uint {
        MDB_RDONLY + FlagsFor::flags(_TypeKeeper::<T>)
    }
}

impl<T> _RO for RODB<T> {
}

pub struct DB<C> {
    native: base::Database,
}

impl<T> _RW for DB<T> {
}

impl<C: _Cursor+_RW> _DB<C> for DB<C> {
    fn new_with_native(db: base::Database) -> DB<C> {
        DB {
            native: db,
        }
    }

    #[inline]
    fn inner<'a>(&'a mut self) -> &'a base::Database {
        &self.native
    }
    /*
    pub fn get<K, V>(&mut self, txn: &_Txn, key: &K) -> MDBResult<V> {
        self.native.get(txn, key)
    }

    pub fn set<K, V>(&mut self, txn: &_RW, key: &K, value: &V) -> MDBResult<()> {
        self.native.put(txn, key, value, 0)
    }

    pub fn upsert<K, V>(&mut self, txn: &_RW, key: &K, value: &V) -> MDBResult<Option<V>> {
        self.native.put_copy_value(txn, key, value, MDB_NOOVERWRITE)
    }

    pub fn del<K>(&mut self, txn: &_RW, key: &K) -> MDBResult<()> {
        self.native.del(txn, key, None)
    }
    */
}

impl<T: _Cursor + FlagsFor> FlagsFor for DB<T> {
    fn flags(_: TypeKeeper<DB<T>>) -> c_uint {
        FlagsFor::flags(_TypeKeeper::<T>)
    }
}

pub struct ROCursor {
    inner: base::Cursor
}

impl _Cursor for ROCursor {
    fn new_with_native(cursor: base::Cursor) -> ROCursor {
        ROCursor {
            inner: cursor,
        }
    }
}

impl ROCursor {
    pub fn get()
}

pub struct Cursor {
    inner: base::Cursor,
}

impl _Cursor for Cursor {
    fn new_with_native(cursor: base::Cursor) -> Cursor {
        Cursor {
            inner: cursor
        }
    }
}

pub struct DuplicateKeysCursor {
    inner: base::Cursor,
}

impl _Cursor for DuplicateKeysCursor {
    fn new_with_native(cursor: base::Cursor) -> DuplicateKeysCursor {
        DuplicateKeysCursor {
            inner: cursor
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base;

    #[test]
    fn test_dbs() {
        let mut ro_env = ReadonlyEnv {
            native:  base::Environment::new().unwrap()
        };
        let mut rw_env = Env {
            native:  base::Environment::new().unwrap()
        };

        let ro_db: RODB = ro_env.get_ro_default_db().unwrap();
        let rw_db: DB = rw_env.get_rw_default_db().unwrap();
        let ro_db: RODB = rw_env.get_ro_default_db().unwrap();
    }
}
