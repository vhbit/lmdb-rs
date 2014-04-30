use base;
use base::{MDBResult, MDBError};
use base::{_DB, _Cursor, _Txn, FlagsFor, TypeKeeper, _TypeKeeper};
use libc::c_uint;
use mdb::consts::*;
use mdb::types::MDB_dbi;

trait _RW {
}

trait _RO {
}

impl _RW for base::Database {
}

trait ROEnv<T: RODB_> {
    fn get_or_create_ro_db(&mut self, name: &str) -> MDBResult<T>;
    fn get_ro_default_db(&mut self) -> MDBResult<~RODB_>;
}

trait RWEnv<T: _DB>: ROEnv<T> {
    fn get_or_create_rw_db(&mut self, name: &str) -> MDBResult<T>;
    fn get_rw_default_db(&mut self) -> MDBResult<T>;
}

impl<RODB_> ROEnv<RODB_> for base::Environment {
    fn get_or_create_ro_db(&mut self, name: &str) -> MDBResult<~RODB_> {
        //self.get_or_create_db::<RODB_>(name, 0)
        Err(MDBError::new_state_error(~""))
    }

    fn get_ro_default_db(&mut self) -> MDBResult<~RODB_> {
        self.get_default_db(0)
    }
}

impl<T: RODB_> RWEnv<T> for base::Environment {
    fn get_or_create_rw_db(&mut self, name: &str) -> MDBResult<T> {
        self.get_or_create_db::<T>(name, 0)
    }

    fn get_rw_default_db(&mut self) -> MDBResult<T> {
        self.get_default_db(0)
    }
}

pub fn new_rw_env<T:RODB_>() -> ~RWEnv<T> {
    let e = ~base::Environment::new().unwrap();
    e as ~RWEnv<T>
}

pub fn new_ro_env<T:RODB_>() -> ~ROEnv<T> {
    let e = ~base::Environment::new().unwrap();
    e as ~ROEnv<T>
}

trait RODB_ : _DB {
    fn get<K, V>(&mut self, txn: &_Txn, key: &K) -> MDBResult<V>;
    //fn new_cursor();
}

impl RODB_ for base::Database {
    fn get<K, V>(&mut self, txn: &_Txn, key: &K) -> MDBResult<V> {
        self.get(txn, key)
    }

    fn set_handle(&mut self, value: MDB_dbi) {
        self.handle = value;
    }
}

trait RWDB_ : RODB_ + _RW {
    fn set<K, V>(&mut self, txn: &_Txn, key: &K, value: &V) -> MDBResult<()>;
    fn upsert<K, V>(&mut self, txn: &_Txn, key: &K, value: &V) -> MDBResult<Option<V>>;
    fn del<K, V>(&mut self, txn: &_Txn, key: &K) -> MDBResult<()>;
    //fn new_cursor();
}

impl RWDB_ for base::Database {
    fn set<K, V>(&mut self, txn: &_Txn, key: &K, value: &V) -> MDBResult<()> {
        self._put(txn, key, value, 0)
    }

    fn upsert<K, V>(&mut self, txn: &_Txn, key: &K, value: &V) -> MDBResult<Option<V>> {
        self._put_copy_value(txn, key, value, MDB_NOOVERWRITE)
    }

    fn del<K, V>(&mut self, txn: &_Txn, key: &K) -> MDBResult<()> {
        self._del(txn, key, None::<&V>)
    }
}

trait RWDBDup_ : RODB_ + _RW {
    fn insert<K, V>(&mut self, txn: &_Txn, key: &K, value: &V) -> MDBResult<()>;
    fn upsert<K, V>(&mut self, txn: &_Txn, key: &K, value: &V) -> MDBResult<Option<V>>;
    fn del<K, V>(&mut self, txn: &_Txn, key: &K) -> MDBResult<()>;
    //fn new_cursor();
}

impl RWDBDup_ for base::Database {
    fn insert<K, V>(&mut self, txn: &_Txn, key: &K, value: &V) -> MDBResult<()> {
        self._put(txn, key, value, 0)
    }

    fn upsert<K, V>(&mut self, txn: &_Txn, key: &K, value: &V) -> MDBResult<Option<V>> {
        self._put_copy_value(txn, key, value, MDB_NOOVERWRITE)
    }

    fn del<K, V>(&mut self, txn: &_Txn, key: &K) -> MDBResult<()> {
        self._del(txn, key, None::<&V>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base;

    #[test]
    fn test_dbs() {
        let mut ro_env = new_ro_env();
        let mut rw_env = new_rw_env();

        let ro_db = ro_env.get_ro_default_db().unwrap();
        let rw_db: base::Database = rw_env.get_rw_default_db().unwrap();
        let ro_db: base::Database = rw_env.get_ro_default_db().unwrap();
    }
}
