use base;
use base::{MDBResult};
use base::{_DB, _Cursor, _Txn, FlagsFor, TypeKeeper, _TypeKeeper};
use libc::c_uint;
use mdb::consts::*;

trait _RW {
}

trait _RO {
}

impl _RW for base::Database {
}

trait ROEnv {
    fn get_or_create_ro_db<T: RODB_>(&mut self, name: &str) -> MDBResult<T>;
    fn get_ro_default_db<T: RODB_>(&mut self) -> MDBResult<T>;
}

trait RWEnv: ROEnv {
    fn get_or_create_rw_db<T: _DB>(&mut self, name: &str) -> MDBResult<T>;
    fn get_rw_default_db<T: _DB>(&mut self) -> MDBResult<T>;
}

impl ROEnv for base::Environment {
    pub fn get_or_create_ro_db<T: RODB_>(&mut self, name: &str) -> MDBResult<T> {
        self.get_or_create_db(name, 0)
    }

    pub fn get_ro_default_db<T: RODB_>(&mut self) -> MDBResult<T> {
        self.get_default_db(0)
    }
}

impl RWEnv for base::Environment {
    pub fn get_or_create_rw_db<T: _DB>(&mut self, name: &str) -> MDBResult<T> {
        self.get_or_create_db(name, 0)
    }

    pub fn get_rw_default_db<T: _DB>(&mut self) -> MDBResult<T> {
        self.get_default_db(0)
    }
}

pub fn new_rw_env() -> ~RWEnv {
    let e = ~base::Environment::new().unwrap();
    e as ~RWEnv
}

pub fn new_ro_env() -> ~ROEnv {
    let e = ~base::Environment::new().unwrap();
    e as ~ROEnv
}

trait RODB_ : _DB {
    fn get<K, V>(&mut self, txn: &_Txn, key: &K) -> MDBResult<V>;
    //fn new_cursor();
}

impl RODB_ for base::Database {
    pub fn get<K, V>(&mut self, txn: &_Txn, key: &K) -> MDBResult<V> {
        self.get(txn, key)
    }
}

trait RWDB_ : RODB_ + _RW {
    fn set<K, V>(&mut self, txn: &_Txn, key: &K, value: &V) -> MDBResult<()>;
    fn upsert<K, V>(&mut self, txn: &_Txn, key: &K, value: &V) -> MDBResult<Option<V>>;
    fn del<K, V>(&mut self, txn: &_Txn, key: &K) -> MDBResult<()>;
    //fn new_cursor();
}

impl RWDB_ for base::Database {
    pub fn set<K, V>(&mut self, txn: &_Txn, key: &K, value: &V) -> MDBResult<()> {
        self._put(txn, key, value, 0)
    }

    pub fn upsert<K, V>(&mut self, txn: &_Txn, key: &K, value: &V) -> MDBResult<Option<V>> {
        self._put_copy_value(txn, key, value, MDB_NOOVERWRITE)
    }

    pub fn del<K, V>(&mut self, txn: &_Txn, key: &K) -> MDBResult<()> {
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
    pub fn insert<K, V>(&mut self, txn: &_Txn, key: &K, value: &V) -> MDBResult<()> {
        self._put(txn, key, value, 0)
    }

    pub fn upsert<K, V>(&mut self, txn: &_Txn, key: &K, value: &V) -> MDBResult<Option<V>> {
        self._put_copy_value(txn, key, value, MDB_NOOVERWRITE)
    }

    pub fn del<K, V>(&mut self, txn: &_Txn, key: &K) -> MDBResult<()> {
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

        let ro_db: base::Database = ro_env.get_ro_default_db().unwrap();
        let rw_db = rw_env.get_rw_default_db().unwrap();
        let ro_db = rw_env.get_ro_default_db().unwrap();
    }
}
