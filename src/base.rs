use std;
use std::ptr;
use libc;
use libc::{c_int, c_uint, size_t, c_char};
use std::result::Result;
use std::default::Default;

use mdb::consts::*;
use mdb::funcs::*;
use mdb::types::*;

use traits::{MDBIncomingValue, MDBOutgoingValue};
use utils::{error_msg, lift, lift_noret};

/// MDBError wraps information about LMDB error
pub struct MDBError {
    code: c_int,
    message: ~str
}

impl MDBError {
    pub fn new_with_code(code: c_int) -> MDBError {
        MDBError {
            code: code,
            message: error_msg(code)
        }
    }

    pub fn new_state_error(msg: ~str) -> MDBError {
        MDBError {
            code: MDB_INVALID_STATE,
            message: msg
        }
    }
}

impl std::fmt::Show for MDBError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt.buf.write_str(self.message)
    }
}

pub type MDBResult<T> = Result<T, MDBError>;

pub enum TypeKeeper<T> {_TypeKeeper}

pub trait FlagsFor {
    fn flags(TypeKeeper<Self>) -> c_uint;
}

pub trait _Cursor {
    fn new_with_native(cursor: Cursor) -> Self;
}

pub trait _DB {
    fn set_handle(&mut self, handle: MDB_dbi)  -> Self;
    //fn new_with_native(db: Database) -> Self;

    /*
    fn inner<'a>(&'a mut self) -> &'a Database;

    fn new_cursor<T: _Cursor>(&mut self, txn: &_Txn) -> MDBResult<T> {
        self.inner().new_cursor(txn)
    }
    */
}

pub trait _Txn {
    fn handle(&self) -> *MDB_txn;
}

/// Database
pub struct Database {
    handle: MDB_dbi,
}

impl Database {
    fn new_with_handle<T: _DB>(handle: MDB_dbi) -> T {
        unsafe {
            let x: T = std::mem::init();
            x.set_handle(handle);
            x
        }
        //Database { handle: handle }
    }

    /*
    fn new_cursor<C: _Cursor>(&mut self, txn: &_Txn) -> MDBResult<C> {
        unsafe {
            let c: *MDB_cursor = std::ptr::RawPtr::null();
            lift(unsafe {mdb_cursor_open(txn.handle(), self.handle, &c)},
                 || _Cursor::new_with_native(Cursor::new_with_handle(c)))
        }
    }
     */

    fn _put<K, V>(&mut self, txn: &_Txn, key: &K, value: &V, flags: c_uint) -> MDBResult<()> {
        Err(MDBError::new_state_error(~""))
    }

    fn _put_copy_value<K, V>(&mut self, txn: &_Txn, key: &K, value: &V, flags: c_uint) -> MDBResult<Option<V>> {
        Err(MDBError::new_state_error(~""))
    }

    fn _del<K, V>(&mut self, txn: &_Txn, key: &K, value: Option<&V>) -> MDBResult<()> {
        Err(MDBError::new_state_error(~""))
    }
}

impl _DB for Database {
    fn set_handle(&mut self, value: MDB_dbi) -> Database {
        self.handle = value;
    }
}


/// Environment
pub struct Environment {
    env: *MDB_env,
    path: Option<~Path>
}

impl Environment {
    /// Initializes environment
    ///
    /// Note that for using named databases, it should be followed by set_maxdbs()
    /// before opening
    pub fn new() -> MDBResult<Environment> {
        let env: *MDB_env = ptr::RawPtr::null();
        lift(unsafe {
            let pEnv: **mut MDB_env = std::cast::transmute(&env);
            mdb_env_create(pEnv)},
             || Environment {env: env, path: None})

    }

    /// Opens environment
    ///
    /// flags bitwise ORed flags for environment, see
    /// [original documentation](http://symas.com/mdb/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340)
    ///
    /// mode is expected to be permissions on UNIX like systems and is ignored on Windows
    pub fn open(&mut self, path: &Path, flags: c_uint, mode: mdb_mode_t) -> MDBResult<()> {
        let res = unsafe {
            // There should be a directory before open
            let temp_res = match (path.exists(), path.is_dir()) {
                (false, _) => {
                    path.with_c_str(|c_path| {
                        libc::mkdir(c_path, mode)
                    })
                },
                (true, true) => MDB_SUCCESS,
                (true, false) => libc::EACCES,
            };

            match temp_res {
                MDB_SUCCESS => {
                    path.with_c_str(|c_path| {
                        mdb_env_open(self.env, c_path, flags, mode)
                    })
                },
                _ => temp_res
            }
        };

        match res {
            MDB_SUCCESS => {
                self.path = Some(~path.clone());
                Ok(())
            },
            _ => Err(MDBError::new_with_code(res)) // FIXME: if it fails, environment should be immediately destroyed
        }
    }

    pub fn stat(&self) -> MDBResult<MDB_stat> {
        let tmp: MDB_stat = unsafe { std::mem::init() };
        lift(unsafe { mdb_env_stat(self.env, &tmp)},
             || tmp)
    }

    pub fn info(&self) -> MDBResult<MDB_envinfo> {
        let tmp: MDB_envinfo = unsafe { std::mem::init() };
        lift(unsafe { mdb_env_info(self.env, &tmp)},
             || tmp)
    }

    /// Sync environment to disk
    pub fn sync(&mut self, force: bool) -> MDBResult<()> {
        lift_noret(unsafe { mdb_env_sync(self.env, if force {1} else {0})})
    }

    pub fn set_flags(&mut self, flags: c_uint, turn_on: bool) -> MDBResult<()> {
        lift_noret(unsafe {
            mdb_env_set_flags(self.env, flags, if turn_on {1} else {0})
        })
    }

    pub fn get_flags(&self) -> MDBResult<c_uint> {
        let flags = 0;
        lift(unsafe {mdb_env_get_flags(self.env, &flags)},
             || flags)
    }

    /// Returns a copy of database path, if it was opened successfully
    pub fn get_path(&self) -> Option<~Path> {
        match self.path {
            Some(ref p) => Some(p.clone()),
            _ => None
        }
    }

    pub fn set_mapsize(&mut self, size: size_t) -> MDBResult<()> {
        lift_noret(unsafe { mdb_env_set_mapsize(self.env, size)})
    }

    pub fn set_maxreaders(&mut self, max_readers: c_uint) -> MDBResult<()> {
        lift_noret(unsafe { mdb_env_set_maxreaders(self.env, max_readers)})
    }

    pub fn get_maxreaders(&self) -> MDBResult<c_uint> {
        let max_readers: c_uint = 0;
        lift(unsafe { mdb_env_get_maxreaders(self.env, &max_readers)},
             || max_readers )
    }

    /// Sets number of max DBs open. Should be called before open.
    pub fn set_maxdbs(&mut self, max_dbs: c_uint) -> MDBResult<()> {
        lift_noret(unsafe { mdb_env_set_maxdbs(self.env, max_dbs)})
    }

    pub fn get_maxkeysize(&self) -> c_int {
        unsafe { mdb_env_get_maxkeysize(self.env) }
    }

    /// Creates a backup copy in specified file descriptor
    pub fn copy_to_fd(&self, fd: mdb_filehandle_t) -> MDBResult<()> {
        lift_noret(unsafe { mdb_env_copyfd(self.env, fd) })
    }

    /// Gets file descriptor of this environment
    pub fn get_fd(&self) -> MDBResult<mdb_filehandle_t> {
        let fd = 0;
        lift({ unsafe { mdb_env_get_fd(self.env, &fd) }}, || fd)
    }

    /// Creates a backup copy in specified path
    // FIXME: check who is responsible for creating path: callee or caller
    pub fn copy_to_path(&self, path: &Path) -> MDBResult<()> {
        path.with_c_str(|c_path| unsafe {
            lift_noret(mdb_env_copy(self.env, c_path))
        })
    }

    fn create_transaction(&mut self, parent: Option<NativeTransaction>, flags: c_uint) -> MDBResult<NativeTransaction> {
        let handle: *MDB_txn = ptr::RawPtr::null();
        let parent_handle = match parent {
            Some(t) => t.handle,
            _ => ptr::RawPtr::<MDB_txn>::null()
        };

        lift(unsafe { mdb_txn_begin(self.env, parent_handle, flags, &handle) },
             || NativeTransaction::new_with_handle(handle))
    }

    /*
    /// Creates a new read-write transaction
    pub fn new_transaction(&mut self) -> MDBResult<Transaction> {
        self.create_transaction(None, 0)
            .and_then(|txn| Ok(Transaction::new_with_native(txn)))
    }

    /// Creates a readonly transaction
    pub fn new_readonly_transaction(&mut self) -> MDBResult<ReadonlyTransaction> {
        self.create_transaction(None, MDB_RDONLY)
            .and_then(|txn| Ok(ReadonlyTransaction::new_with_native(txn)))
    }
    */

    fn get_db_by_name<T: _DB>(&mut self, c_name: *c_char, flags: c_uint) -> MDBResult<T> {
        let dbi: MDB_dbi = 0;

        self.create_transaction(None, 0)
            .and_then(|txn| lift(unsafe { mdb_dbi_open(txn.handle, c_name, flags, &dbi)}, || txn) )
            .and_then(|mut t| t.commit() )
            .and_then(|_| Ok(Database::new_with_handle(dbi)))
    }

    /// Returns or creates database with name
    ///
    /// Note: set_maxdbis should be called before
    pub fn get_or_create_db<T: _DB>(&mut self, name: &str, flags: c_uint) -> MDBResult<T> {
        name.with_c_str(|c_name| {
            self.get_db_by_name(c_name, flags)
        })
    }

    /// Returns default database
    pub fn get_default_db<T: _DB>(&mut self, flags: c_uint) -> MDBResult<T> {
        self.get_db_by_name(std::ptr::RawPtr::null(), flags)
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        unsafe {
            mdb_env_close(self.env);
        }
    }
}

#[deriving(Show, Eq)]
enum TransactionState {
    TxnStateNormal,   // Normal, any operation possible
    TxnStateReleased, // Released (reset on readonly), has to be renewed
    TxnStateInvalid,  // Invalid, no further operation possible
}

struct NativeTransaction {
    handle: *MDB_txn,
    state: TransactionState,
}

impl NativeTransaction {
    fn new_with_handle(h: *MDB_txn) -> NativeTransaction {
        NativeTransaction { handle: h, state: TxnStateNormal }
    }

    /// Transactions are supposed to work in one thread anyway
    /// so state changes are lock free
    #[inline]
    fn change_state(&mut self, expected_state: TransactionState, op: |h: *MDB_txn| -> (TransactionState, c_int)) -> MDBResult<()> {
        if self.state != expected_state {
            let msg = format!("Invalid txn op in state {}", self.state);
            Err(MDBError::new_state_error(msg))
        } else {
            let (ns, res) = op(self.handle);
            match res {
                MDB_SUCCESS => {
                    self.state = ns;
                    Ok(Default::default())
                },
                _ => Err(MDBError::new_with_code(res))
            }
        }
    }

    fn commit(&mut self) -> MDBResult<()> {
        self.change_state(TxnStateNormal, |h: *MDB_txn| unsafe {
            (TxnStateInvalid, mdb_txn_commit(h))
        } )
    }

    #[allow(unused_must_use)]
    fn abort(&mut self) {
        self.change_state(TxnStateNormal, |h: *MDB_txn| unsafe {
            (TxnStateInvalid, { mdb_txn_abort(h); MDB_SUCCESS })
        });
    }

    /// Resets read only transaction, handle is kept. Must be followed
    /// by call to renew
    #[allow(unused_must_use)]
    fn reset(&mut self) {
        self.change_state(TxnStateNormal, |h: *MDB_txn| unsafe {
            (TxnStateReleased, { mdb_txn_reset(h); MDB_SUCCESS })
        });
    }

    /// Acquires a new reader lock after it was released by reset
    fn renew(&mut self) -> MDBResult<()> {
        self.change_state(TxnStateReleased, |h: *MDB_txn| unsafe {
            (TxnStateNormal, mdb_txn_renew(h))
        })
    }

    fn create_child(&mut self, flags: c_uint) -> MDBResult<NativeTransaction> {
        let out: *MDB_txn = ptr::RawPtr::null();
        lift(unsafe { mdb_txn_begin(mdb_txn_env(self.handle), self.handle, flags, &out) },
             || NativeTransaction::new_with_handle(out))
    }

    /// Used in Drop to switch state
    fn silent_abort(&mut self) {
        match self.state {
            TxnStateInvalid => (),
            _ => unsafe {
                mdb_txn_abort(self.handle);
                self.state = TxnStateInvalid;
            }
        }
    }

    #[inline]
    fn in_state<U>(&mut self, expected_state: TransactionState, p: |a: &mut NativeTransaction| -> MDBResult<U>) -> MDBResult<U> {
        if self.state != expected_state {
            Err(MDBError::new_state_error(format!("Unexpected state for transaction: {}", self.state)))
        } else {
            p(self)
        }
    }

    fn get_value<T: MDBOutgoingValue>(&self, db: &Database, key: &MDBIncomingValue) -> MDBResult<T> {
        unsafe {
            let key_val = key.to_mdb_value();
            let data_val: MDB_val = std::mem::init();

            lift(mdb_get(self.handle, db.handle, &key_val, &data_val),
                 || MDBOutgoingValue::from_mdb_value(&data_val))
        }
    }

    pub fn get<T: MDBOutgoingValue>(&mut self, db: &Database, key: &MDBIncomingValue) -> MDBResult<T> {
        self.in_state(TxnStateNormal,
                      |t| t.get_value(db, key))
    }

    fn set_value(&self, db: &Database, key: &MDBIncomingValue, value: &MDBIncomingValue) -> MDBResult<()> {
        self.set_value_with_flags(db, key, value, 0)
    }

    fn set_value_with_flags(&self, db: &Database, key: &MDBIncomingValue, value: &MDBIncomingValue, flags: c_uint) -> MDBResult<()> {
        unsafe {
            let key_val = key.to_mdb_value();
            let data_val = value.to_mdb_value();

            lift_noret(mdb_put(self.handle, db.handle, &key_val, &data_val, flags))
        }
    }

    /// Sets a new value for key, in case of enabled duplicates
    /// it actually appends a new value
    // FIXME: add explicit append function
    // FIXME: think about creating explicit separation of
    // all traits for databases with dup keys
    pub fn set(&mut self, db: &Database, key: &MDBIncomingValue, value: &MDBIncomingValue) -> MDBResult<()> {
        self.in_state(TxnStateNormal,
                      |t| t.set_value(db, key, value))
    }

    /// Deletes all values by key
    fn del_value(&mut self, db: &Database, key: &MDBIncomingValue) -> MDBResult<()> {
        unsafe {
            let key_val = key.to_mdb_value();
            lift_noret(mdb_del(self.handle, db.handle, &key_val, std::ptr::RawPtr::null()))
        }
    }

    /// If duplicate keys are allowed deletes value for key which is equal to data
    pub fn del_exact_value(&mut self, db: &Database, key: &MDBIncomingValue, data: &MDBIncomingValue) -> MDBResult<()> {
        self.in_state(TxnStateNormal,
                      |t| unsafe {
                          let key_val = key.to_mdb_value();
                          let data_val = data.to_mdb_value();

                          lift_noret(mdb_del(t.handle, db.handle, &key_val, &data_val))
                      })
    }

    /// Deletes all values for key
    pub fn del(&mut self, db: &Database, key: &MDBIncomingValue) -> MDBResult<()> {
        self.in_state(TxnStateNormal,
                      |t| t.del_value(db, key))
    }

    /// creates a new cursor in current transaction tied to db
    pub fn new_cursor(&mut self, db: &Database) -> MDBResult<Cursor> {
        Cursor::new(self, db)
    }
}

pub struct Transaction {
    inner: NativeTransaction,
}

pub struct ReadonlyTransaction {
    inner: NativeTransaction,
}

impl Transaction {
    fn new_with_native(txn: NativeTransaction) -> Transaction {
        Transaction {
            inner: txn
        }
    }

    pub fn create_child(&mut self) -> MDBResult<Transaction> {
        match self.inner.create_child(0) {
            Ok(txn) => Ok(Transaction::new_with_native(txn)),
            Err(e) => Err(e)
        }
    }

    pub fn create_ro_child(&mut self) -> MDBResult<ReadonlyTransaction> {
        match self.inner.create_child(MDB_RDONLY) {
            Ok(txn) => Ok(ReadonlyTransaction::new_with_native(txn)),
            Err(e) => Err(e)
        }
    }

    /// Aborts transaction, handle is freed
    pub fn commit(&mut self) -> MDBResult<()> {
        self.inner.commit()
    }

    /// Aborts transaction, handle is freed
    pub fn abort(&mut self) {
        self.inner.abort();
    }

    pub fn get<T: MDBOutgoingValue>(&mut self, db: &Database, key: &MDBIncomingValue) -> MDBResult<T> {
        self.inner.get(db, key)
    }

    pub fn set(&mut self, db: &Database, key: &MDBIncomingValue, value: &MDBIncomingValue) -> MDBResult<()> {
        self.inner.set(db, key, value)
    }

    pub fn del(&mut self, db: &Database, key: &MDBIncomingValue) -> MDBResult<()> {
        self.inner.del(db, key)
    }

    pub fn del_exact(&mut self, db: &Database, key: &MDBIncomingValue, data: &MDBIncomingValue) -> MDBResult<()> {
        self.inner.del_exact_value(db, key, data)
    }

    pub fn new_cursor(&mut self, db: &Database) -> MDBResult<Cursor> {
        self.inner.new_cursor(db)
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.silent_abort();
    }
}

impl ReadonlyTransaction {
    fn new_with_native(txn: NativeTransaction) -> ReadonlyTransaction {
        ReadonlyTransaction {
            inner: txn,
        }
    }

    pub fn create_ro_child(&mut self) -> MDBResult<ReadonlyTransaction> {
        match self.inner.create_child(MDB_RDONLY) {
            Ok(txn) => Ok(ReadonlyTransaction::new_with_native(txn)),
            Err(e) => Err(e)
        }
    }

    /// Aborts transaction, handle is freed
    pub fn abort(&mut self) {
        self.inner.abort();
    }

    /// Resets read only transaction, handle is kept. Must be followed
    /// by call to renew
    pub fn reset(&mut self) {
        self.inner.reset();
    }

    /// Acquires a new reader lock after it was released by reset
    pub fn renew(&mut self) -> MDBResult<()> {
        self.inner.renew()
    }

    pub fn get<T: MDBOutgoingValue>(&mut self, db: &Database, key: &MDBIncomingValue) -> MDBResult<T> {
        self.inner.get(db, key)
    }

    pub fn new_cursor(&mut self, db: &Database) -> MDBResult<Cursor> {
        self.inner.new_cursor(db)
    }
}

impl Drop for ReadonlyTransaction {
    fn drop(&mut self) {
        self.inner.silent_abort();
    }
}

pub struct Cursor {
    handle: *MDB_cursor,
}

impl Cursor {
    fn new_with_handle(handle: *MDB_cursor) -> Cursor {
        Cursor {handle: handle}
    }

    fn new(txn: &NativeTransaction, db: &Database) -> MDBResult<Cursor> {
        let tmp: *MDB_cursor = std::ptr::RawPtr::null();
        lift(unsafe { mdb_cursor_open(txn.handle, db.handle, &tmp) },
             || Cursor {handle: tmp })
    }

    fn move_to<'a>(&mut self, key: Option<&'a MDBIncomingValue>, op: MDB_cursor_op) -> MDBResult<()> {
        // Even if we don't ask for any data and want only to set a position
        // MDB still insists in writing back key and data to provided pointers
        // it's actually not that big deal, considering no actual data copy happens
        let data_val = unsafe {std::mem::init()};
        let key_val = match key {
            Some(k) => k.to_mdb_value(),
            _ => unsafe {std::mem::init()}
        };

        lift_noret(unsafe { mdb_cursor_get(self.handle, &key_val, &data_val, op) })
    }

    /// Moves cursor to first entry
    pub fn to_first(&mut self) -> MDBResult<()> {
        self.move_to(None, MDB_FIRST)
    }

    /// Moves cursor to last entry
    pub fn to_last(&mut self) -> MDBResult<()> {
        self.move_to(None, MDB_LAST)
    }

    /// Moves cursor to first entry for key if it exists
    pub fn to_key(&mut self, key: &MDBIncomingValue) -> MDBResult<()> {
        self.move_to(Some(key), MDB_SET)
    }

    /// Moves cursor to first entry for key greater than
    /// or equal to ke
    pub fn to_gte_key(&mut self, key: &MDBIncomingValue) -> MDBResult<()> {
        self.move_to(Some(key), MDB_SET_RANGE)
    }

    /// Moves cursor to next key, i.e. skip items
    /// with duplicate keys
    pub fn to_next_key(&mut self) -> MDBResult<()> {
        self.move_to(None, MDB_NEXT_NODUP)
    }

    /// Moves cursor to next item with the same key as current
    pub fn to_next_key_item(&mut self) -> MDBResult<()> {
        self.move_to(None, MDB_NEXT_DUP)
    }

    /// Moves cursor to prev entry, i.e. skips items
    /// with duplicate keys
    pub fn to_prev_key(&mut self) -> MDBResult<()> {
        self.move_to(None, MDB_PREV_NODUP)
    }

    /// Moves cursor to prev item with the same key as current
    pub fn to_prev_key_item(&mut self) -> MDBResult<()> {
        self.move_to(None, MDB_PREV_DUP)
    }

    /// Moves cursor to first item with the same key as current
    pub fn to_first_key_item(&mut self) -> MDBResult<()> {
        self.move_to(None, MDB_FIRST_DUP)
    }

    /// Moves cursor to last item with the same key as current
    pub fn to_last_key_item(&mut self) -> MDBResult<()> {
        self.move_to(None, MDB_LAST_DUP)
    }

    /// Retrieves current key/value as tuple
    pub fn get<T: MDBOutgoingValue, U: MDBOutgoingValue>(&mut self) -> MDBResult<(T, U)> {
        unsafe {
            let key_val: MDB_val = std::mem::init();
            let data_val: MDB_val = std::mem::init();
            lift(mdb_cursor_get(self.handle, &key_val, &data_val, MDB_GET_CURRENT),
                 || (MDBOutgoingValue::from_mdb_value(&key_val), MDBOutgoingValue::from_mdb_value(&data_val)))
        }
    }

    fn set_value<'a>(&mut self, key:Option<&'a MDBIncomingValue>, value: &MDBIncomingValue, flags: c_uint) -> MDBResult<()> {
        let data_val = value.to_mdb_value();
        let key_val = unsafe {
            match  key {
                Some(k) => k.to_mdb_value(),
                _ => std::mem::init()
            }
        };

        lift_noret(unsafe {mdb_cursor_put(self.handle, &key_val, &data_val, flags)})
    }

    /// Overwrites value for current item
    /// Note: overwrites max cur_value.len() bytes
    pub fn set(&mut self, value: &MDBIncomingValue) -> MDBResult<()> {
        self.set_value(None, value, MDB_CURRENT)
    }

    /// Adds a new value if it doesn't exist yet
    pub fn upsert(&mut self, key: &MDBIncomingValue, value: &MDBIncomingValue) -> MDBResult<()> {
        self.set_value(Some(key), value, MDB_NOOVERWRITE)
    }

    fn del_value(&mut self, flags: c_uint) -> MDBResult<()> {
        lift_noret(unsafe { mdb_cursor_del(self.handle, flags) })
    }

    /// Deletes only current item
    pub fn del_single(&mut self) -> MDBResult<()> {
        self.del_value(0)
    }

    /// Deletes all items with same key as current
    pub fn del_all(&mut self) -> MDBResult<()> {
        self.del_value(MDB_NODUPDATA)
    }

    /// Returns count of items with the same key as current
    pub fn item_count(&self) -> MDBResult<size_t> {
        let tmp: size_t = 0;
        lift(unsafe {mdb_cursor_count(self.handle, &tmp)},
             || tmp)
    }
}

impl Drop for Cursor {
    fn drop(&mut self) {
        unsafe { mdb_cursor_close(self.handle) };
    }
}

/*
#[cfg(test)]
mod test {
    use std::io::fs;
    use std::rt::unwind::Unwinder;
    use std::path::Path;

    use mdb::consts;
    use super::{Environment};

    #[allow(unused_must_use)]
    fn test_db_in_path(path: &Path, f: ||) {
        // Delete dir to be sure nothing existed before test
        if path.exists() {
            fs::rmdir_recursive(path);
        };

        let mut unwinder = Unwinder::new();
        unwinder.try(f);

        fs::rmdir_recursive(path);
    }

    #[test]
    fn test_environment() {
        let path = Path::new("test-lmdb");
        test_db_in_path(&path, || {
            // It looks pretty tree like, because it is the simplest test and
            // it expected to produce easy traceable results
            match Environment::new() {
                Ok(mut env) => {
                    match env.get_maxreaders() {
                        Ok(readers) => assert!(readers != 0, "Max number of readers couldn't be 0"),
                        Err(err) => fail!("Failed to get max number of readers: {}", err.message)
                    };

                    let test_readers = 33;
                    match env.set_maxreaders(test_readers) {
                        Ok(_) => {
                            match env.get_maxreaders() {
                                Ok(readers) => assert!(readers == test_readers, "Get readers != set readers"),
                                Err(err) => fail!("Failed to get max number of readers: {}", err.message)
                            }
                        },
                        Err(err) => fail!("Failed to set max number of readers: {}", err.message)
                    };

                    match env.open(&path, 0, 0o755) {
                        Ok(..) => {
                            match env.sync(true) {
                                Ok(..) => (),
                                Err(err) => fail!("Failed to sync: {}", err.message)
                            };

                            let test_flags = consts::MDB_NOMEMINIT | consts::MDB_NOMETASYNC;

                            match env.set_flags(test_flags, true) {
                                Ok(_) => {
                                    match env.get_flags() {
                                        Ok(new_flags) => assert!((new_flags & test_flags) == test_flags, "Get flags != set flags"),
                                        Err(err) => fail!("Failed to get flags: {}", err.message)
                                    }
                                },
                                Err(err) => fail!("Failed to set flags: {}", err.message)
                            };

                            match env.get_default_db(0) {
                                Ok(db) => {
                                    let key = ~"hello";
                                    let value = ~"world";

                                    match env.new_transaction() {
                                        Ok(mut tnx) => {
                                            match tnx.set(&db, &key, &value) {
                                                Ok(_) => {
                                                    match tnx.get::<~str>(&db, &key) {
                                                        Ok(v) => assert!(v == value, "Written {:?} and read {:?}", value.as_slice(), v.as_slice()),
                                                        Err(err) => fail!("Failed to read value: {}", err.message)
                                                    }
                                                },
                                                Err(err) => fail!("Failed to write value: {}", err.message)
                                            }
                                        },
                                        Err(err) => fail!("Failed to create transaction: {}", err.message)
                                    }
                                },
                                Err(err) => fail!("Failed to get default database: {}", err.message)
                            }
                        },
                        Err(err) => fail!("Failed to open path {}: {}", path.display(), err.message)
                    }
                },
                Err(err) => fail!("Failed to initialize environment: {}", err.message)
            };
        });
    }

    #[test]
    fn test_single_values() {
        let path = Path::new("single-values");
        test_db_in_path(&path, || {
            let mut env = Environment::new().unwrap();
            let _ = env.open(&path, 0, 0o755);
            let _ = env.set_maxdbs(5);

            let db = env.get_default_db(0).unwrap();
            let mut txn = env.new_transaction().unwrap();

            let test_key1 = ~"key1";
            let test_data1 = ~"value1";
            let test_data2 = ~"value2";

            assert!(txn.get::<()>(&db, &test_key1).is_err(), "Key shouldn't exist yet");

            let _ = txn.set(&db, &test_key1, &test_data1);
            let v: ~str = txn.get(&db, &test_key1).unwrap();
            assert!(v == test_data1, "Data written differs from data read");

            let _ = txn.set(&db, &test_key1, &test_data2);
            let v: ~str = txn.get(&db, &test_key1).unwrap();
            assert!(v == test_data2, "Data written differs from data read");

            let _ = txn.del(&db, &test_key1);
            assert!(txn.get::<()>(&db, &test_key1).is_err(), "Key should be deleted");
        });
    }

    #[test]
    fn test_multiple_values() {
        let path = Path::new("multiple-values");
        test_db_in_path(&path, || {
            let mut env = Environment::new().unwrap();
            let _ = env.open(&path, 0, 0o755);
            let _ = env.set_maxdbs(5);

            let db = env.get_default_db(consts::MDB_DUPSORT).unwrap();
            let mut txn = env.new_transaction().unwrap();

            let test_key1 = ~"key1";
            let test_data1 = ~"value1";
            let test_data2 = ~"value2";

            assert!(txn.get::<()>(&db, &test_key1).is_err(), "Key shouldn't exist yet");

            let _ = txn.set(&db, &test_key1, &test_data1);
            let v: ~str = txn.get(&db, &test_key1).unwrap();
            assert!(v == test_data1, "Data written differs from data read");

            let _ = txn.set(&db, &test_key1, &test_data2);
            let v: ~str = txn.get(&db, &test_key1).unwrap();
            assert!(v == test_data1, "It should still return first value");

            let _ = txn.del_exact(&db, &test_key1, &test_data1);

            let v: ~str = txn.get(&db, &test_key1).unwrap();
            assert!(v == test_data2, "It should return second value");
            let _ = txn.del(&db, &test_key1);

            assert!(txn.get::<()>(&db, &test_key1).is_err(), "Key shouldn't exist anymore!");
        });
    }

    #[test]
    fn test_cursors() {

        let path = Path::new("cursors");
        test_db_in_path(&path, || {
            let mut env = Environment::new().unwrap();
            let _ = env.open(&path, 0, 0o755);
            let _ = env.set_maxdbs(5);

            let db = env.get_default_db(consts::MDB_DUPSORT).unwrap();
            let mut txn = env.new_transaction().unwrap();

            let test_key1 = ~"key1";
            let test_key2 = ~"key2";
            let test_values: Vec<~str> = vec!(~"value1", ~"value2", ~"value3", ~"value4");

            assert!(txn.get::<()>(&db, &test_key1).is_err(), "Key shouldn't exist yet");

            for t in test_values.iter() {
                let _ = txn.set(&db, &test_key1, t);
                let _ = txn.set(&db, &test_key2, t);
            }

            let mut cursor = txn.new_cursor(&db).unwrap();
            assert!(cursor.to_first().is_ok());

            assert!(cursor.to_key(&test_key1).is_ok());
            assert!(cursor.item_count().unwrap() == 4);

            assert!(cursor.del_single().is_ok());
            assert!(cursor.item_count().unwrap() == 3);

            assert!(cursor.to_key(&test_key1).is_ok());
            let new_value = ~"testme";

            assert!(cursor.set(&new_value).is_ok());
            let (_, v): ((), ~str) = cursor.get().unwrap();

            // NOTE: this asserting will work once new_value is
            // of the same length as it is inplace change
            assert!(v == new_value);

            assert!(cursor.del_all().is_ok());
            assert!(cursor.to_key(&test_key1).is_err());

            assert!(cursor.to_key(&test_key2).is_ok());
        });
    }
}
*/
