use std;
use std::ptr;
use libc;
use libc::{c_int, c_uint, size_t, c_char};
use std::result::Result;

use mdb::consts::*;
use mdb::funcs::*;
use mdb::types::*;

use traits::{MDBIncomingValue, MDBOutgoingValue, StateError};
use utils::{error_msg, lift, lift_noret};

use std::fmt::Show;
use std::default::Default;

macro_rules! lift(
    ($e:expr, $r:expr) => (
        {
            let t = $e;
            match t {
                MDB_SUCCESS => Ok($r),
                _ => Err(MDBError::new_with_code(t))
            }
        }
    )
)

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

    #[inline]
    pub fn get_code(&self) -> c_int {
        self.code
    }
}

impl StateError for MDBError {
    fn new_state_error(msg: ~str) -> MDBError {
        MDBError {
            code: MDB_INVALID_STATE,
            message: msg
        }
    }
}

impl std::fmt::Show for MDBError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.message.fmt(fmt)
    }
}

pub type MDBResult<T> = Result<T, MDBError>;

/// Database
pub struct Database {
    handle: MDB_dbi,
}

impl Database {
    fn new_with_handle(handle: MDB_dbi) -> Database {
        Database { handle: handle }
    }
}

struct State<S> {
    log_name: ~str,
    cur_state: S,
}

impl<S: Eq + Show + Clone> State<S> {
    fn new(name: ~str, initial: S) -> State<S> {
        State {
            log_name: name,
            cur_state: initial,
        }
    }

    /*
    #[inline]
    fn is(&self, state: S) -> bool {
        self.cur_state == state
    }
    */

    /// Invokes P if current state is equal to desired
    fn then<E: StateError>(&self, desired: S) -> Result<(), E> {
        if self.cur_state != desired {
            let msg = format!("{}: requires {}, is in {}", self.log_name, desired, self.cur_state);
            Err(StateError::new_state_error(msg))
        } else {
            Ok(())
        }
    }

    fn then_not<E: StateError>(&self, unwanted: S) -> Result<(), E> {
        if self.cur_state == unwanted {
            let msg = format!("{}: shouldn't be {}", self.log_name, self.cur_state);
            Err(StateError::new_state_error(msg))
        } else {
            Ok(())
        }
    }

    fn silent_then<T: Default>(&self, desired: S, p: proc() -> T) -> T {
        if self.cur_state != desired {
            Default::default()
        } else {
            p()
        }
    }

    fn silent_then_not<T: Default>(&self, unwanted: S, p: proc() -> T) -> T {
        if self.cur_state == unwanted {
            Default::default()
        } else {
            p()
        }
    }

    fn change<T, E: StateError>(&mut self, desired: S, next: S, p: proc() -> Result<T, E>) -> Result<T, E> {
        try!(self.then(desired));
        p().and_then(|t| {
            self.cur_state = next.clone(); // FIXME: try to find a cleaner way without cloning
            Ok(t)
        })
    }

    fn force_change_to(&mut self, desired: S, p: ||) {
        if self.cur_state != desired {
            p();
            self.cur_state = desired;
        }
    }
}

#[deriving(Eq, Show, Clone)]
enum EnvState {
    EnvCreated,
    EnvOpened,
    EnvClosed
}

/// Environment
pub struct Environment {
    env: *MDB_env,
    path: Option<Box<Path>>,
    state: State<EnvState>,
    flags: c_uint,
}

impl Environment {
    /// Initializes environment
    ///
    /// Note that for using named databases, it should be followed by set_maxdbs()
    /// before opening
    pub fn new() -> MDBResult<Environment> {
        let env: *MDB_env = ptr::RawPtr::null();
        lift(unsafe {
            let pEnv: **mut MDB_env = std::mem::transmute(&env);
            mdb_env_create(pEnv)},
             || Environment {
                 env: env,
                 path: None,
                 state: State::new("Env".to_owned(), EnvCreated),
                 flags: 0
             })
    }

    fn check_path(path: &Path, flags: c_uint, mode: mdb_mode_t) -> MDBResult<()> {
        let as_file = (flags & MDB_NOSUBDIR) == MDB_NOSUBDIR;

        let res =
            if as_file {
                // FIXME: check file existence/absence
                MDB_SUCCESS
            } else {
                // There should be a directory before open
                match (path.exists(), path.is_dir()) {
                    (false, _) => {
                        path.with_c_str(|c_path| unsafe {
                            libc::mkdir(c_path, mode)
                        })
                    },
                    (true, true) => MDB_SUCCESS,
                    (true, false) => libc::EACCES,
                }
            };

        lift_noret(res)
    }

    /// Opens environment
    ///
    /// flags bitwise ORed flags for environment, see
    /// [original documentation](http://symas.com/mdb/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340)
    ///
    /// mode is expected to be permissions on UNIX like systems and is ignored on Windows
    pub fn open(&mut self, path: &Path, flags: c_uint, mode: mdb_mode_t) -> MDBResult<()> {
        let t = self.env;

        let res = self.state.change(EnvCreated, EnvOpened,
                                    proc() {
                                        Environment::check_path(path, flags, mode)
                                            .and_then(|_| {
                                                path.with_c_str(|c_path| {
                                                    lift_noret(unsafe {mdb_env_open(t, c_path, flags, mode)})})})});

        if res.is_ok() {
            self.path = Some(box path.clone());
            self.flags = flags;
        }

        res
    }

    pub fn stat(&self) -> MDBResult<MDB_stat> {
        try!(self.state.then(EnvOpened));

        let tmp: MDB_stat = unsafe { std::mem::init() };
        lift(unsafe { mdb_env_stat(self.env, &tmp)},
             || tmp)
    }

    pub fn info(&self) -> MDBResult<MDB_envinfo> {
        try!(self.state.then(EnvOpened))
        let tmp: MDB_envinfo = unsafe { std::mem::init() };
        lift(unsafe { mdb_env_info(self.env, &tmp)},
             || tmp)
    }

    /// Sync environment to disk
    pub fn sync(&mut self, force: bool) -> MDBResult<()> {
        try!(self.state.then(EnvOpened));
        lift_noret(unsafe { mdb_env_sync(self.env, if force {1} else {0})})
    }

    pub fn set_flags(&mut self, flags: c_uint, turn_on: bool) -> MDBResult<()> {
        try!(self.state.then_not(EnvClosed));
        lift_noret(unsafe {
            mdb_env_set_flags(self.env, flags, if turn_on {1} else {0})
        })
    }

    pub fn get_flags(&self) -> MDBResult<c_uint> {
        try!(self.state.then_not(EnvClosed));
        let flags = 0;
        lift(unsafe {mdb_env_get_flags(self.env, &flags)},
            || flags)
    }

    /// Returns a copy of database path, if it was opened successfully
    pub fn get_path(&self) -> Option<Box<Path>> {
        match self.path {
            Some(ref p) => Some(p.clone()),
            _ => None
        }
    }

    pub fn set_mapsize(&mut self, size: size_t) -> MDBResult<()> {
        try!(self.state.then(EnvCreated));
        lift_noret(unsafe { mdb_env_set_mapsize(self.env, size)})
    }

    pub fn set_maxreaders(&mut self, max_readers: c_uint) -> MDBResult<()> {
        try!(self.state.then(EnvCreated));
        lift_noret(unsafe { mdb_env_set_maxreaders(self.env, max_readers)})
    }

    pub fn get_maxreaders(&self) -> MDBResult<c_uint> {
        try!(self.state.then_not(EnvClosed));
        let max_readers: c_uint = 0;
        lift(unsafe { mdb_env_get_maxreaders(self.env, &max_readers)},
            || max_readers )
    }

    /// Sets number of max DBs open. Should be called before open.
    pub fn set_maxdbs(&mut self, max_dbs: c_uint) -> MDBResult<()> {
        try!(self.state.then(EnvCreated));
        lift_noret(unsafe { mdb_env_set_maxdbs(self.env, max_dbs)})
    }

    pub fn get_maxkeysize(&self) -> c_int {
        self.state.silent_then_not(EnvClosed,
                                   proc()  unsafe {mdb_env_get_maxkeysize(self.env)})
    }

    /// Creates a backup copy in specified file descriptor
    pub fn copy_to_fd(&self, fd: mdb_filehandle_t) -> MDBResult<()> {
        try!(self.state.then(EnvOpened));
        lift_noret(unsafe { mdb_env_copyfd(self.env, fd) })        
    }

    /// Gets file descriptor of this environment
    pub fn get_fd(&self) -> MDBResult<mdb_filehandle_t> {
        try!(self.state.then(EnvOpened));
        let fd = 0;
        lift({ unsafe { mdb_env_get_fd(self.env, &fd) }}, || fd)
    }

    /// Creates a backup copy in specified path
    // FIXME: check who is responsible for creating path: callee or caller
    pub fn copy_to_path(&self, path: &Path) -> MDBResult<()> {
        try!(self.state.then(EnvOpened));
        path.with_c_str(|c_path| unsafe {
            lift_noret(mdb_env_copy(self.env, c_path))
        })
    }

    fn create_transaction(&self, parent: Option<NativeTransaction>, flags: c_uint) -> MDBResult<NativeTransaction> {
        try!(self.state.then(EnvOpened));
        let handle: *MDB_txn = ptr::RawPtr::null();
        let parent_handle = match parent {
            Some(t) => t.handle,
            _ => ptr::RawPtr::<MDB_txn>::null()
        };

        lift(unsafe { mdb_txn_begin(self.env, parent_handle, flags, &handle) },
             || NativeTransaction::new_with_handle(handle))
    }

    /// Creates a new read-write transaction
    pub fn new_transaction(&self) -> MDBResult<Transaction> {
        self.create_transaction(None, 0)
            .and_then(|txn| Ok(Transaction::new_with_native(txn)))
    }

    /// Creates a readonly transaction
    pub fn new_readonly_transaction(& self) -> MDBResult<ReadonlyTransaction> {
        self.create_transaction(None, MDB_RDONLY)
            .and_then(|txn| Ok(ReadonlyTransaction::new_with_native(txn)))
    }

    fn get_db_by_name(&self, c_name: *c_char, flags: c_uint) -> MDBResult<Database> {
        try!(self.state.then(EnvOpened));

        let dbi: MDB_dbi = 0;

        // FIXME: using macro to avoid capturing txn in closure
        // it's actually pretty awkward although reasonable from compiler view
        self.create_transaction(None, 0)
            .and_then(|txn| lift!(unsafe { mdb_dbi_open(txn.handle, c_name, flags, &dbi)}, txn) )
            .and_then(|mut t| t.commit() )
            .and_then(|_| Ok(Database::new_with_handle(dbi)))
    }

    /// Returns or creates database with name
    ///
    /// Note: set_maxdbis should be called before
    pub fn get_or_create_db(&self, name: &str, flags: c_uint) -> MDBResult<Database> {
        name.with_c_str(|c_name| {
            // FIXME: MDB_CREATE should be included only in read-write Environment
            self.get_db_by_name(c_name, flags | MDB_CREATE)
        })
    }

    /// Returns default database
    pub fn get_default_db(&self, flags: c_uint) -> MDBResult<Database> {
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

#[deriving(Show, Eq, Clone)]
enum TransactionState {
    TxnStateNormal,   // Normal, any operation possible
    TxnStateReleased, // Released (reset on readonly), has to be renewed
    TxnStateInvalid,  // Invalid, no further operation possible
}

struct NativeTransaction<'a> {
    handle: *MDB_txn,
    state: State<TransactionState>,
}

impl<'a> NativeTransaction<'a> {
    fn new_with_handle(h: *MDB_txn) -> NativeTransaction {
        NativeTransaction {
            handle: h,
            state: State::new("Txn".to_owned(), TxnStateNormal) }
    }

    fn commit(&mut self) -> MDBResult<()> {
        let t = self.handle;
        self.state.change(TxnStateNormal, TxnStateInvalid,
                          proc() lift_noret(unsafe { mdb_txn_commit(t) } ))
    }

    #[allow(unused_must_use)]
    fn abort(&mut self) {
        let t = self.handle;
        self.state.change(TxnStateNormal, TxnStateInvalid,
                          proc() lift_noret(unsafe { mdb_txn_abort(t); MDB_SUCCESS }));
    }

    /// Resets read only transaction, handle is kept. Must be followed
    /// by call to renew
    #[allow(unused_must_use)]
    fn reset(&mut self) {
        let t = self.handle;
        self.state.change(TxnStateNormal, TxnStateReleased,
                          proc() lift_noret(unsafe { mdb_txn_reset(t); MDB_SUCCESS }));
    }

    /// Acquires a new reader lock after it was released by reset
    fn renew(&mut self) -> MDBResult<()> {
        let t = self.handle;
        self.state.change(TxnStateReleased, TxnStateNormal,
                          proc() lift_noret(unsafe {mdb_txn_renew(t)}))
    }


    fn create_child(&self, flags: c_uint) -> MDBResult<NativeTransaction> {
        let out: *MDB_txn = ptr::RawPtr::null();
        lift(unsafe { mdb_txn_begin(mdb_txn_env(self.handle), self.handle, flags, &out) },
             || NativeTransaction::new_with_handle(out))
    }

    /// Used in Drop to switch state
    fn silent_abort(&mut self) {
        let t = self.handle;
        self.state.force_change_to(TxnStateInvalid,
                                   || unsafe {mdb_txn_abort(t)})
    }

    fn get_value<T: MDBOutgoingValue>(&self, db: &Database, key: &MDBIncomingValue) -> MDBResult<T> {
        unsafe {
            let key_val = key.to_mdb_value();
            let data_val: MDB_val = std::mem::init();

            lift(mdb_get(self.handle, db.handle, &key_val, &data_val),
                 || MDBOutgoingValue::from_mdb_value(&data_val))
        }
    }

    pub fn get<T: MDBOutgoingValue>(&self, db: &Database, key: &MDBIncomingValue) -> MDBResult<T> {
        try!(self.state.then(TxnStateNormal));
        self.get_value(db, key)
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
    pub fn set(&self, db: &Database, key: &MDBIncomingValue, value: &MDBIncomingValue) -> MDBResult<()> {
        try!(self.state.then(TxnStateNormal))
        self.set_value(db, key, value)
    }

    /// Deletes all values by key
    fn del_value(&self, db: &Database, key: &MDBIncomingValue) -> MDBResult<()> {
        unsafe {
            let key_val = key.to_mdb_value();
            lift_noret(mdb_del(self.handle, db.handle, &key_val, std::ptr::RawPtr::null()))
        }
    }

    /// If duplicate keys are allowed deletes value for key which is equal to data
    pub fn del_exact_value(&self, db: &Database, key: &MDBIncomingValue, data: &MDBIncomingValue) -> MDBResult<()> {
        try!(self.state.then(TxnStateNormal));
        unsafe {
            let key_val = key.to_mdb_value();
            let data_val = data.to_mdb_value();

            lift_noret(mdb_del(self.handle, db.handle, &key_val, &data_val))
        }
    }

    /// Deletes all values for key
    pub fn del(&self, db: &Database, key: &MDBIncomingValue) -> MDBResult<()> {
        try!(self.state.then(TxnStateNormal));
        self.del_value(db, key)
    }

    /// creates a new cursor in current transaction tied to db
    pub fn new_cursor(&'a self, db: &'a Database) -> MDBResult<Cursor<'a>> {
        Cursor::<'a>::new(self, db)
    }
}

pub struct Transaction<'a> {
    inner: NativeTransaction<'a>,
}

pub struct ReadonlyTransaction<'a> {
    inner: NativeTransaction<'a>,
}

impl<'a> Transaction<'a> {
    fn new_with_native(txn: NativeTransaction<'a>) -> Transaction<'a> {
        Transaction {
            inner: txn
        }
    }

    pub fn create_child(&self) -> MDBResult<Transaction> {
        self.inner.create_child(0)
            .and_then(|txn| Ok(Transaction::new_with_native(txn)))
    }

    pub fn create_ro_child(&self) -> MDBResult<ReadonlyTransaction> {
        self.inner.create_child(MDB_RDONLY)
            .and_then(|txn| Ok(ReadonlyTransaction::new_with_native(txn)))
    }

    /// Aborts transaction, handle is freed
    pub fn commit(&mut self) -> MDBResult<()> {
        self.inner.commit()
    }

    /// Aborts transaction, handle is freed
    pub fn abort(&mut self) {
        self.inner.abort();
    }

    pub fn get<T: MDBOutgoingValue>(&self, db: &Database, key: &MDBIncomingValue) -> MDBResult<T> {
        self.inner.get(db, key)
    }

    pub fn set(&self, db: &Database, key: &MDBIncomingValue, value: &MDBIncomingValue) -> MDBResult<()> {
        self.inner.set(db, key, value)
    }

    pub fn del(&self, db: &Database, key: &MDBIncomingValue) -> MDBResult<()> {
        self.inner.del(db, key)
    }

    pub fn del_exact(&self, db: &Database, key: &MDBIncomingValue, data: &MDBIncomingValue) -> MDBResult<()> {
        self.inner.del_exact_value(db, key, data)
    }

    pub fn new_cursor(&'a self, db: &'a Database) -> MDBResult<Cursor<'a>> {
        self.inner.new_cursor(db)
    }
}

#[unsafe_destructor]
impl<'a> Drop for Transaction<'a> {    
    fn drop(&mut self) {
        self.inner.silent_abort();
    }    
}

impl<'a> ReadonlyTransaction<'a> {
    fn new_with_native(txn: NativeTransaction<'a>) -> ReadonlyTransaction<'a> {
        ReadonlyTransaction {
            inner: txn,
        }
    }

    pub fn create_ro_child(&self) -> MDBResult<ReadonlyTransaction> {
        self.inner.create_child(MDB_RDONLY)
            .and_then(|txn| Ok(ReadonlyTransaction::new_with_native(txn)))

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

    pub fn get<T: MDBOutgoingValue>(&self, db: &Database, key: &MDBIncomingValue) -> MDBResult<T> {
        self.inner.get(db, key)
    }    

    /*
    pub fn new_cursor(&'a self, db: &'a Database) -> MDBResult<Cursor<'a>> {
        self.inner.new_cursor(db)
    }
    */

    /// Returns an iterator for values between start_key and end_key.
    /// Currently it works only for unique keys (i.e. it will skip
    /// multiple items when DB created with MDB_DUPSORT).
    /// Iterator is valid while cursor is valid
    pub fn iter_in_keyrange<'a, T: MDBIncomingValue+Clone>(&'a self, db: &'a Database, start_key: &T, end_key: &T) -> MDBResult<CursorKeyRangeIter<'a>> {
        self.inner.new_cursor(db)
            .and_then(|c| Ok(CursorKeyRangeIter {
                                cursor: c,
                                start_key: start_key.clone().to_mdb_value(),
                                end_key: end_key.clone().to_mdb_value(),
                                initialized: false,}))
    }
}


#[unsafe_destructor]
impl<'a> Drop for ReadonlyTransaction<'a> {    
    fn drop(&mut self) {
        self.inner.silent_abort();
    }    
}


struct Cursor<'a> {
    handle: *MDB_cursor,
    data_val: MDB_val,
    key_val: MDB_val,
    txn: &'a NativeTransaction<'a>,
    db: &'a Database
}

impl<'a> Cursor<'a> {
    fn new(txn: &'a NativeTransaction, db: &'a Database) -> MDBResult<Cursor<'a>> {
        let tmp: *MDB_cursor = std::ptr::RawPtr::null();
        lift(unsafe { mdb_cursor_open(txn.handle, db.handle, &tmp) },
             || unsafe {
                 Cursor {
                     handle: tmp,
                     data_val: std::mem::init(),
                     key_val: std::mem::init(),
                     txn: txn,
                     db: db,
                 }
             })
    }

    fn move_to<T: MDBIncomingValue+Clone>(&mut self, key: Option<&T>, op: MDB_cursor_op) -> MDBResult<()> {
        // Even if we don't ask for any data and want only to set a position
        // MDB still insists in writing back key and data to provided pointers
        // it's actually not that big deal, considering no actual data copy happens
        self.data_val = unsafe {std::mem::init()};
        self.key_val = match key {
            Some(k) => k.clone().to_mdb_value(),
            _ => unsafe {std::mem::init()}
        };

        lift_noret(unsafe { mdb_cursor_get(self.handle, &self.key_val, &self.data_val, op) })
    }

    /// Moves cursor to first entry
    pub fn to_first(&mut self) -> MDBResult<()> {
        self.move_to(None::<&~str>, MDB_FIRST)
    }

    /// Moves cursor to last entry
    pub fn to_last(&mut self) -> MDBResult<()> {
        self.move_to(None::<&~str>, MDB_LAST)
    }

    /// Moves cursor to first entry for key if it exists
    pub fn to_key<T:MDBIncomingValue+Clone>(&mut self, key: &T) -> MDBResult<()> {
        self.move_to(Some(key), MDB_SET)
    }

    /// Moves cursor to first entry for key greater than
    /// or equal to ke
    pub fn to_gte_key<T:MDBIncomingValue+Clone>(&mut self, key: &T) -> MDBResult<()> {
        self.move_to(Some(key), MDB_SET_RANGE)
    }

    /// Moves cursor to next key, i.e. skip items
    /// with duplicate keys
    pub fn to_next_key(&mut self) -> MDBResult<()> {
        self.move_to(None::<&~str>, MDB_NEXT_NODUP)
    }

    /// Moves cursor to next item with the same key as current
    pub fn to_next_key_item(&mut self) -> MDBResult<()> {
        self.move_to(None::<&~str>, MDB_NEXT_DUP)
    }

    /// Moves cursor to prev entry, i.e. skips items
    /// with duplicate keys
    pub fn to_prev_key(&mut self) -> MDBResult<()> {
        self.move_to(None::<&~str>, MDB_PREV_NODUP)
    }

    /// Moves cursor to prev item with the same key as current
    pub fn to_prev_key_item(&mut self) -> MDBResult<()> {
        self.move_to(None::<&~str>, MDB_PREV_DUP)
    }

    /// Moves cursor to first item with the same key as current
    pub fn to_first_key_item(&mut self) -> MDBResult<()> {
        self.move_to(None::<&~str>, MDB_FIRST_DUP)
    }

    /// Moves cursor to last item with the same key as current
    pub fn to_last_key_item(&mut self) -> MDBResult<()> {
        self.move_to(None::<&~str>, MDB_LAST_DUP)
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

    fn get_plain(&self) -> (MDB_val, MDB_val) {
        (self.key_val, self.data_val)
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

#[unsafe_destructor]
impl<'a> Drop for Cursor<'a> {
    fn drop(&mut self) {
        unsafe { mdb_cursor_close(self.handle) };
    }
}

pub struct CursorValue {
    key: MDB_val,
    value: MDB_val,
}

/// CursorValue performs lazy data extraction from iterator
/// avoiding any data conversions and memory copy. Lifetime
/// is limited to iterator lifetime
impl CursorValue {
    pub fn get_key<T: MDBOutgoingValue>(&self) -> T {
        MDBOutgoingValue::from_mdb_value(&self.key)
    }

    pub fn get_value<T: MDBOutgoingValue>(&self) -> T {
        MDBOutgoingValue::from_mdb_value(&self.value)
    }

    pub fn get<T: MDBOutgoingValue, U: MDBOutgoingValue>(&self) -> (T, U) {
        (MDBOutgoingValue::from_mdb_value(&self.key),  MDBOutgoingValue::from_mdb_value(&self.value))
    }
}

pub struct CursorKeyRangeIter<'a> {
    cursor: Cursor<'a>,
    start_key: MDB_val,
    end_key: MDB_val,
    initialized: bool
}

impl<'a> Iterator<CursorValue> for CursorKeyRangeIter<'a> {
    fn next(&mut self) -> Option<CursorValue> {
        let move_res = if !self.initialized {
            self.initialized = true;
            self.cursor.to_gte_key(&self.start_key)
        } else {
            self.cursor.to_next_key()
        };

        if move_res.is_err() {
            None
        } else {
            let (k, v): (MDB_val, MDB_val) = self.cursor.get_plain();
            let cmp_res = unsafe {mdb_cmp(self.cursor.txn.handle, self.cursor.db.handle, &k, &self.end_key)};

            if cmp_res > 0 {
                Some(CursorValue {
                    key: k,
                    value: v
                })
            } else {
                None
            }
        }
    }

    fn size_hint(&self) -> (uint, Option<uint>) {
        match self.cursor.item_count() {
            Err(_) => (0, None),
            Ok(x) => (x as uint, None)
        }        
    }
}

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
                                    let key = "hello".to_owned();
                                    let value = "world".to_owned();

                                    match env.new_transaction() {
                                        Ok(tnx) => {
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
            let txn = env.new_transaction().unwrap();

            let test_key1 = "key1".to_owned();
            let test_data1 = "value1".to_owned();
            let test_data2 = "value2".to_owned();

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
            let txn = env.new_transaction().unwrap();

            let test_key1 = "key1".to_owned();
            let test_data1 = "value1".to_owned();
            let test_data2 = "value2".to_owned();

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
            let _ = env.set_maxdbs(5);
            let _ = env.open(&path, 0, 0o755);

            let db = env.get_default_db(consts::MDB_DUPSORT).unwrap();
            let txn = env.new_transaction().unwrap();

            let test_key1 = "key1".to_owned();
            let test_key2 = "key2".to_owned();
            let test_values: Vec<~str> = vec!("value1".to_owned(), "value2".to_owned(), "value3".to_owned(), "value4".to_owned());

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
            let new_value = "testme".to_owned();

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

    #[test]
    fn test_db_creation() {
        let path = Path::new("dbs");
        test_db_in_path(&path, || {
            let mut env = Environment::new().unwrap();
            assert!(env.set_maxdbs(5).is_ok());
            assert!(env.open(&path, 0, 0o755).is_ok());
            assert!(env.get_or_create_db("test-db", 0).is_ok());
        });
    }
}
