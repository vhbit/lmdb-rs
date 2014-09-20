use std;
use libc::{mod, c_int, c_uint, size_t, c_char};
use std::io::fs::PathExtensions;
use std::mem;
use std::ptr;
use std::result::Result;

pub use self::errors::{MdbError, NotFound, InvalidPath, StateError};
use ffi::consts::*;
use ffi::funcs::*;
use ffi::types::*;
use traits::{ToMdbValue, FromMdbValue};

macro_rules! lift_mdb(
        ($e:expr) => (
        {
            let t = $e;
            match t {
                MDB_SUCCESS => Ok(()),
                _ => Err(MdbError::new_with_code(t))
            }
        }))


macro_rules! try_mdb(
        ($e:expr) => (
        {
            let t = $e;
            match t {
                MDB_SUCCESS => (),
                _ => return Err(MdbError::new_with_code(t))
            }
        });

        ($e:expr, $r:expr) => (
        {
            let t = $e;
            match t {
                MDB_SUCCESS => Ok($r),
                _ => return Err(MdbError::new_with_code(t))
            }
        }))

macro_rules! assert_state_eq(
    ($log:ident, $cur:expr, $exp:expr) =>
        ({
            let c = $cur;
            let e = $exp;
            if c == e {
                ()
            } else {
                let msg = format!("{} requires {}, is in {}", stringify!($log), c, e);
                return Err(StateError(msg))
            }}) )

macro_rules! assert_state_not(
    ($log:ident, $cur:expr, $exp:expr) =>
        ({
            let c = $cur;
            let e = $exp;
            if c != e {
                ()
            } else {
                let msg = format!("{} shouldn't be in {}", stringify!($log), e);
                return Err(StateError(msg))
            }}) )

/// MdbError wraps information about LMDB error
pub mod errors {
    use ffi::consts::*;
    use libc::{c_int};
    use std;
    use utils::{error_msg};

    pub enum MdbError {
        NotFound,
        KeyExists,
        TxnFull,
        CursorFull,
        PageFull,
        Corrupted,
        Panic,
        InvalidPath,
        StateError(String),
        Custom(c_int, String)
    }

    impl MdbError {
        pub fn new_with_code(code: c_int) -> MdbError {
            match code {
                MDB_NOTFOUND    => NotFound,
                MDB_KEYEXIST    => KeyExists,
                MDB_TXN_FULL    => TxnFull,
                MDB_CURSOR_FULL => CursorFull,
                MDB_PAGE_FULL   => PageFull,
                MDB_CORRUPTED   => Corrupted,
                MDB_PANIC       => Panic,
                _               => Custom(code, error_msg(code))
            }
        }
    }


    impl std::fmt::Show for MdbError {
        fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
            match self {
                &NotFound => write!(fmt, "not found"),
                &KeyExists => write!(fmt, "key exists"),
                &TxnFull => write!(fmt, "txn full"),
                &CursorFull => write!(fmt, "cursor full"),
                &PageFull => write!(fmt, "page full"),
                &Corrupted => write!(fmt, "corrupted"),
                &Panic => write!(fmt, "panic"),
                &InvalidPath => write!(fmt, "invalid path for database"),
                &StateError(ref msg) => write!(fmt, "{}", msg),
                &Custom(code, ref msg) => write!(fmt, "{}: {}", code, msg)
            }
        }
    }
}

pub type MdbResult<T> = Result<T, MdbError>;

/// Database
pub struct Database {
    handle: MDB_dbi,
}

impl Database {
    fn new_with_handle(handle: MDB_dbi) -> Database {
        Database { handle: handle }
    }
}

#[deriving(PartialEq, Eq, Show, Clone)]
enum EnvState {
    EnvCreated,
    EnvOpened,
    EnvClosed
}

/// Environment
pub struct Environment {
    env: *const MDB_env,
    state: EnvState,
    flags: c_uint,
}

impl Environment {
    /// Initializes environment
    ///
    /// Note that for using named databases, it should be followed by set_maxdbs()
    /// before opening
    pub fn new() -> MdbResult<Environment> {
        let env: *const MDB_env = ptr::null();
        unsafe {
            let p_env: *mut *const MDB_env = std::mem::transmute(&env);
            let _ = try_mdb!(mdb_env_create(p_env));
        }

        Ok(Environment {
            env: env,
            state: EnvCreated,
            flags: 0
        })
    }

    fn check_path(path: &Path, flags: c_uint, mode: mdb_mode_t) -> MdbResult<()> {
        let as_file = (flags & MDB_NOSUBDIR) == MDB_NOSUBDIR;

        if as_file {
            // FIXME: check file existence/absence
            Ok(())
        } else {
            // There should be a directory before open
            match (path.exists(), path.is_dir()) {
                (false, _) => {
                    lift_mdb!(path.with_c_str(|c_path| unsafe {
                        libc::mkdir(c_path, mode)
                    }))
                },
                (true, true) => Ok(()),
                (true, false) => Err(InvalidPath),
            }
        }
    }

    /// Opens environment
    ///
    /// flags bitwise ORed flags for environment, see
    /// [original documentation](http://symas.com/mdb/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340)
    ///
    /// mode is expected to be permissions on UNIX like systems and is ignored on Windows
    pub fn open(&mut self, path: &Path, flags: c_uint, mode: mdb_mode_t) -> MdbResult<()> {
        let _ = try!(Environment::check_path(path, flags, mode));
        assert_state_eq!(env, self.state, EnvCreated);

        let res = path.with_c_str(|c_path| {
            unsafe {
                mdb_env_open(mem::transmute(self.env), c_path, flags, mode)}
        });

        match res {
            MDB_SUCCESS => {
                self.flags = flags;
                self.state = EnvOpened;
                Ok(())
            },
            _ => {
                unsafe { mdb_env_close(mem::transmute(self.env)); }
                Err(MdbError::new_with_code(res))
            }
        }
    }

    pub fn stat(&self) -> MdbResult<MDB_stat> {
        assert_state_eq!(env, self.state, EnvOpened);

        let mut tmp: MDB_stat = unsafe { std::mem::zeroed() };
        try_mdb!(unsafe { mdb_env_stat(self.env, &mut tmp)}, tmp)
    }

    pub fn info(&self) -> MdbResult<MDB_envinfo> {
        assert_state_eq!(env, self.state, EnvOpened);
        let mut tmp: MDB_envinfo = unsafe { std::mem::zeroed() };
        try_mdb!(unsafe { mdb_env_info(self.env, &mut tmp)}, tmp)
    }

    /// Sync environment to disk
    pub fn sync(&mut self, force: bool) -> MdbResult<()> {
        assert_state_eq!(env, self.state, EnvOpened);
        lift_mdb!(unsafe { mdb_env_sync(self.env, if force {1} else {0})})
    }

    pub fn set_flags(&mut self, flags: c_uint, turn_on: bool) -> MdbResult<()> {
        assert_state_not!(env, self.state, EnvClosed)
        lift_mdb!(unsafe {
            mdb_env_set_flags(self.env, flags, if turn_on {1} else {0})
        })
    }

    pub fn get_flags(&self) -> MdbResult<c_uint> {
        assert_state_not!(env, self.state, EnvClosed);
        let mut flags: c_uint = 0;
        try_mdb!(unsafe {mdb_env_get_flags(self.env, &mut flags)}, flags)
    }

    pub fn set_mapsize(&mut self, size: size_t) -> MdbResult<()> {
        assert_state_eq!(env, self.state, EnvCreated);
        lift_mdb!(unsafe { mdb_env_set_mapsize(self.env, size)})
    }

    pub fn set_maxreaders(&mut self, max_readers: c_uint) -> MdbResult<()> {
        assert_state_eq!(env, self.state, EnvCreated);
        lift_mdb!(unsafe { mdb_env_set_maxreaders(self.env, max_readers)})
    }

    pub fn get_maxreaders(&self) -> MdbResult<c_uint> {
        assert_state_not!(env, self.state, EnvClosed);
        let mut max_readers: c_uint = 0;
        try_mdb!(unsafe {
            mdb_env_get_maxreaders(self.env, &mut max_readers)
        }, max_readers)
    }

    /// Sets number of max DBs open. Should be called before open.
    pub fn set_maxdbs(&mut self, max_dbs: c_uint) -> MdbResult<()> {
        assert_state_eq!(env, self.state, EnvCreated);
        lift_mdb!(unsafe { mdb_env_set_maxdbs(self.env, max_dbs)})
    }

    pub fn get_maxkeysize(&self) -> c_int {
        if self.state != EnvClosed {
            unsafe {mdb_env_get_maxkeysize(self.env)}
        } else {
            0
        }
    }

    /// Creates a backup copy in specified file descriptor
    pub fn copy_to_fd(&self, fd: mdb_filehandle_t) -> MdbResult<()> {
        assert_state_eq!(env, self.state, EnvOpened);
        lift_mdb!(unsafe { mdb_env_copyfd(self.env, fd) })
    }

    /// Gets file descriptor of this environment
    pub fn get_fd(&self) -> MdbResult<mdb_filehandle_t> {
        assert_state_eq!(env, self.state, EnvOpened);
        let mut fd = 0;
        try_mdb!({ unsafe { mdb_env_get_fd(self.env, &mut fd) }}, fd)
    }

    /// Creates a backup copy in specified path
    // FIXME: check who is responsible for creating path: callee or caller
    pub fn copy_to_path(&self, path: &Path) -> MdbResult<()> {
        assert_state_eq!(env, self.state, EnvOpened);
        path.with_c_str(|c_path| unsafe {
            lift_mdb!(mdb_env_copy(self.env, c_path))
        })
    }

    fn create_transaction(&self, parent: Option<NativeTransaction>, flags: c_uint) -> MdbResult<NativeTransaction> {
        assert_state_eq!(env, self.state, EnvOpened);
        let mut handle: *const MDB_txn = ptr::null();
        let parent_handle = match parent {
            Some(t) => t.handle,
            _ => ptr::RawPtr::<MDB_txn>::null()
        };

        try_mdb!(unsafe { mdb_txn_begin(self.env, parent_handle, flags, &mut handle) }, NativeTransaction::new_with_handle(handle))
    }

    /// Creates a new read-write transaction
    pub fn new_transaction(&self) -> MdbResult<Transaction> {
        self.create_transaction(None, 0)
            .and_then(|txn| Ok(Transaction::new_with_native(txn)))
    }

    /// Creates a readonly transaction
    pub fn new_ro_transaction(& self) -> MdbResult<ReadonlyTransaction> {
        self.create_transaction(None, MDB_RDONLY)
            .and_then(|txn| Ok(ReadonlyTransaction::new_with_native(txn)))
    }

    fn get_db_by_name(&self, c_name: *const c_char, flags: c_uint) -> MdbResult<Database> {
        assert_state_eq!(env, self.state, EnvOpened);
        let mut dbi: MDB_dbi = 0;
        self.create_transaction(None, 0)
            .and_then(|txn| try_mdb!(unsafe { mdb_dbi_open(txn.handle, c_name, flags, &mut dbi)}, txn) )
            .and_then(|mut t| t.commit() )
            .and_then(|_| Ok(Database::new_with_handle(dbi)))
    }

    /// Returns or creates database with name
    ///
    /// Note: set_maxdbis should be called before
    pub fn get_or_create_db(&self, name: &str, flags: c_uint) -> MdbResult<Database> {
        name.with_c_str(|c_name| {
            // FIXME: MDB_CREATE should be included only in read-write Environment
            self.get_db_by_name(c_name, flags | MDB_CREATE)
        })
    }

    /// Returns default database
    pub fn get_default_db(&self, flags: c_uint) -> MdbResult<Database> {
        self.get_db_by_name(std::ptr::RawPtr::null(), flags)
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        unsafe {
            mdb_env_close(std::mem::transmute(self.env));
        }
    }
}

#[deriving(PartialEq, Show, Eq, Clone)]
enum TransactionState {
    TxnStateNormal,   // Normal, any operation possible
    TxnStateReleased, // Released (reset on readonly), has to be renewed
    TxnStateInvalid,  // Invalid, no further operation possible
}

struct NativeTransaction<'a> {
    handle: *const MDB_txn,
    state: TransactionState,
}

impl<'a> NativeTransaction<'a> {
    fn new_with_handle(h: *const MDB_txn) -> NativeTransaction<'a> {
        NativeTransaction {
            handle: h,
            state: TxnStateNormal }
    }

    fn commit(&mut self) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        try_mdb!(unsafe { mdb_txn_commit(self.handle) } );
        self.state = TxnStateInvalid;
        Ok(())
    }

    fn abort(&mut self) {
        if self.state != TxnStateNormal {
            debug!("Can't abort transaction: current state {}", self.state)
        } else {
            unsafe { mdb_txn_abort(self.handle); }
            self.state = TxnStateInvalid;
        }
    }

    /// Resets read only transaction, handle is kept. Must be followed
    /// by a call to `renew`
    fn reset(&mut self) {
        if self.state != TxnStateNormal {
            debug!("Can't reset transaction: current state {}", self.state);
        } else {
            unsafe { mdb_txn_reset(self.handle); }
            self.state = TxnStateReleased;
        }
    }

    /// Acquires a new reader lock after it was released by reset
    fn renew(&mut self) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateReleased);
        try_mdb!(unsafe {mdb_txn_renew(self.handle)});
        self.state = TxnStateNormal;
        Ok(())
    }

    fn new_child(&self, flags: c_uint) -> MdbResult<NativeTransaction> {
        let mut out: *const MDB_txn = ptr::null();
        try_mdb!(unsafe { mdb_txn_begin(mdb_txn_env(self.handle), self.handle, flags, &mut out) });
        Ok(NativeTransaction::new_with_handle(out))
    }

    /// Used in Drop to switch state
    fn silent_abort(&mut self) {
        unsafe {mdb_txn_abort(self.handle);}
        self.state = TxnStateInvalid;
    }

    fn get_value<T: FromMdbValue>(&self, db: &Database, key: &ToMdbValue) -> MdbResult<T> {
        let key_val = key.to_mdb_value();
        unsafe {
            let mut data_val: MDB_val = std::mem::zeroed();
            try_mdb!(mdb_get(self.handle, db.handle, &key_val, &mut data_val));
            Ok(FromMdbValue::from_mdb_value(&data_val))
        }
    }

    pub fn get<T: FromMdbValue>(&self, db: &Database, key: &ToMdbValue) -> MdbResult<T> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        self.get_value(db, key)
    }

    fn set_value(&self, db: &Database, key: &ToMdbValue, value: &ToMdbValue) -> MdbResult<()> {
        self.set_value_with_flags(db, key, value, 0)
    }

    fn set_value_with_flags(&self, db: &Database, key: &ToMdbValue, value: &ToMdbValue, flags: c_uint) -> MdbResult<()> {
        unsafe {
            let key_val = key.to_mdb_value();
            let data_val = value.to_mdb_value();

            lift_mdb!(mdb_put(self.handle, db.handle, &key_val, &data_val, flags))
        }
    }

    /// Sets a new value for key, in case of enabled duplicates
    /// it actually appends a new value
    // FIXME: add explicit append function
    // FIXME: think about creating explicit separation of
    // all traits for databases with dup keys
    pub fn set(&self, db: &Database, key: &ToMdbValue, value: &ToMdbValue) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        self.set_value(db, key, value)
    }

    /// Deletes all values by key
    fn del_value(&self, db: &Database, key: &ToMdbValue) -> MdbResult<()> {
        unsafe {
            let key_val = key.to_mdb_value();
            lift_mdb!(mdb_del(self.handle, db.handle, &key_val, std::ptr::null()))
        }
    }

    /// If duplicate keys are allowed deletes value for key which is equal to data
    pub fn del_exact_value(&self, db: &Database, key: &ToMdbValue, data: &ToMdbValue) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        unsafe {
            let key_val = key.to_mdb_value();
            let data_val = data.to_mdb_value();

            lift_mdb!(mdb_del(self.handle, db.handle, &key_val, &data_val))
        }
    }

    /// Deletes all values for key
    pub fn del(&self, db: &Database, key: &ToMdbValue) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        self.del_value(db, key)
    }

    /// creates a new cursor in current transaction tied to db
    pub fn new_cursor(&'a self, db: &'a Database) -> MdbResult<Cursor<'a>> {
        Cursor::<'a>::new(self, db)
    }

    /// Deletes provided database completely
    pub fn del_db(&self, db: &Database) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        unsafe {
            lift_mdb!(mdb_drop(self.handle, db.handle, 1))
        }
    }

    /// Empties provided database
    pub fn empty_db(&self, db: &Database) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        unsafe {
            lift_mdb!(mdb_drop(self.handle, db.handle, 0))
        }
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

    pub fn new_child(&self) -> MdbResult<Transaction> {
        self.inner.new_child(0)
            .and_then(|txn| Ok(Transaction::new_with_native(txn)))
    }

    pub fn new_ro_child(&self) -> MdbResult<ReadonlyTransaction> {
        self.inner.new_child(MDB_RDONLY)
            .and_then(|txn| Ok(ReadonlyTransaction::new_with_native(txn)))
    }

    /// Aborts transaction, handle is freed
    pub fn commit(&mut self) -> MdbResult<()> {
        self.inner.commit()
    }

    /// Aborts transaction, handle is freed
    pub fn abort(&mut self) {
        self.inner.abort();
    }

    pub fn get<T: FromMdbValue>(&self, db: &Database, key: &ToMdbValue) -> MdbResult<T> {
        self.inner.get(db, key)
    }

    pub fn set(&self, db: &Database, key: &ToMdbValue, value: &ToMdbValue) -> MdbResult<()> {
        self.inner.set(db, key, value)
    }

    pub fn del(&self, db: &Database, key: &ToMdbValue) -> MdbResult<()> {
        self.inner.del(db, key)
    }

    pub fn del_exact(&self, db: &Database, key: &ToMdbValue, data: &ToMdbValue) -> MdbResult<()> {
        self.inner.del_exact_value(db, key, data)
    }

    pub fn new_cursor(&'a self, db: &'a Database) -> MdbResult<Cursor<'a>> {
        self.inner.new_cursor(db)
    }

    pub fn del_db(&self, db: &Database) -> MdbResult<()> {
        self.inner.del_db(db)
    }

    pub fn empty_db(&self, db: &Database) -> MdbResult<()> {
        self.inner.empty_db(db)
    }

    /// Returns an iterator for all values in database
    pub fn iter<'a>(&'a self, db: &'a Database) -> MdbResult<CursorIter<'a>> {
        self.inner.new_cursor(db)
            .and_then(|c| Ok(CursorIter { cursor: c, initialized: false}))
    }

    /// Returns an iterator for values between start_key and end_key.
    /// Currently it works only for unique keys (i.e. it will skip
    /// multiple items when DB created with MDB_DUPSORT).
    /// Iterator is valid while cursor is valid
    pub fn keyrange<'a, T: ToMdbValue+Clone>(&'a self, db: &'a Database, start_key: &T, end_key: &T) -> MdbResult<CursorKeyRangeIter<'a>> {
        self.inner.new_cursor(db)
            .and_then(|c| Ok(CursorKeyRangeIter {
                                cursor: c,
                                start_key: start_key.clone().to_mdb_value(),
                                end_key: end_key.clone().to_mdb_value(),
                                initialized: false,}))
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

    pub fn new_ro_child(&self) -> MdbResult<ReadonlyTransaction> {
        self.inner.new_child(MDB_RDONLY)
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
    pub fn renew(&mut self) -> MdbResult<()> {
        self.inner.renew()
    }

    pub fn get<T: FromMdbValue>(&self, db: &Database, key: &ToMdbValue) -> MdbResult<T> {
        self.inner.get(db, key)
    }

    pub fn new_cursor(&'a self, db: &'a Database) -> MdbResult<Cursor<'a>> {
        self.inner.new_cursor(db)
    }

    /// Returns an iterator for all values in database
    pub fn iter<'a>(&'a self, db: &'a Database) -> MdbResult<CursorIter<'a>> {
        self.inner.new_cursor(db)
            .and_then(|c| Ok(CursorIter { cursor: c, initialized: false}))
    }

    /// Returns an iterator for values between start_key and end_key.
    /// Currently it works only for unique keys (i.e. it will skip
    /// multiple items when DB created with MDB_DUPSORT).
    /// Iterator is valid while cursor is valid
    pub fn keyrange<'a, T: ToMdbValue+Clone>(&'a self, db: &'a Database, start_key: &T, end_key: &T) -> MdbResult<CursorKeyRangeIter<'a>> {
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


pub struct Cursor<'a> {
    handle: *const MDB_cursor,
    data_val: MDB_val,
    key_val: MDB_val,
    txn: &'a NativeTransaction<'a>,
    db: &'a Database
}

impl<'a> Cursor<'a> {
    fn new(txn: &'a NativeTransaction, db: &'a Database) -> MdbResult<Cursor<'a>> {
        let mut tmp: *const MDB_cursor = std::ptr::null();
        try_mdb!(unsafe { mdb_cursor_open(txn.handle, db.handle, &mut tmp) });
        Ok(Cursor {
            handle: tmp,
            data_val: unsafe { std::mem::zeroed() },
            key_val: unsafe { std::mem::zeroed() },
            txn: txn,
            db: db,
        })
    }

    fn move_to<T: ToMdbValue+Clone>(&mut self, key: Option<&T>, op: MDB_cursor_op) -> MdbResult<()> {
        // Even if we don't ask for any data and want only to set a position
        // MDB still insists in writing back key and data to provided pointers
        // it's actually not that big deal, considering no actual data copy happens
        self.data_val = unsafe {std::mem::zeroed()};
        self.key_val = match key {
            Some(k) => k.clone().to_mdb_value(),
            _ => unsafe {std::mem::zeroed()}
        };

        lift_mdb!(unsafe { mdb_cursor_get(self.handle, &mut self.key_val, &mut self.data_val, op) })
    }

    /// Moves cursor to first entry
    pub fn to_first(&mut self) -> MdbResult<()> {
        self.move_to(None::<&String>, MDB_FIRST)
    }

    /// Moves cursor to last entry
    pub fn to_last(&mut self) -> MdbResult<()> {
        self.move_to(None::<&String>, MDB_LAST)
    }

    /// Moves cursor to first entry for key if it exists
    pub fn to_key<T:ToMdbValue+Clone>(&mut self, key: &T) -> MdbResult<()> {
        self.move_to(Some(key), MDB_SET)
    }

    /// Moves cursor to first entry for key greater than
    /// or equal to ke
    pub fn to_gte_key<T:ToMdbValue+Clone>(&mut self, key: &T) -> MdbResult<()> {
        self.move_to(Some(key), MDB_SET_RANGE)
    }

    /// Moves cursor to next key, i.e. skip items
    /// with duplicate keys
    pub fn to_next_key(&mut self) -> MdbResult<()> {
        self.move_to(None::<&String>, MDB_NEXT_NODUP)
    }

    /// Moves cursor to next item with the same key as current
    pub fn to_next_key_item(&mut self) -> MdbResult<()> {
        self.move_to(None::<&String>, MDB_NEXT_DUP)
    }

    /// Moves cursor to prev entry, i.e. skips items
    /// with duplicate keys
    pub fn to_prev_key(&mut self) -> MdbResult<()> {
        self.move_to(None::<&String>, MDB_PREV_NODUP)
    }

    /// Moves cursor to prev item with the same key as current
    pub fn to_prev_key_item(&mut self) -> MdbResult<()> {
        self.move_to(None::<&String>, MDB_PREV_DUP)
    }

    /// Moves cursor to first item with the same key as current
    pub fn to_first_key_item(&mut self) -> MdbResult<()> {
        self.move_to(None::<&String>, MDB_FIRST_DUP)
    }

    /// Moves cursor to last item with the same key as current
    pub fn to_last_key_item(&mut self) -> MdbResult<()> {
        self.move_to(None::<&String>, MDB_LAST_DUP)
    }

    /// Retrieves current key/value as tuple
    pub fn get<T: FromMdbValue, U: FromMdbValue>(&mut self) -> MdbResult<(T, U)> {
        unsafe {
            let mut key_val: MDB_val = std::mem::zeroed();
            let mut data_val: MDB_val = std::mem::zeroed();
            try_mdb!(mdb_cursor_get(self.handle, &mut key_val, &mut data_val, MDB_GET_CURRENT));
            Ok((FromMdbValue::from_mdb_value(&key_val), FromMdbValue::from_mdb_value(&data_val)))
        }
    }

    fn get_plain(&self) -> (MDB_val, MDB_val) {
        (self.key_val, self.data_val)
    }

    fn set_value<'a>(&mut self, key:Option<&'a ToMdbValue>, value: &ToMdbValue, flags: c_uint) -> MdbResult<()> {
        let data_val = value.to_mdb_value();
        let key_val = unsafe {
            match  key {
                Some(k) => k.to_mdb_value(),
                _ => std::mem::zeroed()
            }
        };

        lift_mdb!(unsafe {mdb_cursor_put(self.handle, &key_val, &data_val, flags)})
    }

    /// Overwrites value for current item
    /// Note: overwrites max cur_value.len() bytes
    pub fn set(&mut self, value: &ToMdbValue) -> MdbResult<()> {
        self.set_value(None, value, MDB_CURRENT)
    }

    /// Adds a new value if it doesn't exist yet
    pub fn upsert(&mut self, key: &ToMdbValue, value: &ToMdbValue) -> MdbResult<()> {
        self.set_value(Some(key), value, MDB_NOOVERWRITE)
    }

    fn del_value(&mut self, flags: c_uint) -> MdbResult<()> {
        lift_mdb!(unsafe { mdb_cursor_del(self.handle, flags) })
    }

    /// Deletes only current item
    pub fn del_single(&mut self) -> MdbResult<()> {
        self.del_value(0)
    }

    /// Deletes all items with same key as current
    pub fn del_all(&mut self) -> MdbResult<()> {
        self.del_value(MDB_NODUPDATA)
    }

    /// Returns count of items with the same key as current
    pub fn item_count(&self) -> MdbResult<size_t> {
        let mut tmp: size_t = 0;
        try_mdb!(unsafe {mdb_cursor_count(self.handle, &mut tmp)}, tmp)
    }
}

#[unsafe_destructor]
impl<'a> Drop for Cursor<'a> {
    fn drop(&mut self) {
        unsafe { mdb_cursor_close(std::mem::transmute(self.handle)) };
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
    pub fn get_key<T: FromMdbValue>(&self) -> T {
        FromMdbValue::from_mdb_value(&self.key)
    }

    pub fn get_value<T: FromMdbValue>(&self) -> T {
        FromMdbValue::from_mdb_value(&self.value)
    }

    pub fn get<T: FromMdbValue, U: FromMdbValue>(&self) -> (T, U) {
        (FromMdbValue::from_mdb_value(&self.key),  FromMdbValue::from_mdb_value(&self.value))
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

pub struct CursorIter<'a> {
    cursor: Cursor<'a>,
    initialized: bool
}

impl<'a> Iterator<CursorValue> for CursorIter<'a> {
    fn next(&mut self) -> Option<CursorValue> {
        let move_res = if !self.initialized {
            self.initialized = true;
            self.cursor.to_first()
        } else {
            self.cursor.to_next_key()
        };

        if move_res.is_err() {
            None
        } else {
            let (k, v): (MDB_val, MDB_val) = self.cursor.get_plain();
            Some(CursorValue {
                key: k,
                value: v
            })
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
    use std::io::fs::{mod, PathExtensions};
    use std::rt::unwind;
    use std::path::Path;

    use ffi::consts;
    use super::{Environment};

    fn test_db_in_path(path: &Path, f: ||) {
        // Delete dir to be sure nothing existed before test
        if path.exists() {
            let _ = fs::rmdir_recursive(path);
        };

        let _ = unsafe { unwind::try(f) };

        let _ = fs::rmdir_recursive(path);
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
                        Err(err) => fail!("Failed to get max number of readers: {}", err)
                    };

                    let test_readers = 33;
                    match env.set_maxreaders(test_readers) {
                        Ok(_) => {
                            match env.get_maxreaders() {
                                Ok(readers) => assert!(readers == test_readers, "Get readers != set readers"),
                                Err(err) => fail!("Failed to get max number of readers: {}", err)
                            }
                        },
                        Err(err) => fail!("Failed to set max number of readers: {}", err)
                    };

                    match env.open(&path, 0, 0o755) {
                        Ok(..) => {
                            match env.sync(true) {
                                Ok(..) => (),
                                Err(err) => fail!("Failed to sync: {}", err)
                            };

                            let test_flags = consts::MDB_NOMEMINIT | consts::MDB_NOMETASYNC;

                            match env.set_flags(test_flags, true) {
                                Ok(_) => {
                                    match env.get_flags() {
                                        Ok(new_flags) => assert!((new_flags & test_flags) == test_flags, "Get flags != set flags"),
                                        Err(err) => fail!("Failed to get flags: {}", err)
                                    }
                                },
                                Err(err) => fail!("Failed to set flags: {}", err)
                            };

                            match env.get_default_db(0) {
                                Ok(db) => {
                                    let key = "hello";
                                    let value = "world";

                                    match env.new_transaction() {
                                        Ok(tnx) => {
                                            match tnx.set(&db, &key, &value) {
                                                Ok(_) => {
                                                    match tnx.get::<String>(&db, &key) {
                                                        Ok(v) => assert!(v.as_slice() == value, "Written {:?} and read {:?}", value.as_slice(), v.as_slice()),
                                                        Err(err) => fail!("Failed to read value: {}", err)
                                                    }
                                                },
                                                Err(err) => fail!("Failed to write value: {}", err)
                                            }
                                        },
                                        Err(err) => fail!("Failed to create transaction: {}", err)
                                    }
                                },
                                Err(err) => fail!("Failed to get default database: {}", err)
                            }
                        },
                        Err(err) => fail!("Failed to open path {}: {}", path.display(), err)
                    }
                },
                Err(err) => fail!("Failed to initialize environment: {}", err)
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

            let test_key1 = "key1";
            let test_data1 = "value1";
            let test_data2 = "value2";

            assert!(txn.get::<()>(&db, &test_key1).is_err(), "Key shouldn't exist yet");

            let _ = txn.set(&db, &test_key1, &test_data1);
            let v: String = txn.get(&db, &test_key1).unwrap();
            assert!(v.as_slice() == test_data1, "Data written differs from data read");

            let _ = txn.set(&db, &test_key1, &test_data2);
            let v: String = txn.get(&db, &test_key1).unwrap();
            assert!(v.as_slice() == test_data2, "Data written differs from data read");

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

            let test_key1 = "key1";
            let test_data1 = "value1";
            let test_data2 = "value2";

            assert!(txn.get::<()>(&db, &test_key1).is_err(), "Key shouldn't exist yet");

            let _ = txn.set(&db, &test_key1, &test_data1);
            let v: String = txn.get(&db, &test_key1).unwrap();
            assert!(v.as_slice() == test_data1, "Data written differs from data read");

            let _ = txn.set(&db, &test_key1, &test_data2);
            let v: String = txn.get(&db, &test_key1).unwrap();
            assert!(v.as_slice() == test_data1, "It should still return first value");

            let _ = txn.del_exact(&db, &test_key1, &test_data1);

            let v: String = txn.get(&db, &test_key1).unwrap();
            assert!(v.as_slice() == test_data2, "It should return second value");
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

            let test_key1 = "key1";
            let test_key2 = "key2";
            let test_values: Vec<String> = vec!("value1".to_string(), "value2".to_string(), "value3".to_string(), "value4".to_string());

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
            let new_value = "testme";

            assert!(cursor.set(&new_value).is_ok());
            let (_, v): ((), String) = cursor.get().unwrap();

            // NOTE: this asserting will work once new_value is
            // of the same length as it is inplace change
            assert!(v.as_slice() == new_value);

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
