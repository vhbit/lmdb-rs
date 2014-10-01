use std;
use std::cell::{UnsafeCell};
use std::collections::HashMap;
use libc::{mod, c_int, c_uint, size_t};
use std::io::fs::PathExtensions;
use std::mem;
use std::ptr;
use std::result::Result;
use sync::{Mutex};

pub use self::errors::{MdbError, NotFound, InvalidPath, StateError};
use ffi::consts::*;
use ffi::funcs::*;
use ffi::types::*;
use traits::{MdbValue, ToMdbValue, FromMdbValue};


macro_rules! lift_mdb {
    ($e:expr) => (lift_mdb!($e, ()));
    ($e:expr, $r:expr) => (
        {
            let t = $e;
            match t {
                MDB_SUCCESS => Ok($r),
                _ => return Err(MdbError::new_with_code(t))
            }
        })
}

macro_rules! try_mdb {
        ($e:expr) => (
        {
            let t = $e;
            match t {
                MDB_SUCCESS => (),
                _ => return Err(MdbError::new_with_code(t))
            }
        })
}

macro_rules! assert_state_eq {
    ($log:ident, $cur:expr, $exp:expr) =>
        ({
            let c = $cur;
            let e = $exp;
            if c == e {
                ()
            } else {
                let msg = format!("{} requires {}, is in {}", stringify!($log), c, e);
                return Err(StateError(msg))
            }})
}

macro_rules! assert_state_not {
    ($log:ident, $cur:expr, $exp:expr) =>
        ({
            let c = $cur;
            let e = $exp;
            if c != e {
                ()
            } else {
                let msg = format!("{} shouldn't be in {}", stringify!($log), e);
                return Err(StateError(msg))
            }})
}

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

pub trait WriteTransaction<'a> {
    fn get_write_transaction(&'a self) -> &'a NativeTransaction;
}

pub trait ReadTransaction<'a> {
    fn get_read_transaction(&'a self) -> &'a NativeTransaction;
}

bitflags! {
    #[doc = "A set of environment open flags"]
    flags EnvFlags: c_uint {
        #[doc="Use a fixed address for the mmap region. This flag must be specified when creating the environment, and is stored persistently in the environment. If successful, the memory map will always reside at the same virtual address and pointers used to reference data items in the database will be constant across multiple invocations. This option may not always work, depending on how the operating system has allocated memory to shared libraries and other uses. The feature is highly experimental."]
        static EnvFixedMap    = MDB_FIXEDMAP,
        #[doc="By default, LMDB creates its environment in a directory whose pathname is given in path, and creates its data and lock files under that directory. With this option, path is used as-is for the database main data file. The database lock file is the path with \"-lock\" appended."]
        static EnvNoSubDir    = MDB_NOSUBDIR,
        #[doc="Don't flush system buffers to disk when committing a transaction. This optimization means a system crash can corrupt the database or lose the last transactions if buffers are not yet flushed to disk. The risk is governed by how often the system flushes dirty buffers to disk and how often mdb_env_sync() is called. However, if the filesystem preserves write order and the MDB_WRITEMAP flag is not used, transactions exhibit ACI (atomicity, consistency, isolation) properties and only lose D (durability). I.e. database integrity is maintained, but a system crash may undo the final transactions. Note that (MDB_NOSYNC | MDB_WRITEMAP) leaves the system with no hint for when to write transactions to disk, unless mdb_env_sync() is called. (MDB_MAPASYNC | MDB_WRITEMAP) may be preferable. This flag may be changed at any time using mdb_env_set_flags()."]
        static EnvNoSync      = MDB_NOSYNC,
        #[doc="Open the environment in read-only mode. No write operations will be allowed. LMDB will still modify the lock file - except on read-only filesystems, where LMDB does not use locks."]
        static EnvReadOnly    = MDB_RDONLY,
        #[doc="Flush system buffers to disk only once per transaction, omit the metadata flush. Defer that until the system flushes files to disk, or next non-MDB_RDONLY commit or mdb_env_sync(). This optimization maintains database integrity, but a system crash may undo the last committed transaction. I.e. it preserves the ACI (atomicity, consistency, isolation) but not D (durability) database property. This flag may be changed at any time using mdb_env_set_flags()."]
        static EnvNoMetaSync  = MDB_NOMETASYNC,
        #[doc="Use a writeable memory map unless MDB_RDONLY is set. This is faster and uses fewer mallocs, but loses protection from application bugs like wild pointer writes and other bad updates into the database. Incompatible with nested transactions. Processes with and without MDB_WRITEMAP on the same environment do not cooperate well."]
        static EnvWriteMap    = MDB_WRITEMAP,
        #[doc="When using MDB_WRITEMAP, use asynchronous flushes to disk. As with MDB_NOSYNC, a system crash can then corrupt the database or lose the last transactions. Calling mdb_env_sync() ensures on-disk database integrity until next commit. This flag may be changed at any time using mdb_env_set_flags()."]
        static EnvMapAsync    = MDB_MAPASYNC,
        #[doc="Don't use Thread-Local Storage. Tie reader locktable slots to MDB_txn objects instead of to threads. I.e. mdb_txn_reset() keeps the slot reseved for the MDB_txn object. A thread may use parallel read-only transactions. A read-only transaction may span threads if the user synchronizes its use. Applications that multiplex many user threads over individual OS threads need this option. Such an application must also serialize the write transactions in an OS thread, since LMDB's write locking is unaware of the user threads."]
        static EnvNoTls       = MDB_NOTLS,
        #[doc="Don't do any locking. If concurrent access is anticipated, the caller must manage all concurrency itself. For proper operation the caller must enforce single-writer semantics, and must ensure that no readers are using old transactions while a writer is active. The simplest approach is to use an exclusive lock so that no readers may be active at all when a writer begins. "]
        static EnvNoLock      = MDB_NOLOCK,
        #[doc="Turn off readahead. Most operating systems perform readahead on read requests by default. This option turns it off if the OS supports it. Turning it off may help random read performance when the DB is larger than RAM and system RAM is full. The option is not implemented on Windows."]
        static EnvNoReadAhead = MDB_NORDAHEAD,
        #[doc="Don't initialize malloc'd memory before writing to unused spaces in the data file. By default, memory for pages written to the data file is obtained using malloc. While these pages may be reused in subsequent transactions, freshly malloc'd pages will be initialized to zeroes before use. This avoids persisting leftover data from other code (that used the heap and subsequently freed the memory) into the data file. Note that many other system libraries may allocate and free memory from the heap for arbitrary uses. E.g., stdio may use the heap for file I/O buffers. This initialization step has a modest performance cost so some applications may want to disable it using this flag. This option can be a problem for applications which handle sensitive data like passwords, and it makes memory checkers like Valgrind noisy. This flag is not needed with MDB_WRITEMAP, which writes directly to the mmap instead of using malloc for pages. The initialization is also skipped if MDB_RESERVE is used; the caller is expected to overwrite all of the memory that was reserved in that case. This flag may be changed at any time using mdb_env_set_flags()."]
        static EnvNoMemInit   = MDB_NOMEMINIT
    }
}

bitflags! {
    #[doc = "A set of database flags"]
    flags DbFlags: c_uint {
        #[doc="Keys are strings to be compared in reverse order, from the end of the strings to the beginning. By default, Keys are treated as strings and compared from beginning to end."]
        static DbReverseKey   = MDB_REVERSEKEY,
        #[doc="Duplicate keys may be used in the database. (Or, from another perspective, keys may have multiple data items, stored in sorted order.) By default keys must be unique and may have only a single data item."]
        static DbAllowDups    = MDB_DUPSORT,
        #[doc="Keys are binary integers in native byte order. Setting this option requires all keys to be the same size, typically sizeof(int) or sizeof(size_t)."]
        static DbIntKey       = MDB_INTEGERKEY,
        #[doc="This flag may only be used in combination with MDB_DUPSORT. This option tells the library that the data items for this database are all the same size, which allows further optimizations in storage and retrieval. When all data items are the same size, the MDB_GET_MULTIPLE and MDB_NEXT_MULTIPLE cursor operations may be used to retrieve multiple items at once."]
        static DbDupFixed     = MDB_DUPFIXED,
        #[doc="This option specifies that duplicate data items are also integers, and should be sorted as such."]
        static DbAllowIntDups = MDB_INTEGERDUP,
        #[doc="This option specifies that duplicate data items should be compared as strings in reverse order."]
        static DbReversedDups = MDB_REVERSEDUP,
        #[doc="Create the named database if it doesn't exist. This option is not allowed in a read-only transaction or a read-only environment."]
        static DbCreate       = MDB_CREATE,
    }
}

/// Database
pub struct Database {
    handle: MDB_dbi,
    owns: bool
}

impl Database {
    fn new_with_handle(handle: MDB_dbi, owns: bool) -> Database {
        Database { handle: handle, owns: owns }
    }

    pub fn get<'a, T: FromMdbValue<'a>>(&self, txn: &'a ReadTransaction<'a>, key: &'a ToMdbValue<'a>) -> MdbResult<T> {
        txn.get_read_transaction().get(self, key)
    }

    pub fn set<'a>(&'a self, txn: &'a WriteTransaction<'a>, key: &'a ToMdbValue<'a>, value: &'a ToMdbValue<'a>) -> MdbResult<()> {
        txn.get_write_transaction().set(self, key, value)
    }

    pub fn del<'a>(&self, txn: &'a WriteTransaction<'a>, key: &'a ToMdbValue<'a>) -> MdbResult<()> {
        txn.get_write_transaction().del(self, key)
    }

    pub fn del_exact<'a>(&self, txn: &'a WriteTransaction<'a>, key: &'a ToMdbValue<'a>, data: &'a ToMdbValue<'a>) -> MdbResult<()> {
        txn.get_write_transaction().del_exact_value(self, key, data)
    }

    pub fn new_cursor<'a>(&'a self, txn: &'a ReadTransaction<'a>) -> MdbResult<Cursor<'a>> {
        txn.get_read_transaction().new_cursor(self)
    }

    pub fn del_db<'a>(&self, txn: &'a WriteTransaction<'a>) -> MdbResult<()> {
        txn.get_write_transaction().del_db(self)
    }

    pub fn clear<'a>(&self, txn: &'a WriteTransaction<'a>) -> MdbResult<()> {
        txn.get_write_transaction().empty_db(self)
    }

    /// Returns an iterator for all values in database
    pub fn iter<'a>(&'a self, txn: &'a ReadTransaction<'a>) -> MdbResult<CursorIter<'a>> {
        txn.get_read_transaction().new_cursor(self)
            .and_then(|c| Ok(CursorIter::new(c)))
    }

    /// Returns an iterator for values between start_key and end_key.
    /// Currently it works only for unique keys (i.e. it will skip
    /// multiple items when DB created with MDB_DUPSORT).
    /// Iterator is valid while cursor is valid
    pub fn keyrange<'a, T: 'a>(&'a self, txn: &'a ReadTransaction<'a>, start_key: &'a T, end_key: &'a T)
                                             -> MdbResult<CursorKeyRangeIter<'a>>
                                             where T: ToMdbValue<'a> + Clone {
        txn.get_read_transaction().new_cursor(self)
            .and_then(|c| Ok(CursorKeyRangeIter::new(c, start_key, end_key)))
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if self.owns {
            // FIXME: drop dbi handle
            // unsafe { mdb_dbi_close(self.handle) }
        }
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
    flags: EnvFlags,
//    reader_pool: Mutex<Vec<Option<ReadonlyTransaction>>>,
    db_cache: Mutex<UnsafeCell<HashMap<String, Database>>>,
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
            flags: EnvFlags::empty(),
//            reader_pool: Mutex::new(Vec::new()),
            db_cache: Mutex::new(UnsafeCell::new(HashMap::new())),
        })
    }

    fn check_path(path: &Path, flags: EnvFlags, mode: mdb_mode_t) -> MdbResult<()> {
        let as_file = flags.contains(EnvNoSubDir);

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
    pub fn open(&mut self, path: &Path, flags: EnvFlags, mode: mdb_mode_t) -> MdbResult<()> {
        let _ = try!(Environment::check_path(path, flags, mode));
        assert_state_eq!(env, self.state, EnvCreated);

        let res = path.with_c_str(|c_path| {
            unsafe {
                mdb_env_open(mem::transmute(self.env), c_path, flags.bits, mode)}
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
        lift_mdb!(unsafe { mdb_env_stat(self.env, &mut tmp)}, tmp)
    }

    pub fn info(&self) -> MdbResult<MDB_envinfo> {
        assert_state_eq!(env, self.state, EnvOpened);
        let mut tmp: MDB_envinfo = unsafe { std::mem::zeroed() };
        lift_mdb!(unsafe { mdb_env_info(self.env, &mut tmp)}, tmp)
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
        lift_mdb!(unsafe {mdb_env_get_flags(self.env, &mut flags)}, flags)
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
        lift_mdb!(unsafe {
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
        lift_mdb!({ unsafe { mdb_env_get_fd(self.env, &mut fd) }}, fd)
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
            _ => ptr::null()
        };

        lift_mdb!(unsafe { mdb_txn_begin(self.env, parent_handle, flags, &mut handle) },
                 NativeTransaction::new_with_handle(handle, flags as uint))
    }

    /// Creates a new read-write transaction
    ///
    /// Use `get_reader` to get much faster lock-free alternative
    pub fn new_transaction(&self) -> MdbResult<Transaction> {
        self.create_transaction(None, 0)
            .and_then(|txn| Ok(Transaction::new_with_native(txn)))
    }

    /// Creates a readonly transaction
    pub fn get_reader(&self) -> MdbResult<ReadonlyTransaction> {
        self.create_transaction(None, MDB_RDONLY)
            .and_then(|txn| Ok(ReadonlyTransaction::new_with_native(txn)))
    }

    fn create_db<'a>(&'a self, db_name: Option<&'a str>, flags: DbFlags) -> MdbResult<MDB_dbi> {
        let mut dbi: MDB_dbi = 0;
        let mut txn = try!(self.create_transaction(None, 0));
        let db_res = match db_name {
            None => unsafe { mdb_dbi_open(txn.handle, ptr::null(), flags.bits(), &mut dbi) },
            Some(db_name) => {
                db_name.with_c_str(|c_name| unsafe {
                    mdb_dbi_open(txn.handle, c_name, flags.bits(), &mut dbi)
                })
            }
        };

        try_mdb!(db_res);
        try!(txn.commit());
        Ok(dbi)
    }

    fn get_db_by_name<'a>(&'a self, db_name: &'a str, flags: DbFlags) -> MdbResult<Database> {
        assert_state_eq!(env, self.state, EnvOpened);

        let guard = self.db_cache.lock();
        let ref cell = *guard;
        let cache = unsafe { cell.get() };

        unsafe {
            let tmp = (*cache).find_equiv(&db_name);
            if tmp.is_some() {
                return Ok(Database::new_with_handle(tmp.unwrap().handle, false))
            }
        }

        let dbi = try!(self.create_db(Some(db_name), flags));
        let db = Database::new_with_handle(dbi, true);
        unsafe { (*cache).insert(db_name.to_string(), db) };

        match unsafe { (*cache).find_equiv(&db_name) } {
            Some(db) => Ok(Database::new_with_handle(db.handle, false)),
            _ => Err(InvalidPath)
        }
    }

    /// Returns or creates a named database
    ///
    /// Note: set_maxdbis should be called before
    pub fn get_or_create_db<'a>(&'a self, name: &'a str, flags: DbFlags) -> MdbResult<Database> {
        // FIXME: MDB_CREATE should be included only in read-write Environment
        self.get_db_by_name(name, flags | DbCreate)
    }

    /// Returns default database
    pub fn get_default_db<'a>(&'a self, flags: DbFlags) -> MdbResult<Database> {
        // FIXME: cache default DB
        let dbi = try!(self.create_db(None, flags));
        Ok(Database::new_with_handle(dbi, false))
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

pub struct NativeTransaction<'a> {
    handle: *const MDB_txn,
    flags: uint,
    state: TransactionState,
}

impl<'a> NativeTransaction<'a> {
    fn new_with_handle(h: *const MDB_txn, flags: uint) -> NativeTransaction<'a> {
        NativeTransaction {
            handle: h,
            flags: flags,
            state: TxnStateNormal }
    }

    fn is_readonly(&self) -> bool {
        (self.flags as u32 & MDB_RDONLY) == MDB_RDONLY
    }

    fn commit(&mut self) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        try_mdb!(unsafe { mdb_txn_commit(self.handle) } );
        self.state = if self.is_readonly() {
            TxnStateReleased
        } else {
            TxnStateInvalid
        };
        Ok(())
    }

    fn abort(&mut self) {
        if self.state != TxnStateNormal {
            debug!("Can't abort transaction: current state {}", self.state)
        } else {
            unsafe { mdb_txn_abort(self.handle); }
            self.state = if self.is_readonly() {
                TxnStateReleased
            } else {
                TxnStateInvalid
            };
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
        Ok(NativeTransaction::new_with_handle(out, flags as uint))
    }

    /// Used in Drop to switch state
    fn silent_abort(&mut self) {
        unsafe {mdb_txn_abort(self.handle);}
        self.state = TxnStateInvalid;
    }

    fn get_value<T: FromMdbValue<'a>>(&'a self, db: &Database, key: &'a ToMdbValue<'a>) -> MdbResult<T> {
        let key_val = key.to_mdb_value();
        unsafe {
            let mut data_val: MdbValue = std::mem::zeroed();
            try_mdb!(mdb_get(self.handle, db.handle, &key_val.value, &mut data_val.value));
            Ok(FromMdbValue::from_mdb_value(mem::transmute(&data_val)))
        }
    }

    pub fn get<'a, T: FromMdbValue<'a>>(&'a self, db: &Database, key: &'a ToMdbValue<'a>) -> MdbResult<T> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        self.get_value(db, key)
    }

    fn set_value<'a>(&'a self, db: &Database, key: &'a ToMdbValue<'a>, value: &'a ToMdbValue<'a>) -> MdbResult<()> {
        self.set_value_with_flags(db, key, value, 0)
    }

    fn set_value_with_flags<'a>(&'a self, db: &Database, key: &'a ToMdbValue<'a>, value: &'a ToMdbValue<'a>, flags: c_uint) -> MdbResult<()> {
        unsafe {
            let key_val = key.to_mdb_value();
            let data_val = value.to_mdb_value();

            lift_mdb!(mdb_put(self.handle, db.handle, &key_val.value, &data_val.value, flags))
        }
    }

    /// Sets a new value for key, in case of enabled duplicates
    /// it actually appends a new value
    // FIXME: add explicit append function
    // FIXME: think about creating explicit separation of
    // all traits for databases with dup keys
    pub fn set<'a>(&'a self, db: &Database, key: &'a ToMdbValue<'a>, value: &'a ToMdbValue<'a>) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        self.set_value(db, key, value)
    }

    /// Deletes all values by key
    fn del_value<'a>(&'a self, db: &Database, key: &'a ToMdbValue<'a>) -> MdbResult<()> {
        unsafe {
            let key_val = key.to_mdb_value();
            lift_mdb!(mdb_del(self.handle, db.handle, &key_val.value, std::ptr::null()))
        }
    }

    /// If duplicate keys are allowed deletes value for key which is equal to data
    pub fn del_exact_value<'a>(&'a self, db: &Database, key: &'a ToMdbValue<'a>, data: &'a ToMdbValue<'a>) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        unsafe {
            let key_val = key.to_mdb_value();
            let data_val = data.to_mdb_value();

            lift_mdb!(mdb_del(self.handle, db.handle, &key_val.value, &data_val.value))
        }
    }

    /// Deletes all values for key
    pub fn del<'a>(&'a self, db: &Database, key: &'a ToMdbValue<'a>) -> MdbResult<()> {
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

    /// Commits transaction, moves it out
    pub fn commit(self) -> MdbResult<()> {
        //self.inner.commit()
        let mut t = self;
        t.inner.commit()
    }

    /// Aborts transaction, moves it out
    pub fn abort(self) {
        let mut t = self;
        t.inner.abort();
    }
}

impl<'a> WriteTransaction<'a> for Transaction<'a> {
    fn get_write_transaction(&'a self) -> &'a NativeTransaction {
        &self.inner
    }
}

impl<'a> ReadTransaction<'a> for Transaction<'a> {
    fn get_read_transaction(&'a self) -> &'a NativeTransaction {
        &self.inner
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

    /// Aborts transaction. But readonly transaction could be
    /// reused later by calling `renew`
    pub fn abort(&mut self) {
        self.inner.abort();
    }

    /// Resets read only transaction, handle is kept. Must be followed
    /// by call to `renew`
    pub fn reset(&mut self) {
        self.inner.reset();
    }

    /// Acquires a new reader lock after transaction
    /// `abort` or `reset`
    pub fn renew(&mut self) -> MdbResult<()> {
        self.inner.renew()
    }
}

impl<'a> ReadTransaction<'a> for ReadonlyTransaction<'a> {
    fn get_read_transaction(&'a self) -> &'a NativeTransaction {
        &self.inner
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

    fn move_to<T: 'a>(&mut self, key: Option<&'a T>, op: MDB_cursor_op) -> MdbResult<()>
        where T: ToMdbValue<'a> + Clone {
        // Even if we don't ask for any data and want only to set a position
        // MDB still insists in writing back key and data to provided pointers
        // it's actually not that big deal, considering no actual data copy happens
        self.data_val = unsafe {std::mem::zeroed()};
        self.key_val = match key {
            Some(k) => k.to_mdb_value().value,
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
    pub fn to_key<T:'a>(&mut self, key: &'a T) -> MdbResult<()> where T: ToMdbValue<'a>+Clone{
        self.move_to(Some(key), MDB_SET)
    }

    /// Moves cursor to first entry for key greater than
    /// or equal to ke
    pub fn to_gte_key<T: 'a>(&mut self, key: &'a T) -> MdbResult<()> where T: ToMdbValue<'a>+Clone{
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
    pub fn get<T: FromMdbValue<'a>, U: FromMdbValue<'a>>(&mut self) -> MdbResult<(T, U)> {
        unsafe {
            let mut key_val: MdbValue = std::mem::zeroed();
            let mut data_val: MdbValue = std::mem::zeroed();
            try_mdb!(mdb_cursor_get(self.handle, &mut key_val.value, &mut data_val.value, MDB_GET_CURRENT));

            Ok((FromMdbValue::from_mdb_value(mem::transmute(&key_val)),
                FromMdbValue::from_mdb_value(mem::transmute(&data_val))))
        }
    }

    fn get_plain(&self) -> (MdbValue<'a>, MdbValue<'a>) {
        let k: MdbValue<'a> = MdbValue { value: self.key_val };
        let v: MdbValue<'a> = MdbValue { value: self.data_val };
        (k, v)
    }

    fn set_value<'a>(&mut self, key:Option<&'a ToMdbValue<'a>>, value: &'a ToMdbValue<'a>, flags: c_uint) -> MdbResult<()> {
        let data_val = value.to_mdb_value();
        let key_val = unsafe {
            match  key {
                Some(k) => k.to_mdb_value(),
                _ => std::mem::zeroed()
            }
        };

        lift_mdb!(unsafe {mdb_cursor_put(self.handle, &key_val.value, &data_val.value, flags)})
    }

    /// Overwrites value for current item
    /// Note: overwrites max cur_value.len() bytes
    pub fn set<'a>(&mut self, value: &'a ToMdbValue<'a>) -> MdbResult<()> {
        self.set_value(None, value, MDB_CURRENT)
    }

    /*
    /// Adds a new value if it doesn't exist yet
    pub fn upsert(&mut self, key: &ToMdbValue, value: &ToMdbValue) -> MdbResult<()> {
        self.set_value(Some(key), value, MDB_NOOVERWRITE)
    }
    */

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
        lift_mdb!(unsafe {mdb_cursor_count(self.handle, &mut tmp)}, tmp)
    }
}

#[unsafe_destructor]
impl<'a> Drop for Cursor<'a> {
    fn drop(&mut self) {
        unsafe { mdb_cursor_close(std::mem::transmute(self.handle)) };
    }
}

pub struct CursorValue<'cursor> {
    key: MdbValue<'cursor>,
    value: MdbValue<'cursor>,
}

/// CursorValue performs lazy data extraction from iterator
/// avoiding any data conversions and memory copy. Lifetime
/// is limited to iterator lifetime
impl<'cursor> CursorValue<'cursor> {
    pub fn get_key<T: FromMdbValue<'cursor>>(&'cursor self) -> T {
        FromMdbValue::from_mdb_value(&self.key)
    }

    pub fn get_value<T: FromMdbValue<'cursor>>(&'cursor self) -> T {
        FromMdbValue::from_mdb_value(&self.value)
    }

    pub fn get<T: FromMdbValue<'cursor>, U: FromMdbValue<'cursor>>(&'cursor self) -> (T, U) {
        (FromMdbValue::from_mdb_value(&self.key),  FromMdbValue::from_mdb_value(&self.value))
    }
}

pub struct CursorKeyRangeIter<'a> {
    cursor: Cursor<'a>,
    start_key: MdbValue<'a>,
    end_key: MdbValue<'a>,
    initialized: bool
}

impl<'a> CursorKeyRangeIter<'a> {
    pub fn new(cursor: Cursor<'a>, start_key: &'a ToMdbValue<'a>, end_key: &'a ToMdbValue<'a>) -> CursorKeyRangeIter<'a> {
        CursorKeyRangeIter {
            cursor: cursor,
            start_key: start_key.to_mdb_value(),
            end_key: end_key.to_mdb_value(),
            initialized: false
        }
    }

    // Moves out cursor for further usage
    pub fn unwrap(self) -> Cursor<'a> {
        self.cursor
    }
}

impl<'a> Iterator<CursorValue<'a>> for CursorKeyRangeIter<'a> {
    fn next(&mut self) -> Option<CursorValue<'a>> {
        let move_res = if !self.initialized {
            self.initialized = true;
            let tmp = MdbValue {
                value: self.start_key.value
            };
            unsafe {
                self.cursor.to_gte_key(mem::transmute::<&MdbValue, &'a MdbValue<'a>>(&tmp))
            }
        } else {
            self.cursor.to_next_key()
        };

        if move_res.is_err() {
            None
        } else {
            let (k, v) = self.cursor.get_plain();
            let cmp_res = unsafe {mdb_cmp(self.cursor.txn.handle, self.cursor.db.handle, &k.value, &self.end_key.value)};

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

impl<'a> CursorIter<'a> {
    pub fn new(cursor: Cursor<'a>) -> CursorIter<'a> {
        CursorIter {
            cursor: cursor,
            initialized: false
        }
    }

    // Moves out corresponding cursor
    pub fn unwrap(self) -> Cursor<'a> {
        self.cursor
    }
}

impl<'a> Iterator<CursorValue<'a>> for CursorIter<'a> {
    fn next(&mut self) -> Option<CursorValue<'a>> {
        let move_res = if !self.initialized {
            self.initialized = true;
            self.cursor.to_first()
        } else {
            self.cursor.to_next_key()
        };

        if move_res.is_err() {
            None
        } else {
            let (k, v) = self.cursor.get_plain();
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
    use super::{Environment, EnvFlags, DbFlags};

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

                    match env.open(&path, EnvFlags::empty(), 0o755) {
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

                            match env.get_default_db(DbFlags::empty()) {
                                Ok(db) => {
                                    let key = "hello".to_string();
                                    let value = "world".to_string();

                                    match env.new_transaction() {
                                        Ok(txn) => {
                                            match db.set(&txn, &key, &value) {
                                                Ok(_) => {
                                                    match db.get::<String>(&txn, &key) {
                                                        Ok(v) => assert!(v.as_slice() == value.as_slice(), "Written {:?} and read {:?}", value.as_slice(), v.as_slice()),
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
            let _ = env.open(&path, EnvFlags::empty(), 0o755);
            let _ = env.set_maxdbs(5);

            let db = env.get_default_db(DbFlags::empty()).unwrap();
            let txn = env.new_transaction().unwrap();

            let test_key1 = "key1".to_string();
            let test_data1 = "value1".to_string();
            let test_data2 = "value2".to_string();

            assert!(db.get::<()>(&txn, &test_key1).is_err(), "Key shouldn't exist yet");

            let _ = db.set(&txn, &test_key1, &test_data1);
            let v: String = db.get(&txn, &test_key1).unwrap();
            assert!(v.as_slice() == test_data1.as_slice(), "Data written differs from data read");

            let _ = db.set(&txn, &test_key1, &test_data2);
            let v: String = db.get(&txn, &test_key1).unwrap();
            assert!(v.as_slice() == test_data2.as_slice(), "Data written differs from data read");

            let _ = db.del(&txn, &test_key1);
            assert!(db.get::<()>(&txn, &test_key1).is_err(), "Key should be deleted");
        });
    }

    #[test]
    fn test_multiple_values() {
        let path = Path::new("multiple-values");
        test_db_in_path(&path, || {
            let mut env = Environment::new().unwrap();
            let _ = env.open(&path, EnvFlags::empty(), 0o755);
            let _ = env.set_maxdbs(5);

            let db = env.get_default_db(super::DbAllowDups).unwrap();
            let txn = env.new_transaction().unwrap();

            let test_key1 = "key1".to_string();
            let test_data1 = "value1".to_string();
            let test_data2 = "value2".to_string();

            assert!(db.get::<()>(&txn, &test_key1).is_err(), "Key shouldn't exist yet");

            let _ = db.set(&txn, &test_key1, &test_data1);
            let v: String = db.get(&txn, &test_key1).unwrap();
            assert!(v.as_slice() == test_data1.as_slice(), "Data written differs from data read");

            let _ = db.set(&txn, &test_key1, &test_data2);
            let v: String = db.get(&txn, &test_key1).unwrap();
            assert!(v.as_slice() == test_data1.as_slice(), "It should still return first value");

            let _ = db.del_exact(&txn, &test_key1, &test_data1);

            let v: String = db.get(&txn, &test_key1).unwrap();
            assert!(v.as_slice() == test_data2.as_slice(), "It should return second value");
            let _ = db.del(&txn, &test_key1);

            assert!(db.get::<()>(&txn, &test_key1).is_err(), "Key shouldn't exist anymore!");
        });
    }

    #[test]
    fn test_cursors() {
        let path = Path::new("cursors");
        test_db_in_path(&path, || {
            let mut env = Environment::new().unwrap();
            let _ = env.set_maxdbs(5);
            let _ = env.open(&path, EnvFlags::empty(), 0o755);

            let db = env.get_default_db(super::DbAllowDups).unwrap();
            let txn = env.new_transaction().unwrap();

            let test_key1 = "key1".to_string();
            let test_key2 = "key2".to_string();
            let test_values: Vec<String> = vec!("value1".to_string(), "value2".to_string(), "value3".to_string(), "value4".to_string());

            assert!(db.get::<()>(&txn, &test_key1).is_err(), "Key shouldn't exist yet");

            for t in test_values.iter() {
                let _ = db.set(&txn, &test_key1, t);
                let _ = db.set(&txn, &test_key2, t);
            }

            let mut cursor = db.new_cursor(&txn).unwrap();
            assert!(cursor.to_first().is_ok());

            assert!(cursor.to_key(&test_key1).is_ok());
            assert!(cursor.item_count().unwrap() == 4);

            assert!(cursor.del_single().is_ok());
            assert!(cursor.item_count().unwrap() == 3);

            assert!(cursor.to_key(&test_key1).is_ok());
            let new_value = "testme".to_string();

            assert!(cursor.set(&new_value).is_ok());
            let (_, v): ((), String) = cursor.get().unwrap();

            // NOTE: this asserting will work once new_value is
            // of the same length as it is inplace change
            assert!(v.as_slice() == new_value.as_slice());

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
            assert!(env.open(&path, EnvFlags::empty(), 0o755).is_ok());
            assert!(env.get_or_create_db("test-db", DbFlags::empty()).is_ok());
        });
    }

    /*
    #[test]
    fn test_compilation_of_moved_items() {
        let path = Path::new("dbcom");
        test_db_in_path(&path, || {
            let mut env = Environment::new().unwrap();
            assert!(env.set_maxdbs(5).is_ok());
            assert!(env.open(&path, 0, 0o755).is_ok());

            let db = env.get_default_db(0).unwrap();
            let mut txn = env.new_transaction().unwrap();

            txn.commit();

            let test_key1 = "key1";
            let test_data1 = "value1";

            assert!(txn.get::<()>(&db, &test_key1).is_err(), "Key shouldn't exist yet");
        })
    }
    */
}
