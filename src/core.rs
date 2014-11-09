//! High level wrapper of LMDB APIs
//!
//! Requires knowledge of LMDB terminology
//!
//! # Environment
//!
//! Environment is actually the center point of LMDB, it's a container
//! of everything else. As some settings couldn't be adjusted after
//! opening, `Environment` is constructed using `EnvBuilder`, which
//! sets up maximum size, maximum count of named databases, maximum
//! readers which could be used from different threads without locking
//! and so on.
//!
//! # Database
//!
//! Actual key-value store. The most crucial aspect is whether a database
//! allows duplicates or not. It is specified on creation and couldn't be
//! changed later. Entries for the same key are called `items`.
//!
//! There are a couple of optmizations to use, like marking
//! keys or data as integer, allowing sorting using reverse key, marking
//! keys/data as fixed size.
//!
//! # Transaction
//!
//! Absolutely every db operation happens in a transaction. It could
//! be a read-only transaction (reader), which is lockless and therefore
//! cheap. Or it could be a read-write transaction, which is unique, i.e.
//! there could be only one writer at a time.
//!
//! While readers are cheap and lockless, they work better being short-lived
//! as in other case they may lock pages from being reused. Readers have
//! a special API for marking as finished and renewing.
//!
//! It is perfectly fine to create nested transactions.
//!
//!
//! # Example
//!

#![allow(non_upper_case_globals)]

use std;
use std::cell::{UnsafeCell};
use std::collections::HashMap;
use libc::{mod, c_int, c_uint, size_t, c_void};
use std::io::fs::PathExtensions;
use std::io::FilePermission;
use std::mem;
use std::ptr;
use std::result::Result;
use sync::{Mutex};

pub use self::errors::{MdbError, NotFound, InvalidPath, StateError};
use ffi;
use ffi::types::MDB_val;
use traits::{ToMdbValue, FromMdbValue};

macro_rules! lift_mdb {
    ($e:expr) => (lift_mdb!($e, ()));
    ($e:expr, $r:expr) => (
        {
            let t = $e;
            match t {
                ffi::MDB_SUCCESS => Ok($r),
                _ => return Err(MdbError::new_with_code(t))
            }
        })
}

macro_rules! try_mdb {
        ($e:expr) => (
        {
            let t = $e;
            match t {
                ffi::MDB_SUCCESS => (),
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

    #[unstable]
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

    #[unstable]
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

#[stable]
pub type MdbResult<T> = Result<T, MdbError>;

#[experimental]
pub trait ReadTransaction {
    fn get_read_transaction<'a>(&'a self) -> &'a NativeTransaction;
}

#[experimental]
pub trait WriteTransaction: ReadTransaction {
    fn get_write_transaction<'a>(&'a self) -> &'a NativeTransaction;
}

bitflags! {
    #[doc = "A set of environment flags which could be changed after opening"]
    #[unstable]
    flags EnvFlags: c_uint {
        #[doc="Don't flush system buffers to disk when committing a transaction. This optimization means a system crash can corrupt the database or lose the last transactions if buffers are not yet flushed to disk. The risk is governed by how often the system flushes dirty buffers to disk and how often mdb_env_sync() is called. However, if the filesystem preserves write order and the MDB_WRITEMAP flag is not used, transactions exhibit ACI (atomicity, consistency, isolation) properties and only lose D (durability). I.e. database integrity is maintained, but a system crash may undo the final transactions. Note that (MDB_NOSYNC | MDB_WRITEMAP) leaves the system with no hint for when to write transactions to disk, unless mdb_env_sync() is called. (MDB_MAPASYNC | MDB_WRITEMAP) may be preferable. This flag may be changed at any time using mdb_env_set_flags()."]
        const EnvNoSync      = ffi::MDB_NOSYNC,
        #[doc="Flush system buffers to disk only once per transaction, omit the metadata flush. Defer that until the system flushes files to disk, or next non-MDB_RDONLY commit or mdb_env_sync(). This optimization maintains database integrity, but a system crash may undo the last committed transaction. I.e. it preserves the ACI (atomicity, consistency, isolation) but not D (durability) database property. This flag may be changed at any time using mdb_env_set_flags()."]
        const EnvNoMetaSync  = ffi::MDB_NOMETASYNC,
        #[doc="When using MDB_WRITEMAP, use asynchronous flushes to disk. As with MDB_NOSYNC, a system crash can then corrupt the database or lose the last transactions. Calling mdb_env_sync() ensures on-disk database integrity until next commit. This flag may be changed at any time using mdb_env_set_flags()."]
        const EnvMapAsync    = ffi::MDB_MAPASYNC,
        #[doc="Don't initialize malloc'd memory before writing to unused spaces in the data file. By default, memory for pages written to the data file is obtained using malloc. While these pages may be reused in subsequent transactions, freshly malloc'd pages will be initialized to zeroes before use. This avoids persisting leftover data from other code (that used the heap and subsequently freed the memory) into the data file. Note that many other system libraries may allocate and free memory from the heap for arbitrary uses. E.g., stdio may use the heap for file I/O buffers. This initialization step has a modest performance cost so some applications may want to disable it using this flag. This option can be a problem for applications which handle sensitive data like passwords, and it makes memory checkers like Valgrind noisy. This flag is not needed with MDB_WRITEMAP, which writes directly to the mmap instead of using malloc for pages. The initialization is also skipped if MDB_RESERVE is used; the caller is expected to overwrite all of the memory that was reserved in that case. This flag may be changed at any time using mdb_env_set_flags()."]
        const EnvNoMemInit   = ffi::MDB_NOMEMINIT
    }
}

bitflags! {
    #[doc = "A set of all environment flags"]
    #[unstable]
    flags EnvCreateFlags: c_uint {
        #[doc="Use a fixed address for the mmap region. This flag must be"]
        #[doc=" specified when creating the environment, and is stored persistently"]
        #[doc=" in the environment. If successful, the memory map will always reside"]
        #[doc=" at the same virtual address and pointers used to reference data items"]
        #[doc=" in the database will be constant across multiple invocations. This "]
        #[doc="option may not always work, depending on how the operating system has"]
        #[doc=" allocated memory to shared libraries and other uses. The feature is highly experimental."]
        const EnvCreateFixedMap    = ffi::MDB_FIXEDMAP,
        #[doc="By default, LMDB creates its environment in a directory whose"]
        #[doc=" pathname is given in path, and creates its data and lock files"]
        #[doc=" under that directory. With this option, path is used as-is"]
        #[doc=" for the database main data file. The database lock file is"]
        #[doc=" the path with \"-lock\" appended."]
        const EnvCreateNoSubDir    = ffi::MDB_NOSUBDIR,
        #[doc="Don't flush system buffers to disk when committing a"]
        #[doc=" transaction. This optimization means a system crash can corrupt"]
        #[doc=" the database or lose the last transactions if buffers are not"]
        #[doc=" yet flushed to disk. The risk is governed by how often the"]
        #[doc=" system flushes dirty buffers to disk and how often"]
        #[doc=" mdb_env_sync() is called. However, if the filesystem preserves"]
        #[doc=" write order and the MDB_WRITEMAP flag is not used, transactions"]
        #[doc=" exhibit ACI (atomicity, consistency, isolation) properties and"]
        #[doc=" only lose D (durability). I.e. database integrity is"]
        #[doc=" maintained, but a system crash may undo the final"]
        #[doc=" transactions. Note that (MDB_NOSYNC | MDB_WRITEMAP) leaves"]
        #[doc=" the system with no hint for when to write transactions to"]
        #[doc=" disk, unless mdb_env_sync() is called."]
        #[doc=" (MDB_MAPASYNC | MDB_WRITEMAP) may be preferable. This flag"]
        #[doc=" may be changed at any time using mdb_env_set_flags()."]
        const EnvCreateNoSync      = ffi::MDB_NOSYNC,
        #[doc="Open the environment in read-only mode. No write operations"]
        #[doc=" will be allowed. LMDB will still modify the lock file - except"]
        #[doc=" on read-only filesystems, where LMDB does not use locks."]
        const EnvCreateReadOnly    = ffi::MDB_RDONLY,
        #[doc="Flush system buffers to disk only once per transaction,"]
        #[doc=" omit the metadata flush. Defer that until the system flushes"]
        #[doc=" files to disk, or next non-MDB_RDONLY commit or mdb_env_sync()."]
        #[doc=" This optimization maintains database integrity, but a system"]
        #[doc=" crash may undo the last committed transaction. I.e. it"]
        #[doc=" preserves the ACI (atomicity, consistency, isolation) but"]
        #[doc=" not D (durability) database property. This flag may be changed"]
        #[doc=" at any time using mdb_env_set_flags()."]
        const EnvCreateNoMetaSync  = ffi::MDB_NOMETASYNC,
        #[doc="Use a writeable memory map unless MDB_RDONLY is set. This is"]
        #[doc="faster and uses fewer mallocs, but loses protection from"]
        #[doc="application bugs like wild pointer writes and other bad updates"]
        #[doc="into the database. Incompatible with nested"]
        #[doc="transactions. Processes with and without MDB_WRITEMAP on the"]
        #[doc="same environment do not cooperate well."]
        const EnvCreateWriteMap    = ffi::MDB_WRITEMAP,
        #[doc="When using MDB_WRITEMAP, use asynchronous flushes to disk. As"]
        #[doc="with MDB_NOSYNC, a system crash can then corrupt the database or"]
        #[doc="lose the last transactions. Calling mdb_env_sync() ensures"]
        #[doc="on-disk database integrity until next commit. This flag may be"]
        #[doc="changed at any time using mdb_env_set_flags()."]
        const EnvCreataMapAsync    = ffi::MDB_MAPASYNC,
        #[doc="Don't use Thread-Local Storage. Tie reader locktable slots to"]
        #[doc="ffi::MDB_txn objects instead of to threads. I.e. mdb_txn_reset()"]
        #[doc="keeps the slot reseved for the ffi::MDB_txn object. A thread may"]
        #[doc="use parallel read-only transactions. A read-only transaction may"]
        #[doc="span threads if the user synchronizes its use. Applications that"]
        #[doc="multiplex many user threads over individual OS threads need this"]
        #[doc="option. Such an application must also serialize the write"]
        #[doc="transactions in an OS thread, since LMDB's write locking is"]
        #[doc="unaware of the user threads."]
        const EnvCreateNoTls       = ffi::MDB_NOTLS,
        #[doc="Don't do any locking. If concurrent access is anticipated, the"]
        #[doc="caller must manage all concurrency itself. For proper operation"]
        #[doc="the caller must enforce single-writer semantics, and must ensure"]
        #[doc="that no readers are using old transactions while a writer is"]
        #[doc="active. The simplest approach is to use an exclusive lock so"]
        #[doc="that no readers may be active at all when a writer begins. "]
        const EnvCreateNoLock      = ffi::MDB_NOLOCK,
        #[doc="Turn off readahead. Most operating systems perform readahead on"]
        #[doc="read requests by default. This option turns it off if the OS"]
        #[doc="supports it. Turning it off may help random read performance"]
        #[doc="when the DB is larger than RAM and system RAM is full. The"]
        #[doc="option is not implemented on Windows."]
        const EnvCreateNoReadAhead = ffi::MDB_NORDAHEAD,
        #[doc="Don't initialize malloc'd memory before writing to unused spaces"]
        #[doc="in the data file. By default, memory for pages written to the"]
        #[doc="data file is obtained using malloc. While these pages may be"]
        #[doc="reused in subsequent transactions, freshly malloc'd pages will"]
        #[doc="be initialized to zeroes before use. This avoids persisting"]
        #[doc="leftover data from other code (that used the heap and"]
        #[doc="subsequently freed the memory) into the data file. Note that"]
        #[doc="many other system libraries may allocate and free memory from"]
        #[doc="the heap for arbitrary uses. E.g., stdio may use the heap for"]
        #[doc="file I/O buffers. This initialization step has a modest"]
        #[doc="performance cost so some applications may want to disable it"]
        #[doc="using this flag. This option can be a problem for applications"]
        #[doc="which handle sensitive data like passwords, and it makes memory"]
        #[doc="checkers like Valgrind noisy. This flag is not needed with"]
        #[doc="MDB_WRITEMAP, which writes directly to the mmap instead of using"]
        #[doc="malloc for pages. The initialization is also skipped if"]
        #[doc="MDB_RESERVE is used; the caller is expected to overwrite all of"]
        #[doc="the memory that was reserved in that case. This flag may be"]
        #[doc="changed at any time using mdb_env_set_flags()."]
        const EnvCreateNoMemInit   = ffi::MDB_NOMEMINIT
    }
}

bitflags! {
    #[doc = "A set of database flags"]
    #[stable]
    flags DbFlags: c_uint {
        #[doc="Keys are strings to be compared in reverse order, from the"]
        #[doc=" end of the strings to the beginning. By default, Keys are"]
        #[doc=" treated as strings and compared from beginning to end."]
        const DbReverseKey   = ffi::MDB_REVERSEKEY,
        #[doc="Duplicate keys may be used in the database. (Or, from another"]
        #[doc="perspective, keys may have multiple data items, stored in sorted"]
        #[doc="order.) By default keys must be unique and may have only a"]
        #[doc="single data item."]
        const DbAllowDups    = ffi::MDB_DUPSORT,
        #[doc="Keys are binary integers in native byte order. Setting this"]
        #[doc="option requires all keys to be the same size, typically"]
        #[doc="sizeof(int) or sizeof(size_t)."]
        const DbIntKey       = ffi::MDB_INTEGERKEY,
        #[doc="This flag may only be used in combination with"]
        #[doc="ffi::MDB_DUPSORT. This option tells the library that the data"]
        #[doc="items for this database are all the same size, which allows"]
        #[doc="further optimizations in storage and retrieval. When all data"]
        #[doc="items are the same size, the ffi::MDB_GET_MULTIPLE and"]
        #[doc="ffi::MDB_NEXT_MULTIPLE cursor operations may be used to retrieve"]
        #[doc="multiple items at once."]
        const DbDupFixed     = ffi::MDB_DUPFIXED,
        #[doc="This option specifies that duplicate data items are also"]
        #[doc="integers, and should be sorted as such."]
        const DbAllowIntDups = ffi::MDB_INTEGERDUP,
        #[doc="This option specifies that duplicate data items should be"]
        #[doc=" compared as strings in reverse order."]
        const DbReversedDups = ffi::MDB_REVERSEDUP,
        #[doc="Create the named database if it doesn't exist. This option"]
        #[doc=" is not allowed in a read-only transaction or a read-only"]
        #[doc=" environment."]
        const DbCreate       = ffi::MDB_CREATE,
    }
}

/// Database
#[unstable]
pub struct Database {
    handle: ffi::MDB_dbi,
    owns: bool
}

#[unstable]
impl Database {
    fn new_with_handle(handle: ffi::MDB_dbi, owns: bool) -> Database {
        Database { handle: handle, owns: owns }
    }

    /// Retrieves a value by key. In case of DbAllowDups it will be the first item
    pub fn get<'a,  V: FromMdbValue<'a, V>+'a>(&self, txn: &'a ReadTransaction, key: &ToMdbValue) -> MdbResult<MdbWrapper<'a, V>> {
        txn.get_read_transaction().get(self, key)
    }

    /// Sets value for key. In case of DbAllowDups it will add a new item
    pub fn set<K: ToMdbValue, V: ToMdbValue>(&self, txn: &WriteTransaction, key: &K, value: &V) -> MdbResult<()> {
        txn.get_write_transaction().set(self, key, value)
    }

    /// Deletes value for key.
    pub fn del<K: ToMdbValue>(&self, txn: &WriteTransaction, key: &K) -> MdbResult<()> {
        txn.get_write_transaction().del(self, key)
    }

    /// Should be used only with DbAllowDups. Deletes corresponding (key, value)
    pub fn del_item<K: ToMdbValue, V: ToMdbValue>(&self, txn: &WriteTransaction, key: &K, data: &V) -> MdbResult<()> {
        txn.get_write_transaction().del_item(self, key, data)
    }

    /// Returns a new cursor
    pub fn new_cursor<'a>(&'a self, txn: &'a ReadTransaction) -> MdbResult<Cursor<'a>> {
        txn.get_read_transaction().new_cursor(self)
    }

    /// Deletes current db, also moves it out
    pub fn del_db(self, txn: &WriteTransaction) -> MdbResult<()> {
        txn.get_write_transaction().del_db(self)
    }

    /// Removes all key/values from db
    pub fn clear<'a>(&self, txn: &'a WriteTransaction) -> MdbResult<()> {
        txn.get_write_transaction().empty_db(self)
    }

    /// Returns an iterator for all values in database
    pub fn iter<'a>(&'a self, txn: &'a ReadTransaction) -> MdbResult<CursorIterator<'a, CursorIter>> {
        txn.get_read_transaction().new_cursor(self)
            .and_then(|c| Ok(CursorIterator::wrap(c, CursorIter)))
    }

    /// Returns an iterator for values between start_key and end_key.
    /// Currently it works only for unique keys (i.e. it will skip
    /// multiple items when DB created with ffi::MDB_DUPSORT).
    /// Iterator is valid while cursor is valid
    pub fn keyrange<'c, 't: 'c, 'db: 't, K: ToMdbValue + 'c>(&'db self, txn: &'t ReadTransaction, start_key: &'c K, end_key: &'c K)
                                       -> MdbResult<CursorIterator<'c, CursorKeyRangeIter>> {
        txn.get_read_transaction().new_cursor(self)
            .and_then(|c| {
                let key_range = CursorKeyRangeIter::new(start_key, end_key);
                let wrap = CursorIterator::wrap(c, key_range);
                Ok(wrap)
            })
    }

    /// Returns an iterator for all items (i.e. values with same key)
    pub fn item_iter<'c, 't: 'c, 'db:'t, K: ToMdbValue>(&'db self, txn: &'t ReadTransaction,
                                                key: &K) -> MdbResult<CursorIterator<'c, CursorItemIter<'c>>> {
        txn.get_read_transaction().new_item_iter(self, key)
    }

    pub fn get_handle(&self) -> ffi::MDB_dbi {
        self.handle
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if self.owns {
            // FIXME: drop dbi handle
            // unsafe { ffi::mdb_dbi_close(self.handle) }
        }
    }

}

#[stable]
pub struct EnvBuilder {
    flags: EnvCreateFlags,
    max_readers: Option<uint>,
    max_dbs: Option<uint>,
    map_size: Option<u64>,
}

/// Constructs environment with settigs which couldn't be
/// changed after opening
#[stable]
impl EnvBuilder {
    pub fn new() -> EnvBuilder {
        EnvBuilder {
            flags: EnvCreateFlags::empty(),
            max_readers: None,
            max_dbs: None,
            map_size: None,
        }
    }

    /// Sets environment flags
    pub fn flags(mut self, flags: EnvCreateFlags) -> EnvBuilder {
        self.flags = flags;
        self
    }

    /// Sets max concurrent readers operating on environment
    pub fn max_readers(mut self, max_readers: uint) -> EnvBuilder {
        self.max_readers = Some(max_readers);
        self
    }

    /// Set max number of databases
    pub fn max_dbs(mut self, max_dbs: uint) -> EnvBuilder {
        self.max_dbs = Some(max_dbs);
        self
    }

    /// Sets max environment size, i.e. size in memory/disk of
    /// all data
    pub fn map_size(mut self, map_size: u64) -> EnvBuilder {
        self.map_size = Some(map_size);
        self
    }

    /// Opens environment in specified path
    pub fn open(self, path: &Path, perms: FilePermission) -> MdbResult<Environment> {
        let env: *mut ffi::MDB_env = ptr::null_mut();
        unsafe {
            let p_env: *mut *mut ffi::MDB_env = std::mem::transmute(&env);
            let _ = try_mdb!(ffi::mdb_env_create(p_env));
        }

        try_mdb!(unsafe { ffi::mdb_env_set_flags(env, self.flags.bits(), 1)});

        if let Some(map_size) = self.map_size {
            try_mdb!(unsafe { ffi::mdb_env_set_mapsize(env, map_size as size_t)});
        }

        if let Some(max_readers) = self.max_readers {
            try_mdb!(unsafe { ffi::mdb_env_set_maxreaders(env, max_readers as u32)});
        }

        if let Some(max_dbs) = self.max_dbs {
            try_mdb!(unsafe { ffi::mdb_env_set_maxdbs(env, max_dbs as u32)});
        }

        let _ = try!(EnvBuilder::check_path(path, self.flags, perms));

        let res = path.with_c_str(|c_path| {
            unsafe {
                ffi::mdb_env_open(mem::transmute(env), c_path, self.flags.bits,
                             perms.bits() as libc::mode_t)}
        });

        drop(self);

        match res {
            ffi::MDB_SUCCESS => {
                Ok(Environment::from_raw(env))
            },
            _ => {
                unsafe { ffi::mdb_env_close(mem::transmute(env)); }
                Err(MdbError::new_with_code(res))
            }
        }

    }

    fn check_path(path: &Path, flags: EnvCreateFlags, perms: FilePermission) -> MdbResult<()> {
        let as_file = flags.contains(EnvCreateNoSubDir);

        if as_file {
            // FIXME: check file existence/absence
            Ok(())
        } else {
            // There should be a directory before open
            match (path.exists(), path.is_dir()) {
                (false, _) => {
                    lift_mdb!(path.with_c_str(|c_path| unsafe {
                        libc::mkdir(c_path, perms.bits() as libc::mode_t)
                    }))
                },
                (true, true) => Ok(()),
                (true, false) => Err(InvalidPath),
            }
        }
    }

}

/// Represents LMDB Environment. Should be opened using `EnvBuilder`
#[unstable]
pub struct Environment {
    env: *mut ffi::MDB_env,
    db_cache: Mutex<UnsafeCell<HashMap<String, Database>>>,
}

#[unstable]
impl Environment {
    pub fn new() -> EnvBuilder {
        EnvBuilder::new()
    }

    fn from_raw(env: *mut ffi::MDB_env) -> Environment {
        Environment {
            env: env,
            db_cache: Mutex::new(UnsafeCell::new(HashMap::new())),
        }
    }

    pub fn stat(&self) -> MdbResult<ffi::MDB_stat> {
        let mut tmp: ffi::MDB_stat = unsafe { std::mem::zeroed() };
        lift_mdb!(unsafe { ffi::mdb_env_stat(self.env, &mut tmp)}, tmp)
    }

    pub fn info(&self) -> MdbResult<ffi::MDB_envinfo> {
        let mut tmp: ffi::MDB_envinfo = unsafe { std::mem::zeroed() };
        lift_mdb!(unsafe { ffi::mdb_env_info(self.env, &mut tmp)}, tmp)
    }

    /// Sync environment to disk
    pub fn sync(&self, force: bool) -> MdbResult<()> {
        lift_mdb!(unsafe { ffi::mdb_env_sync(self.env, if force {1} else {0})})
    }

    /// This one sets only flags which are available for change even
    /// after opening, see also [get_flags](#method.get_flags) and [get_all_flags](#method.get_all_flags)
    pub fn set_flags(&mut self, flags: EnvFlags, turn_on: bool) -> MdbResult<()> {
        lift_mdb!(unsafe {
            ffi::mdb_env_set_flags(self.env, flags.bits(), if turn_on {1} else {0})
        })
    }

    /// Get flags of environment, which could be changed after it was opened
    /// use [get_all_flags](#method.get_all_flags) if you need also creation time flags
    pub fn get_flags(&self) -> MdbResult<EnvFlags> {
        let tmp = try!(self.get_all_flags());
        Ok(EnvFlags::from_bits_truncate(tmp.bits()))
    }

    /// Get all flags of environment, including which were specified on creation
    /// See also [get_flags](#method.get_flags) if you're interested only in modifiable flags
    pub fn get_all_flags(&self) -> MdbResult<EnvCreateFlags> {
        let mut flags: c_uint = 0;
        lift_mdb!(unsafe {ffi::mdb_env_get_flags(self.env, &mut flags)}, EnvCreateFlags::from_bits_truncate(flags))
    }

    pub fn get_maxreaders(&self) -> MdbResult<c_uint> {
        let mut max_readers: c_uint = 0;
        lift_mdb!(unsafe {
            ffi::mdb_env_get_maxreaders(self.env, &mut max_readers)
        }, max_readers)
    }

    pub fn get_maxkeysize(&self) -> c_int {
        unsafe {ffi::mdb_env_get_maxkeysize(self.env)}
    }

    /// Creates a backup copy in specified file descriptor
    pub fn copy_to_fd(&self, fd: ffi::mdb_filehandle_t) -> MdbResult<()> {
        lift_mdb!(unsafe { ffi::mdb_env_copyfd(self.env, fd) })
    }

    /// Gets file descriptor of this environment
    pub fn get_fd(&self) -> MdbResult<ffi::mdb_filehandle_t> {
        let mut fd = 0;
        lift_mdb!({ unsafe { ffi::mdb_env_get_fd(self.env, &mut fd) }}, fd)
    }

    /// Creates a backup copy in specified path
    // FIXME: check who is responsible for creating path: callee or caller
    pub fn copy_to_path(&self, path: &Path) -> MdbResult<()> {
        path.with_c_str(|c_path| unsafe {
            lift_mdb!(ffi::mdb_env_copy(self.env, c_path))
        })
    }

    fn create_transaction(&self, parent: Option<NativeTransaction>, flags: c_uint) -> MdbResult<NativeTransaction> {
        let mut handle: *mut ffi::MDB_txn = ptr::null_mut();
        let parent_handle = match parent {
            Some(t) => t.handle,
            _ => ptr::null_mut()
        };

        lift_mdb!(unsafe { ffi::mdb_txn_begin(self.env, parent_handle, flags, &mut handle) },
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
        self.create_transaction(None, ffi::MDB_RDONLY)
            .and_then(|txn| Ok(ReadonlyTransaction::new_with_native(txn)))
    }

    fn create_db<'a>(&'a self, db_name: Option<&'a str>, flags: DbFlags) -> MdbResult<ffi::MDB_dbi> {
        let mut dbi: ffi::MDB_dbi = 0;
        let mut txn = try!(self.create_transaction(None, 0));
        let db_res = match db_name {
            None => unsafe { ffi::mdb_dbi_open(txn.handle, ptr::null(), flags.bits(), &mut dbi) },
            Some(db_name) => {
                db_name.with_c_str(|c_name| unsafe {
                    ffi::mdb_dbi_open(txn.handle, c_name, flags.bits(), &mut dbi)
                })
            }
        };

        try_mdb!(db_res);
        try!(txn.commit());
        Ok(dbi)
    }

    fn get_db_by_name<'a>(&'a self, db_name: &'a str, flags: DbFlags) -> MdbResult<Database> {
        let guard = self.db_cache.lock();
        let ref cell = *guard;
        let cache = unsafe { cell.get() };

        unsafe {
            let tmp = (*cache).get(&db_name.to_string());
            if tmp.is_some() {
                return Ok(Database::new_with_handle(tmp.unwrap().handle, false))
            }
        }

        let dbi = try!(self.create_db(Some(db_name), flags));
        let db = Database::new_with_handle(dbi, true);
        unsafe { (*cache).insert(db_name.to_string(), db) };

        match unsafe { (*cache).get(&db_name.to_string()) } {
            Some(db) => Ok(Database::new_with_handle(db.handle, false)),
            _ => Err(InvalidPath)
        }
    }

    /// Returns or creates a named database
    ///
    /// Note: set_maxdbis should be called before
    pub fn get_or_create_db<'a>(&'a self, name: &'a str, flags: DbFlags) -> MdbResult<Database> {
        // FIXME: ffi::MDB_CREATE should be included only in read-write Environment
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
            ffi::mdb_env_close(std::mem::transmute(self.env));
        }
    }
}

#[deriving(PartialEq, Show, Eq, Clone)]
enum TransactionState {
    TxnStateNormal,   // Normal, any operation possible
    TxnStateReleased, // Released (reset on readonly), has to be renewed
    TxnStateInvalid,  // Invalid, no further operation possible
}

#[experimental]
pub struct NativeTransaction {
    handle: *mut ffi::MDB_txn,
    flags: uint,
    state: TransactionState,
}

#[experimental]
impl NativeTransaction {
    fn new_with_handle(h: *mut ffi::MDB_txn, flags: uint) -> NativeTransaction {
        NativeTransaction {
            handle: h,
            flags: flags,
            state: TxnStateNormal
        }
    }

    fn is_readonly(&self) -> bool {
        (self.flags as u32 & ffi::MDB_RDONLY) == ffi::MDB_RDONLY
    }

    fn commit(&mut self) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        try_mdb!(unsafe { ffi::mdb_txn_commit(self.handle) } );
        self.state = if self.is_readonly() {
            TxnStateReleased
        } else {
            TxnStateInvalid
        };
        Ok(())
    }

    fn abort(&mut self) {
        if self.state != TxnStateNormal {
            error!("Can't abort transaction: current state {}", self.state)
        } else {
            unsafe { ffi::mdb_txn_abort(self.handle); }
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
            error!("Can't reset transaction: current state {}", self.state);
        } else {
            unsafe { ffi::mdb_txn_reset(self.handle); }
            self.state = TxnStateReleased;
        }
    }

    /// Acquires a new reader lock after it was released by reset
    fn renew(&mut self) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateReleased);
        try_mdb!(unsafe {ffi::mdb_txn_renew(self.handle)});
        self.state = TxnStateNormal;
        Ok(())
    }

    fn new_child(&self, flags: c_uint) -> MdbResult<NativeTransaction> {
        let mut out: *mut ffi::MDB_txn = ptr::null_mut();
        try_mdb!(unsafe { ffi::mdb_txn_begin(ffi::mdb_txn_env(self.handle), self.handle, flags, &mut out) });
        Ok(NativeTransaction::new_with_handle(out, flags as uint))
    }

    /// Used in Drop to switch state
    fn silent_abort(&mut self) {
        if self.state == TxnStateNormal {
            unsafe {ffi::mdb_txn_abort(self.handle);}
        }
        self.state = TxnStateInvalid;
    }

    fn get_value<'a, V: FromMdbValue<'a, V>+'a>(&'a self, db: &Database, key: &ToMdbValue) -> MdbResult<MdbWrapper<'a, V>> {
        let key_val = key.to_mdb_value();
        unsafe {
            let mut data_val: MdbValue = std::mem::zeroed();
            try_mdb!(ffi::mdb_get(self.handle, db.handle, mem::transmute(&key_val.value), &mut data_val.value));
            Ok(MdbWrapper::new(FromMdbValue::from_mdb_value(mem::transmute(&data_val))))
        }
    }

    pub fn get<'a, V: FromMdbValue<'a, V>+'a>(&'a self, db: &Database, key: &ToMdbValue) -> MdbResult<MdbWrapper<'a, V>> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        self.get_value(db, key)
    }

    fn set_value<K: ToMdbValue, V: ToMdbValue>(&self, db: &Database, key: &K, value: &V) -> MdbResult<()> {
        self.set_value_with_flags(db, key, value, 0)
    }

    fn set_value_with_flags<K: ToMdbValue, V: ToMdbValue>(&self, db: &Database, key: &K, value: &V, flags: c_uint) -> MdbResult<()> {
        unsafe {
            let mut key_val = key.to_mdb_value();
            let mut data_val = value.to_mdb_value();

            lift_mdb!(ffi::mdb_put(self.handle, db.handle, &mut key_val.value, &mut data_val.value, flags))
        }
    }

    /// Sets a new value for key, in case of enabled duplicates
    /// it actually appends a new value
    // FIXME: add explicit append function
    // FIXME: think about creating explicit separation of
    // all traits for databases with dup keys
    pub fn set<K: ToMdbValue, V: ToMdbValue>(&self, db: &Database, key: &K, value: &V) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        self.set_value(db, key, value)
    }

    /// Deletes all values by key
    fn del_value<T: ToMdbValue>(&self, db: &Database, key: &T) -> MdbResult<()> {
        unsafe {
            let mut key_val = key.to_mdb_value();
            lift_mdb!(ffi::mdb_del(self.handle, db.handle, &mut key_val.value, ptr::null_mut()))
        }
    }

    /// If duplicate keys are allowed deletes value for key which is equal to data
    pub fn del_item<'a, K: ToMdbValue, V: ToMdbValue>(&'a self, db: &'a Database, key: &K, data: &V) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        unsafe {
            let mut key_val = key.to_mdb_value();
            let mut data_val = data.to_mdb_value();

            lift_mdb!(ffi::mdb_del(self.handle, db.handle, &mut key_val.value, &mut data_val.value))
        }
    }

    /// Deletes all values for key
    pub fn del<K: ToMdbValue>(&self, db: &Database, key: &K) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        self.del_value(db, key)
    }

    /// creates a new cursor in current transaction tied to db
    pub fn new_cursor<'a>(&'a self, db: &'a Database) -> MdbResult<Cursor<'a>> {
        Cursor::<'a>::new(self, db)
    }

    /// Creates a new item cursor, i.e. cursor which navigates all
    /// values with the same key (if AllowsDup was specified)
    pub fn new_item_iter<'a, K: ToMdbValue>(&'a self, db: &'a Database, key: &K) -> MdbResult<CursorIterator<'a, CursorItemIter>> {
        let cursor = try!(self.new_cursor(db));
        let inner_iter = CursorItemIter::new(key);
        Ok(CursorIterator::wrap(cursor, inner_iter))
    }

    /// Deletes provided database completely
    pub fn del_db(&self, db: Database) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        unsafe {
            lift_mdb!(ffi::mdb_drop(self.handle, db.handle, 1))
        }
    }

    /// Empties provided database
    pub fn empty_db(&self, db: &Database) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TxnStateNormal);
        unsafe {
            lift_mdb!(ffi::mdb_drop(self.handle, db.handle, 0))
        }
    }

    pub fn get_handle(&self) -> *mut ffi::MDB_txn {
        self.handle
    }
}

#[unstable]
pub struct Transaction {
    inner: NativeTransaction,
}

#[unstable]
impl Transaction {
    fn new_with_native(txn: NativeTransaction) -> Transaction {
        Transaction {
            inner: txn
        }
    }

    pub fn new_child(&self) -> MdbResult<Transaction> {
        self.inner.new_child(0)
            .and_then(|txn| Ok(Transaction::new_with_native(txn)))
    }

    pub fn new_ro_child(&self) -> MdbResult<ReadonlyTransaction> {
        self.inner.new_child(ffi::MDB_RDONLY)
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

    pub fn get_handle(&self) -> *mut ffi::MDB_txn {
        self.inner.get_handle()
    }
}

impl WriteTransaction for Transaction {
    fn get_write_transaction<'a>(&'a self) -> &'a NativeTransaction {
        &self.inner
    }
}

impl ReadTransaction for Transaction {
    fn get_read_transaction<'a>(&'a self) -> &'a NativeTransaction {
        &self.inner
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.silent_abort();
    }
}

#[unstable]
pub struct ReadonlyTransaction {
    inner: NativeTransaction,
}

#[unstable]
impl ReadonlyTransaction {
    fn new_with_native(txn: NativeTransaction) -> ReadonlyTransaction {
        ReadonlyTransaction {
            inner: txn,
        }
    }

    pub fn new_ro_child(&self) -> MdbResult<ReadonlyTransaction> {
        self.inner.new_child(ffi::MDB_RDONLY)
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

impl ReadTransaction for ReadonlyTransaction {
    fn get_read_transaction<'a>(&'a self) -> &'a NativeTransaction {
        &self.inner
    }
}

impl Drop for ReadonlyTransaction {
    fn drop(&mut self) {
        self.inner.silent_abort();
    }
}

#[unstable]
pub struct Cursor<'txn> {
    handle: *mut ffi::MDB_cursor,
    data_val: ffi::MDB_val,
    key_val: ffi::MDB_val,
    txn: &'txn NativeTransaction,
    db: &'txn Database,
    valid_key: bool,
}

#[unstable]
impl<'txn> Cursor<'txn> {
    fn new<'db: 'txn>(txn: &'txn NativeTransaction, db: &'db Database) -> MdbResult<Cursor<'txn>> {
        let mut tmp: *mut ffi::MDB_cursor = std::ptr::null_mut();
        try_mdb!(unsafe { ffi::mdb_cursor_open(txn.handle, db.handle, &mut tmp) });
        Ok(Cursor {
            handle: tmp,
            data_val: unsafe { std::mem::zeroed() },
            key_val: unsafe { std::mem::zeroed() },
            txn: txn,
            db: db,
            valid_key: false,
        })
    }

    fn navigate(&mut self, op: ffi::MDB_cursor_op) -> MdbResult<()> {
        self.valid_key = false;

        let res = unsafe {
            ffi::mdb_cursor_get(self.handle, &mut self.key_val, &mut self.data_val, op)
        };
        match res {
            ffi::MDB_SUCCESS => {
                // MDB_SET is the only cursor operation which doesn't
                // writes back a new value. In this case any access to
                // cursor key value should cause a cursor retrieval
                // to get back pointer to database owned memory instead
                // of value used to set the cursor as it might be
                // already destroyed and there is no need to borrow it
                self.valid_key = op != ffi::MDB_SET;
                Ok(())
            },
            e => Err(MdbError::new_with_code(e))
        }
    }

    fn move_to<K, V>(&mut self, key: &K, value: Option<&V>, op: ffi::MDB_cursor_op) -> MdbResult<()>
        where K: ToMdbValue, V: ToMdbValue {
        self.key_val = key.to_mdb_value().value;
        self.data_val = match value {
            Some(v) => v.to_mdb_value().value,
            _ => unsafe {std::mem::zeroed() }
        };

        self.navigate(op)
    }

    /// Moves cursor to first entry
    pub fn to_first(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_FIRST)
    }

    /// Moves cursor to last entry
    pub fn to_last(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_LAST)
    }

    /// Moves cursor to first entry for key if it exists
    pub fn to_key<'k, K: ToMdbValue>(&mut self, key: &'k K) -> MdbResult<()> {
        self.move_to(key, None::<&MdbValue<'k>>, ffi::MDB_SET)
    }

    /// Moves cursor to first entry for key greater than
    /// or equal to ke
    pub fn to_gte_key<'k, K: ToMdbValue>(&mut self, key: &'k K) -> MdbResult<()> {
        self.move_to(key, None::<&MdbValue<'k>>, ffi::MDB_SET_RANGE)
    }

    /// Moves cursor to specific item (for example, if cursor
    /// already points to a correct key and you need to delete
    /// a specific item through cursor)
    pub fn to_item<K, V>(&mut self, key: &K, value: & V) -> MdbResult<()> where K: ToMdbValue, V: ToMdbValue {
        self.move_to(key, Some(value), ffi::MDB_GET_BOTH)
    }

    /// Moves cursor to next key, i.e. skip items
    /// with duplicate keys
    pub fn to_next_key(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_NEXT_NODUP)
    }

    /// Moves cursor to next item with the same key as current
    pub fn to_next_item(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_NEXT_DUP)
    }

    /// Moves cursor to prev entry, i.e. skips items
    /// with duplicate keys
    pub fn to_prev_key(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_PREV_NODUP)
    }

    /// Moves cursor to prev item with the same key as current
    pub fn to_prev_item(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_PREV_DUP)
    }

    /// Moves cursor to first item with the same key as current
    pub fn to_first_item(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_FIRST_DUP)
    }

    /// Moves cursor to last item with the same key as current
    pub fn to_last_item(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_LAST_DUP)
    }

    /// Retrieves current key/value as tuple
    pub fn get<'a, T: FromMdbValue<'a, T>+'a, U: FromMdbValue<'a, U>+'a>(&'a mut self) -> MdbResult<(MdbWrapper<'a, T>, MdbWrapper<'a, U>)> {
        let (k, v) = try!(self.get_plain());

        unsafe {
            Ok((MdbWrapper::new(FromMdbValue::from_mdb_value(mem::transmute(&k))),
                MdbWrapper::new(FromMdbValue::from_mdb_value(mem::transmute(&v)))))
        }
    }

    /// Retrieves current value
    pub fn get_value<'a, V: FromMdbValue<'a, V>+'a>(&'a mut self) -> MdbResult<MdbWrapper<'a, V>> {
        let (_, v) = try!(self.get_plain());

        unsafe {
            Ok(MdbWrapper::new(FromMdbValue::from_mdb_value(mem::transmute(&v))))
        }
    }

    /// Retrieves current key
    pub fn get_key<'a, K: FromMdbValue<'a, K>+'a>(&'a mut self) -> MdbResult<MdbWrapper<'a, K>> {
        let (k, _) = try!(self.get_plain());

        unsafe {
            Ok(MdbWrapper::new(FromMdbValue::from_mdb_value(mem::transmute(&k))))
        }
    }

    #[inline]
    fn ensure_key_valid(&mut self) -> MdbResult<()> {
        // If key might be invalid simply perform cursor get to be sure
        // it points to database memory instead of user one
        if !self.valid_key {
            unsafe {
                try_mdb!(ffi::mdb_cursor_get(self.handle, &mut self.key_val,
                                             ptr::null_mut(),
                                             ffi::MDB_GET_CURRENT));
            }
            self.valid_key = true;
        }
        Ok(())
    }

    #[inline]
    fn get_plain(&mut self) -> MdbResult<(MdbValue<'txn>, MdbValue<'txn>)> {
        try!(self.ensure_key_valid());
        let k = MdbValue {value: self.key_val};
        let v = MdbValue {value: self.data_val};

        Ok((k, v))
    }

    fn set_value<V: ToMdbValue>(&mut self, value: &V, flags: c_uint) -> MdbResult<()> {
        try!(self.ensure_key_valid());
        self.data_val = value.to_mdb_value().value;
        lift_mdb!(unsafe {ffi::mdb_cursor_put(self.handle, &mut self.key_val, &mut self.data_val, flags)})
    }

    pub fn set<K: ToMdbValue, V: ToMdbValue>(&mut self, key: &K, value: &V, flags: c_uint) -> MdbResult<()> {
        self.key_val = key.to_mdb_value().value;
        self.valid_key = true;
        let res = self.set_value(value, flags);
        self.valid_key = false;
        res
    }

    /// Overwrites value for current item
    /// Note: overwrites max cur_value.len() bytes
    pub fn replace<V: ToMdbValue>(&mut self, value: &V) -> MdbResult<()> {
        self.set_value(value, ffi::MDB_CURRENT)
    }

    /// Adds a new item when created with allowed duplicates
    pub fn add_item<V: ToMdbValue>(&mut self, value: &V) -> MdbResult<()> {
        self.set_value(value, 0)
    }

    fn del_value(&mut self, flags: c_uint) -> MdbResult<()> {
        lift_mdb!(unsafe { ffi::mdb_cursor_del(self.handle, flags) })
    }

    /// Deletes current key
    pub fn del(&mut self) -> MdbResult<()> {
        self.del_all()
    }

    /// Deletes only current item
    pub fn del_item(&mut self) -> MdbResult<()> {
        self.del_value(0)
    }

    /// Deletes all items with same key as current
    pub fn del_all(&mut self) -> MdbResult<()> {
        self.del_value(ffi::MDB_NODUPDATA)
    }

    /// Returns count of items with the same key as current
    pub fn item_count(&self) -> MdbResult<size_t> {
        let mut tmp: size_t = 0;
        lift_mdb!(unsafe {ffi::mdb_cursor_count(self.handle, &mut tmp)}, tmp)
    }

    pub fn get_item<'k, K: ToMdbValue>(&'txn self, k: &'k K) -> CursorItemAccessor<'txn, 'k, K> {
        CursorItemAccessor {
            cursor: self,
            key: k
        }
    }
}

#[unsafe_destructor]
impl<'txn> Drop for Cursor<'txn> {
    fn drop(&mut self) {
        unsafe { ffi::mdb_cursor_close(self.handle) };
    }
}


#[experimental]
pub struct CursorItemAccessor<'c, 'k, K: 'k> {
    cursor: &'c Cursor<'c>,
    key: &'k K,
}

impl<'k, 'c: 'k, K: ToMdbValue> CursorItemAccessor<'c, 'k, K> {
    pub fn get<'a, V: FromMdbValue<'a,V> + 'a>(&'a mut self) -> MdbResult<MdbWrapper<'a, V>> {
        let c: &'c mut Cursor<'c> = unsafe { mem::transmute(self.cursor) };
        try!(c.to_key(self.key));
        c.get_value()
    }

    pub fn add<V: ToMdbValue>(&mut self, v: &V) -> MdbResult<()> {
        let c: &mut Cursor = unsafe { mem::transmute(self.cursor)};
        c.set(self.key, v, 0)
    }

    pub fn del<V: ToMdbValue>(&mut self, v: &V) -> MdbResult<()> {
        let c: &mut Cursor = unsafe {mem::transmute(self.cursor)};
        try!(c.to_item(self.key, v));
        c.del_item()
    }

    pub fn del_all(&mut self) -> MdbResult<()> {
        let c: &mut Cursor = unsafe {mem::transmute(self.cursor)};
        try!(c.to_key(self.key));
        c.del_all()
    }
}

#[experimental]
pub struct CursorValue<'cursor> {
    key: MdbValue<'cursor>,
    value: MdbValue<'cursor>,
}

/// CursorValue performs lazy data extraction from iterator
/// avoiding any data conversions and memory copy. Lifetime
/// is limited to iterator lifetime
#[experimental]
impl<'cursor> CursorValue<'cursor> {
    pub fn get_key<T: FromMdbValue<'cursor, T>+'cursor>(&'cursor self) -> MdbWrapper<'cursor, T> {
        MdbWrapper::new(FromMdbValue::from_mdb_value(&self.key))
    }

    pub fn get_value<T: FromMdbValue<'cursor, T>+'cursor>(&'cursor self) -> MdbWrapper<'cursor, T> {
        MdbWrapper::new(FromMdbValue::from_mdb_value(&self.value))
    }

    pub fn get<T: FromMdbValue<'cursor, T>+'cursor, U: FromMdbValue<'cursor, U>+'cursor>(&'cursor self) -> (MdbWrapper<'cursor, T>, MdbWrapper<'cursor, U>) {
        (MdbWrapper::new(FromMdbValue::from_mdb_value(&self.key)),
         MdbWrapper::new(FromMdbValue::from_mdb_value(&self.value)))
    }
}

/// This one should once become public and allow to create custom
/// iterators
#[experimental]
trait CursorIteratorInner {
    /// Returns true if initialization successful, for example that
    /// the key exists.
    fn init_cursor<'a, 'b: 'a>(&'a self, cursor: &mut Cursor<'b>) -> bool;

    /// Returns true if there is still data and iterator is in correct range
    fn move_to_next<'iter, 'cursor: 'iter>(&'iter self, cursor: &'cursor mut Cursor<'cursor>) -> bool;

    /// Returns size hint considering current state of cursor
    fn get_size_hint(&self, _cursor: &Cursor) -> (uint, Option<uint>) {
        (0, None)
    }
}

#[experimental]
pub struct CursorIterator<'c, I> {
    inner: I,
    has_data: bool,
    cursor: Cursor<'c>,
}

impl<'c, I: CursorIteratorInner + 'c> CursorIterator<'c, I> {
    fn wrap(cursor: Cursor<'c>, inner: I) -> CursorIterator<'c, I> {
        let mut cursor = cursor;
        let has_data = inner.init_cursor(&mut cursor);
        CursorIterator {
            inner: inner,
            has_data: has_data,
            cursor: cursor
        }
    }

    #[allow(dead_code)]
    fn unwrap(self) -> Cursor<'c> {
        self.cursor
    }
}

impl<'c, I: CursorIteratorInner + 'c> Iterator<CursorValue<'c>> for CursorIterator<'c, I> {
    fn next(&mut self) -> Option<CursorValue> {
        if !self.has_data {
            None
        } else {
            match self.cursor.get_plain() {
                Err(_) => None,
                Ok((k, v)) => {
                    self.has_data = unsafe { self.inner.move_to_next(mem::transmute(&mut self.cursor)) };
                    Some(CursorValue {
                        key: k,
                        value: v
                    })
                }
            }
        }
    }

    fn size_hint(&self) -> (uint, Option<uint>) {
        self.inner.get_size_hint(&self.cursor)
    }
}

#[experimental]
pub struct CursorKeyRangeIter<'a> {
    start_key: MdbValue<'a>,
    end_key: MdbValue<'a>,
}

#[experimental]
impl<'a> CursorKeyRangeIter<'a> {
    pub fn new<K: ToMdbValue+'a>(start_key: &'a K, end_key: &'a K) -> CursorKeyRangeIter<'a> {
        CursorKeyRangeIter {
            start_key: start_key.to_mdb_value(),
            end_key: end_key.to_mdb_value(),
        }
    }
}

impl<'a> CursorIteratorInner for CursorKeyRangeIter<'a> {
    fn init_cursor<'a, 'b: 'a>(&'a self, cursor: & mut Cursor<'b>) -> bool {
        unsafe {
            cursor.to_gte_key(mem::transmute::<&'a MdbValue<'a>, &'b MdbValue<'b>>(&self.start_key)).is_ok()
        }
    }

    fn move_to_next<'i, 'c: 'i>(&'i self, cursor: &'c mut Cursor<'c>) -> bool {
        let moved = cursor.to_next_key().is_ok();
        if !moved {
            false
        } else {
            // As `get_plain` borrows mutably there is no
            // way to get comparison straight after
            // so here goes the workaround
            let k = match cursor.get_plain() {
                Err(_) => None,
                Ok((k, _)) => {
                    Some(MdbValue {
                        value: k.value
                    })
                }
            };

            match k {
                None => false,
                Some(mut k) => {
                    let cmp_res = unsafe {
                        ffi::mdb_cmp(cursor.txn.handle, cursor.db.handle,
                                     &mut k.value, mem::transmute(&self.end_key.value))
                    };
                    cmp_res > 0
                }
            }
        }
    }
}


#[experimental]
pub struct CursorIter;

#[experimental]
impl<'a> CursorIteratorInner for CursorIter {
    fn init_cursor<'a, 'b: 'a>(&'a self, cursor: & mut Cursor<'b>) -> bool {
        cursor.to_first().is_ok()
    }

    fn move_to_next<'i, 'c: 'i>(&'i self, cursor: &'c mut Cursor<'c>) -> bool {
        cursor.to_next_key().is_ok()
    }
}

#[experimental]
pub struct CursorItemIter<'a> {
    key: MdbValue<'a>,
}

#[experimental]
impl<'a> CursorItemIter<'a> {
    pub fn new<K: ToMdbValue+'a>(key: &'a K) -> CursorItemIter<'a> {
        CursorItemIter {
            key: key.to_mdb_value(),
        }
    }
}

impl<'a> CursorIteratorInner for CursorItemIter<'a> {
    fn init_cursor<'a, 'b: 'a>(&'a self, cursor: & mut Cursor<'b>) -> bool {
        unsafe {
            cursor.to_key(mem::transmute::<&MdbValue, &'b MdbValue<'b>>(&self.key)).is_ok()
        }
    }

    fn move_to_next<'i, 'c: 'i>(&'i self, cursor: &'c mut Cursor<'c>) -> bool {
        cursor.to_next_item().is_ok()
    }

    fn get_size_hint(&self, c: &Cursor) -> (uint, Option<uint>) {
        match c.item_count() {
            Err(_) => (0, None),
            Ok(cnt) => (0, Some(cnt as uint))
        }
    }
}

#[stable]
pub struct MdbValue<'a> {
    value: MDB_val
}

impl<'a> MdbValue<'a> {
    #[unstable]
    pub unsafe fn new(data: *const c_void, len: uint) -> MdbValue<'a> {
        MdbValue {
            value: MDB_val {
                mv_data: data,
                mv_size: len as size_t
            }
        }
    }

    pub fn new_from_sized<T>(data: &'a T) -> MdbValue<'a> {
        unsafe {
            MdbValue::new(mem::transmute(data), mem::size_of::<T>())
        }
    }

    pub unsafe fn get_ref<'a>(&'a self) -> *const c_void {
        self.value.mv_data
    }

    pub fn get_size(&self) -> uint {
        self.value.mv_size as uint
    }
}


/// Smart wrapper which allows to access
/// value without copying it
pub struct MdbWrapper<'a, T> {
    value: Option<T>
}

impl<'a, T> MdbWrapper<'a, T> {
    fn new(value: T) -> MdbWrapper<'a, T> {
        MdbWrapper {
            value: Some(value)
        }
    }
}

#[allow(dead_code)]
impl<'a, T: Clone> MdbWrapper<'a, T> {
    /// Converts into owned if required
    pub fn to_owned(&self) -> T {
        self.value.as_ref().unwrap().clone()
    }
}

#[unsafe_destructor]
impl<'a, T> Drop for MdbWrapper<'a, T> {
    fn drop(&mut self) {
        if self.value.is_some() {
            let tmp = self.value.take();
            unsafe {mem::forget(tmp)}
        }
    }
}

impl<'a, T> Deref<T> for MdbWrapper<'a, T> {
    fn deref<'a>(&'a self) -> &'a T {
        self.value.as_ref().unwrap()
    }
}
