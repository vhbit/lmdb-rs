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

use libc::{self, c_int, c_uint, size_t, c_void};
use std;
use std::borrow::ToOwned;
use std::cell::{UnsafeCell};
use std::cmp::{Ordering};
use std::collections::HashMap;
use std::error::Error;
use std::ffi::{CString};
use std::path::Path;
use std::mem;
use std::os::unix::ffi::{OsStrExt};
use std::ptr;
use std::result::Result;
use std::slice;
use std::sync::{Arc, Mutex};

use ffi::{self, MDB_val};
pub use MdbError::{NotFound, KeyExists, Other, StateError, Corrupted, Panic};
pub use MdbError::{InvalidPath, TxnFull, CursorFull, PageFull, CacheError};
use traits::{AsByteSlice, FromBytes};
use utils::{error_msg};


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
                let msg = format!("{} requires {:?}, is in {:?}", stringify!($log), c, e);
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
                let msg = format!("{} shouldn't be in {:?}", stringify!($log), e);
                return Err(StateError(msg))
            }})
}

trait InternalMdbValue<'a> {
    fn to_mdb_value(&'a self) -> MdbValue<'a>;
}

impl<'a> InternalMdbValue<'a> for &'a [u8] {
    #[inline(always)]
    fn to_mdb_value(&'a self) -> MdbValue<'a> {
        unsafe {
            MdbValue::new(std::mem::transmute(self.as_ptr()),
                          self.len())
        }
    }
}

impl AsByteSlice for [u8] {
    #[inline(always)]
    fn as_byte_slice<'a>(&'a self) -> &'a [u8] {
        self
    }
}

impl AsByteSlice for str {
    #[inline(always)]
    fn as_byte_slice<'a>(&'a self) -> &'a [u8] {
        self.as_bytes()
    }
}

impl<'a> AsByteSlice for &'a str {
    #[inline(always)]
    fn as_byte_slice<'b>(&'b self) -> &'b [u8] {
        self.as_bytes()
    }
}

impl AsByteSlice for String {
    #[inline(always)]
    fn as_byte_slice<'a>(&'a self) -> &'a [u8] {
        self.as_bytes()
    }
}

impl AsByteSlice for Vec<u8> {
    #[inline(always)]
    fn as_byte_slice<'a>(&'a self) -> &'a [u8] {
        &self
    }
}

/// MdbError wraps information about LMDB error
#[derive(Debug)]
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
    CacheError,
    Other(c_int, String)
}


impl MdbError {
    pub fn new_with_code(code: c_int) -> MdbError {
        match code {
            ffi::MDB_NOTFOUND    => NotFound,
            ffi::MDB_KEYEXIST    => KeyExists,
            ffi::MDB_TXN_FULL    => TxnFull,
            ffi::MDB_CURSOR_FULL => CursorFull,
            ffi::MDB_PAGE_FULL   => PageFull,
            ffi::MDB_CORRUPTED   => Corrupted,
            ffi::MDB_PANIC       => Panic,
            _                    => Other(code, error_msg(code))
        }
    }
}


impl std::fmt::Display for MdbError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            &NotFound | &KeyExists | &TxnFull |
            &CursorFull | &PageFull | &Corrupted |
            &Panic | &InvalidPath | &CacheError => write!(fmt, "{}", self.description()),
            &StateError(ref msg) => write!(fmt, "{}", msg),
            &Other(code, ref msg) => write!(fmt, "{}: {}", code, msg)
        }
    }
}

impl Error for MdbError {
    fn description(&self) -> &'static str {
        match self {
            &NotFound => "not found",
            &KeyExists => "key exists",
            &TxnFull => "txn full",
            &CursorFull => "cursor full",
            &PageFull => "page full",
            &Corrupted => "corrupted",
            &Panic => "panic",
            &InvalidPath => "invalid path for database",
            &StateError(_) => "state error",
            &CacheError => "db cache error",
            &Other(_, _) => "other error",
        }
    }
}


pub type MdbResult<T> = Result<T, MdbError>;

bitflags! {
    #[doc = "A set of environment flags which could be changed after opening"]

    flags EnvFlags: c_uint {

        #[doc="Don't flush system buffers to disk when committing a
        transaction. This optimization means a system crash can
        corrupt the database or lose the last transactions if buffers
        are not yet flushed to disk. The risk is governed by how
        often the system flushes dirty buffers to disk and how often
        mdb_env_sync() is called. However, if the filesystem
        preserves write order and the MDB_WRITEMAP flag is not used,
        transactions exhibit ACI (atomicity, consistency, isolation)
        properties and only lose D (durability). I.e. database
        integrity is maintained, but a system crash may undo the
        final transactions. Note that (MDB_NOSYNC | MDB_WRITEMAP)
        leaves the system with no hint for when to write transactions
        to disk, unless mdb_env_sync() is called. (MDB_MAPASYNC |
        MDB_WRITEMAP) may be preferable. This flag may be changed at
        any time using mdb_env_set_flags()."]
        const EnvNoSync      = ffi::MDB_NOSYNC,

        #[doc="Flush system buffers to disk only once per transaction,
        omit the metadata flush. Defer that until the system flushes
        files to disk, or next non-MDB_RDONLY commit or
        mdb_env_sync(). This optimization maintains database
        integrity, but a system crash may undo the last committed
        transaction. I.e. it preserves the ACI (atomicity,
        consistency, isolation) but not D (durability) database
        property. This flag may be changed at any time using
        mdb_env_set_flags()."]
        const EnvNoMetaSync  = ffi::MDB_NOMETASYNC,

        #[doc="When using MDB_WRITEMAP, use asynchronous flushes to
        disk. As with MDB_NOSYNC, a system crash can then corrupt the
        database or lose the last transactions. Calling
        mdb_env_sync() ensures on-disk database integrity until next
        commit. This flag may be changed at any time using
        mdb_env_set_flags()."]
        const EnvMapAsync    = ffi::MDB_MAPASYNC,

        #[doc="Don't initialize malloc'd memory before writing to
        unused spaces in the data file. By default, memory for pages
        written to the data file is obtained using malloc. While
        these pages may be reused in subsequent transactions, freshly
        malloc'd pages will be initialized to zeroes before use. This
        avoids persisting leftover data from other code (that used
        the heap and subsequently freed the memory) into the data
        file. Note that many other system libraries may allocate and
        free memory from the heap for arbitrary uses. E.g., stdio may
        use the heap for file I/O buffers. This initialization step
        has a modest performance cost so some applications may want
        to disable it using this flag. This option can be a problem
        for applications which handle sensitive data like passwords,
        and it makes memory checkers like Valgrind noisy. This flag
        is not needed with MDB_WRITEMAP, which writes directly to the
        mmap instead of using malloc for pages. The initialization is
        also skipped if MDB_RESERVE is used; the caller is expected
        to overwrite all of the memory that was reserved in that
        case. This flag may be changed at any time using
        mdb_env_set_flags()."]
        const EnvNoMemInit   = ffi::MDB_NOMEMINIT
    }
}

bitflags! {
    #[doc = "A set of all environment flags"]

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
pub struct Database<'a> {
    handle: ffi::MDB_dbi,
    txn: &'a RawTransaction<'a>,
}

// FIXME: provide different interfaces for read-only/read-write databases
// FIXME: provide different interfaces for simple KV and storage with duplicates

impl<'a> Database<'a> {
    fn new_with_handle(handle: ffi::MDB_dbi, txn: &'a RawTransaction<'a>) -> Database<'a> {
        Database { handle: handle, txn: txn }
    }

    /// Retrieves current db's statistics.
    pub fn stat(&'a self) -> MdbResult<ffi::MDB_stat> {
        self.txn.stat(self.handle)
    }

    /// Retrieves a value by key. In case of DbAllowDups it will be the first value
    pub fn get<V: FromBytes + 'a>(&'a self, key: &AsByteSlice) -> MdbResult<V> {
        self.txn.get(self.handle, key)
    }

    /// Sets value for key. In case of DbAllowDups it will add a new item
    pub fn set<K: AsByteSlice, V: AsByteSlice>(&self, key: &K, value: &V) -> MdbResult<()> {
        self.txn.set(self.handle, key, value)
    }

    /// Deletes value for key.
    pub fn del<K: AsByteSlice>(&self, key: &K) -> MdbResult<()> {
        self.txn.del(self.handle, key)
    }

    /// Should be used only with DbAllowDups. Deletes corresponding (key, value)
    pub fn del_item(&self, key: &AsByteSlice, data: &AsByteSlice) -> MdbResult<()> {
        self.txn.del_item(self.handle, key, data)
    }

    /// Returns a new cursor
    pub fn new_cursor(&'a self) -> MdbResult<Cursor<'a>> {
        self.txn.new_cursor(self.handle)
    }

    /// Deletes current db, also moves it out
    pub fn del_db(self) -> MdbResult<()> {
        self.txn.del_db(self)
    }

    /// Removes all key/values from db
    pub fn clear(&self) -> MdbResult<()> {
        self.txn.clear_db(self.handle)
    }

    /// Returns an iterator for all values in database
    pub fn iter(&'a self) -> MdbResult<CursorIterator<'a, CursorIter>> {
        self.txn.new_cursor(self.handle)
            .and_then(|c| Ok(CursorIterator::wrap(c, CursorIter)))
    }

    /// Returns an iterator through keys starting with start_key (>=), start_key is included
    pub fn keyrange_from<'c, K: AsByteSlice + 'c>(&'c self, start_key: &'c K) -> MdbResult<CursorIterator<'c, CursorFromKeyIter<K>>> {
        let cursor = try!(self.txn.new_cursor(self.handle));
        let key_range = CursorFromKeyIter::new(start_key);
        let wrap = CursorIterator::wrap(cursor, key_range);
        Ok(wrap)
    }

    /// Returns an iterator through keys less than end_key, end_key is not included
    pub fn keyrange_to<'c, K: AsByteSlice + 'c>(&'c self, end_key: &'c K) -> MdbResult<CursorIterator<'c, CursorToKeyIter>> {
        let cursor = try!(self.txn.new_cursor(self.handle));
        let key_range = CursorToKeyIter::new(end_key);
        let wrap = CursorIterator::wrap(cursor, key_range);
        Ok(wrap)
    }

    /// Returns an iterator through keys `start_key <= x < end_key`. This is, start_key is
    /// included in the iteration, while end_key is kept excluded.
    pub fn keyrange_from_to<'c, K: AsByteSlice + 'c>(&'c self, start_key: &'c K, end_key: &'c K)
                               -> MdbResult<CursorIterator<'c, CursorKeyRangeIter<K>>>
    {
        let cursor = try!(self.txn.new_cursor(self.handle));
        let key_range = CursorKeyRangeIter::new(start_key, end_key, false);
        let wrap = CursorIterator::wrap(cursor, key_range);
        Ok(wrap)
    }

    /// Returns an iterator for values between start_key and end_key (included).
    /// Currently it works only for unique keys (i.e. it will skip
    /// multiple items when DB created with ffi::MDB_DUPSORT).
    /// Iterator is valid while cursor is valid
    pub fn keyrange<'c, K: AsByteSlice + 'c>(&'c self, start_key: &'c K, end_key: &'c K)
                               -> MdbResult<CursorIterator<'c, CursorKeyRangeIter<K>>>
    {
        let cursor = try!(self.txn.new_cursor(self.handle));
        let key_range = CursorKeyRangeIter::new(start_key, end_key, true);
        let wrap = CursorIterator::wrap(cursor, key_range);
        Ok(wrap)
    }

    /// Returns an iterator for all items (i.e. values with same key)
    pub fn item_iter<'c, 'db: 'c, K: AsByteSlice>(&'db self, key: &'c K) -> MdbResult<CursorIterator<'c, CursorItemIter<'c, K>>> {
        let cursor = try!(self.txn.new_cursor(self.handle));
        let inner_iter = CursorItemIter::<'c>::new(key);
        Ok(CursorIterator::<'c>::wrap(cursor, inner_iter))
    }
}


/// Constructs environment with settigs which couldn't be
/// changed after opening. By default it tries to create
/// corresponding dir if it doesn't exist, use `autocreate_dir()`
/// to override that behavior
#[derive(Copy, Clone)]
pub struct EnvBuilder {
    flags: EnvCreateFlags,
    max_readers: Option<usize>,
    max_dbs: Option<usize>,
    map_size: Option<u64>,
    autocreate_dir: bool,
}

impl EnvBuilder {
    pub fn new() -> EnvBuilder {
        EnvBuilder {
            flags: EnvCreateFlags::empty(),
            max_readers: None,
            max_dbs: None,
            map_size: None,
            autocreate_dir: true,
        }
    }

    /// Sets environment flags
    pub fn flags(mut self, flags: EnvCreateFlags) -> EnvBuilder {
        self.flags = flags;
        self
    }

    /// Sets max concurrent readers operating on environment
    pub fn max_readers(mut self, max_readers: usize) -> EnvBuilder {
        self.max_readers = Some(max_readers);
        self
    }

    /// Set max number of databases
    pub fn max_dbs(mut self, max_dbs: usize) -> EnvBuilder {
        self.max_dbs = Some(max_dbs);
        self
    }

    /// Sets max environment size, i.e. size in memory/disk of
    /// all data
    pub fn map_size(mut self, map_size: u64) -> EnvBuilder {
        self.map_size = Some(map_size);
        self
    }

    /// Sets whetever `lmdb-rs` should try to autocreate dir with default
    /// permissions on opening (default is true)
    pub fn autocreate_dir(mut self, autocreate_dir: bool)  -> EnvBuilder {
        self.autocreate_dir = autocreate_dir;
        self
    }

    /// Opens environment in specified path
    pub fn open(self, path: &Path, perms: u32) -> MdbResult<Environment> {
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

        if self.autocreate_dir {
            let _ = try!(EnvBuilder::check_path(path, self.flags));
        }

        let res = unsafe {
            // FIXME: revert back once `convert` is stable
            // let c_path = path.as_os_str().to_cstring().unwrap();
            let path_str = try!(path.to_str().ok_or(MdbError::InvalidPath));
            let c_path = try!(CString::new(path_str).map_err(|_| MdbError::InvalidPath));
            ffi::mdb_env_open(mem::transmute(env), c_path.as_ptr(), self.flags.bits,
                              perms as libc::mode_t)
        };

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

    fn check_path(path: &Path, flags: EnvCreateFlags) -> MdbResult<()> {
        use std::{fs, io};

        if flags.contains(EnvCreateNoSubDir) {
            // FIXME: check parent dir existence/absence
            warn!("checking for path in NoSubDir mode isn't implemented yet");
            return Ok(());
        }

        // There should be a directory before open
        match fs::metadata(path) {
            Ok(meta) => {
                if meta.is_dir() {
                    Ok(())
                } else {
                    Err(MdbError::InvalidPath)
                }
            },
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    fs::create_dir_all(path.clone()).map_err(|e| {
                        error!("failed to auto create dir: {}", e);
                        MdbError::InvalidPath
                    })
                } else {
                    Err(MdbError::InvalidPath)
                }
            }
        }
    }
}

struct EnvHandle(*mut ffi::MDB_env);

impl Drop for EnvHandle {
    fn drop(&mut self) {
        unsafe {
            if self.0 != ptr::null_mut() {
                ffi::mdb_env_close(self.0);
            }
        }
    }
}

/// Represents LMDB Environment. Should be opened using `EnvBuilder`
pub struct Environment {
    env: Arc<EnvHandle>,
    db_cache: Arc<Mutex<UnsafeCell<HashMap<String, ffi::MDB_dbi>>>>,
}

impl Environment {
    pub fn new() -> EnvBuilder {
        EnvBuilder::new()
    }

    fn from_raw(env: *mut ffi::MDB_env) -> Environment {
        Environment {
            env: Arc::new(EnvHandle(env)),
            db_cache: Arc::new(Mutex::new(UnsafeCell::new(HashMap::new()))),
        }
    }

    /// Check for stale entries in the reader lock table.
    ///
    /// Returns the number of stale slots that were cleared.
    pub fn reader_check(&self) -> MdbResult<c_int> {
        let mut dead: c_int = 0;
        lift_mdb!(unsafe { ffi::mdb_reader_check(self.env.0, &mut dead as *mut c_int)}, dead)
    }

    /// Retrieve environment statistics
    pub fn stat(&self) -> MdbResult<ffi::MDB_stat> {
        let mut tmp: ffi::MDB_stat = unsafe { std::mem::zeroed() };
        lift_mdb!(unsafe { ffi::mdb_env_stat(self.env.0, &mut tmp)}, tmp)
    }

    pub fn info(&self) -> MdbResult<ffi::MDB_envinfo> {
        let mut tmp: ffi::MDB_envinfo = unsafe { std::mem::zeroed() };
        lift_mdb!(unsafe { ffi::mdb_env_info(self.env.0, &mut tmp)}, tmp)
    }

    /// Sync environment to disk
    pub fn sync(&self, force: bool) -> MdbResult<()> {
        lift_mdb!(unsafe { ffi::mdb_env_sync(self.env.0, if force {1} else {0})})
    }

    /// This one sets only flags which are available for change even
    /// after opening, see also [get_flags](#method.get_flags) and [get_all_flags](#method.get_all_flags)
    pub fn set_flags(&mut self, flags: EnvFlags, turn_on: bool) -> MdbResult<()> {
        lift_mdb!(unsafe {
            ffi::mdb_env_set_flags(self.env.0, flags.bits(), if turn_on {1} else {0})
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
        lift_mdb!(unsafe {ffi::mdb_env_get_flags(self.env.0, &mut flags)}, EnvCreateFlags::from_bits_truncate(flags))
    }

    pub fn get_maxreaders(&self) -> MdbResult<c_uint> {
        let mut max_readers: c_uint = 0;
        lift_mdb!(unsafe {
            ffi::mdb_env_get_maxreaders(self.env.0, &mut max_readers)
        }, max_readers)
    }

    pub fn get_maxkeysize(&self) -> c_int {
        unsafe {ffi::mdb_env_get_maxkeysize(self.env.0)}
    }

    /// Creates a backup copy in specified file descriptor
    pub fn copy_to_fd(&self, fd: ffi::mdb_filehandle_t) -> MdbResult<()> {
        lift_mdb!(unsafe { ffi::mdb_env_copyfd(self.env.0, fd) })
    }

    /// Gets file descriptor of this environment
    pub fn get_fd(&self) -> MdbResult<ffi::mdb_filehandle_t> {
        let mut fd = 0;
        lift_mdb!({ unsafe { ffi::mdb_env_get_fd(self.env.0, &mut fd) }}, fd)
    }

    /// Creates a backup copy in specified path
    // FIXME: check who is responsible for creating path: callee or caller
    pub fn copy_to_path(&self, path: &Path) -> MdbResult<()> {
        // FIXME: revert back once `convert` is stable
        // let c_path = path.as_os_str().to_cstring().unwrap();
        let path_str = try!(path.to_str().ok_or(MdbError::InvalidPath));
        let c_path = try!(CString::new(path_str).map_err(|_| MdbError::InvalidPath));

        unsafe {
            lift_mdb!(ffi::mdb_env_copy(self.env.0, c_path.as_ptr()))
        }
    }

    fn create_transaction(&self, parent: Option<RawTransaction>, flags: c_uint) -> MdbResult<RawTransaction> {
        let mut handle: *mut ffi::MDB_txn = ptr::null_mut();
        let parent_handle = match parent {
            Some(t) => t.handle,
            _ => ptr::null_mut()
        };

        lift_mdb!(unsafe { ffi::mdb_txn_begin(self.env.0, parent_handle, flags, &mut handle) },
                 RawTransaction::new_with_handle(handle, flags as usize, self))
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

    fn _open_db(&self, db_name: & str, flags: DbFlags, force_creation: bool) -> MdbResult<ffi::MDB_dbi> {
        debug!("Opening {} (create={})", db_name, force_creation);
        // From LMDB docs for mdb_dbi_open:
        //
        // This function must not be called from multiple concurrent
        // transactions. A transaction that uses this function must finish
        // (either commit or abort) before any other transaction may use
        // this function
        match self.db_cache.lock() {
            Err(_) => Err(MdbError::CacheError),
            Ok(guard) => {
                let ref cell = *guard;
                let cache = cell.get();

                unsafe {
                    if let Some(db) = (*cache).get(db_name) {
                        debug!("Cached value for {}: {}", db_name, *db);
                        return Ok(*db);
                    }
                }

                let mut txn = try!(self.create_transaction(None, 0));
                let opt_name = if db_name.len() > 0 {Some(db_name)} else {None};
                let flags = if force_creation {flags | DbCreate} else {flags - DbCreate};

                let mut db: ffi::MDB_dbi = 0;
                let db_res = match opt_name {
                    None => unsafe { ffi::mdb_dbi_open(txn.handle, ptr::null(), flags.bits(), &mut db) },
                    Some(db_name) => {
                        let db_name = CString::new(db_name.as_bytes()).unwrap();
                        unsafe {
                            ffi::mdb_dbi_open(txn.handle, db_name.as_ptr(), flags.bits(), &mut db)
                        }
                    }
                };

                try_mdb!(db_res);
                try!(txn.commit());

                debug!("Caching: {} -> {}", db_name, db);
                unsafe {
                    (*cache).insert(db_name.to_owned(), db);
                };

                Ok(db)
            }
        }
    }

    /// Opens existing DB
    pub fn get_db(& self, db_name: &str, flags: DbFlags) -> MdbResult<DbHandle> {
        let db = try!(self._open_db(db_name, flags, false));
        Ok(DbHandle {handle: db, flags: flags})
    }

    /// Opens or creates a DB
    pub fn create_db(&self, db_name: &str, flags: DbFlags) -> MdbResult<DbHandle> {
        let db = try!(self._open_db(db_name, flags, true));
        Ok(DbHandle {handle: db, flags: flags})
    }

    /// Opens default DB with specified flags
    pub fn get_default_db(&self, flags: DbFlags) -> MdbResult<DbHandle> {
        self.get_db("", flags)
    }

    fn drop_db_from_cache(&self, handle: ffi::MDB_dbi) {
        match self.db_cache.lock() {
            Err(_) => (),
            Ok(guard) => {
                let ref cell = *guard;

                unsafe {
                    let cache = cell.get();

                    let mut key = None;
                    for (k, v) in (*cache).iter() {
                        if *v == handle {
                            key = Some(k);
                            break;
                        }
                    }

                    if let Some(key) = key {
                        (*cache).remove(key);
                    }
                }
            }
        }
    }
}

unsafe impl Sync for Environment {}
unsafe impl Send for Environment {}

impl Clone for Environment {
    fn clone(&self) -> Environment {
        Environment {
            env: self.env.clone(),
            db_cache: self.db_cache.clone()
        }
    }
}

#[allow(dead_code)]
#[derive(Copy, Clone)]
/// A handle to a database
///
/// It can be cached to avoid opening db on every access
/// In the current state it is unsafe as other thread
/// can ask to drop it.
pub struct DbHandle {
    handle: ffi::MDB_dbi,
    flags: DbFlags
}

unsafe impl Sync for DbHandle {}
unsafe impl Send for DbHandle {}

#[derive(Copy, PartialEq, Debug, Eq, Clone)]
enum TransactionState {
    Normal,   // Normal, any operation possible
    Released, // Released (reset on readonly), has to be renewed
    Invalid,  // Invalid, no further operation possible
}

struct RawTransaction<'a> {
    handle: *mut ffi::MDB_txn,
    env: &'a Environment,
    flags: usize,
    state: TransactionState,
}

impl<'a> RawTransaction<'a> {
    fn new_with_handle(h: *mut ffi::MDB_txn, flags: usize, env: &Environment) -> RawTransaction {
        // debug!("new native txn");
        RawTransaction {
            handle: h,
            flags: flags,
            state: TransactionState::Normal,
            env: env,
        }
    }

    fn is_readonly(&self) -> bool {
        (self.flags as u32 & ffi::MDB_RDONLY) == ffi::MDB_RDONLY
    }

    fn commit(&mut self) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        debug!("commit txn");
        try_mdb!(unsafe { ffi::mdb_txn_commit(self.handle) } );
        self.state = if self.is_readonly() {
            TransactionState::Released
        } else {
            TransactionState::Invalid
        };
        Ok(())
    }

    fn abort(&mut self) {
        if self.state != TransactionState::Normal {
            debug!("Can't abort transaction: current state {:?}", self.state)
        } else {
            debug!("abort txn");
            unsafe { ffi::mdb_txn_abort(self.handle); }
            self.state = if self.is_readonly() {
                TransactionState::Released
            } else {
                TransactionState::Invalid
            };
        }
    }

    /// Resets read only transaction, handle is kept. Must be followed
    /// by a call to `renew`
    fn reset(&mut self) {
        if self.state != TransactionState::Normal {
            debug!("Can't reset transaction: current state {:?}", self.state);
        } else {
            unsafe { ffi::mdb_txn_reset(self.handle); }
            self.state = TransactionState::Released;
        }
    }

    /// Acquires a new reader lock after it was released by reset
    fn renew(&mut self) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Released);
        try_mdb!(unsafe {ffi::mdb_txn_renew(self.handle)});
        self.state = TransactionState::Normal;
        Ok(())
    }

    fn new_child(&self, flags: c_uint) -> MdbResult<RawTransaction> {
        let mut out: *mut ffi::MDB_txn = ptr::null_mut();
        try_mdb!(unsafe { ffi::mdb_txn_begin(ffi::mdb_txn_env(self.handle), self.handle, flags, &mut out) });
        Ok(RawTransaction::new_with_handle(out, flags as usize, self.env))
    }

    /// Used in Drop to switch state
    fn silent_abort(&mut self) {
        if self.state == TransactionState::Normal {
            debug!("silent abort");
            unsafe {ffi::mdb_txn_abort(self.handle);}
            self.state = TransactionState::Invalid;
        }
    }

    fn get_value<V: FromBytes + 'a>(&'a self, db: ffi::MDB_dbi, key: &AsByteSlice) -> MdbResult<V> {
        let tmp_slice = key.as_byte_slice();
        let mut key_val = tmp_slice.to_mdb_value();
        unsafe {
            let mut data_val: MdbValue = std::mem::zeroed();
            try_mdb!(ffi::mdb_get(self.handle, db, &mut key_val.value, &mut data_val.value));
            Ok(FromBytes::from_bytes(&data_val.as_ref()))
        }
    }

    fn get<V: FromBytes + 'a>(&'a self, db: ffi::MDB_dbi, key: &AsByteSlice) -> MdbResult<V> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        self.get_value(db, key)
    }

    fn set_value(&self, db: ffi::MDB_dbi, key: &AsByteSlice, value: &AsByteSlice) -> MdbResult<()> {
        self.set_value_with_flags(db, key, value, 0)
    }

    fn set_value_with_flags(&self, db: ffi::MDB_dbi, key: &AsByteSlice, value: &AsByteSlice, flags: c_uint) -> MdbResult<()> {
        unsafe {
            let tmp1 = key.as_byte_slice();
            let mut key_val = tmp1.to_mdb_value();
            let tmp2 = value.as_byte_slice();
            let mut data_val = tmp2.to_mdb_value();

            lift_mdb!(ffi::mdb_put(self.handle, db, &mut key_val.value, &mut data_val.value, flags))
        }
    }

    /// Sets a new value for key, in case of enabled duplicates
    /// it actually appends a new value
    // FIXME: add explicit append function
    // FIXME: think about creating explicit separation of
    // all traits for databases with dup keys
    fn set(&self, db: ffi::MDB_dbi, key: &AsByteSlice, value: &AsByteSlice) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        self.set_value(db, key, value)
    }

    /// Deletes all values by key
    fn del_value(&self, db: ffi::MDB_dbi, key: &AsByteSlice) -> MdbResult<()> {
        unsafe {
            let tmp = key.as_byte_slice();
            let mut key_val = tmp.to_mdb_value();
            lift_mdb!(ffi::mdb_del(self.handle, db, &mut key_val.value, ptr::null_mut()))
        }
    }

    /// If duplicate keys are allowed deletes value for key which is equal to data
    fn del_item(&self, db: ffi::MDB_dbi, key: &AsByteSlice, data: &AsByteSlice) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        unsafe {
            let tmp1 = key.as_byte_slice();
            let mut key_val = tmp1.to_mdb_value();
            let tmp2 = data.as_byte_slice();
            let mut data_val = tmp2.to_mdb_value();

            lift_mdb!(ffi::mdb_del(self.handle, db, &mut key_val.value, &mut data_val.value))
        }
    }

    /// Deletes all values for key
    fn del(&self, db: ffi::MDB_dbi, key: &AsByteSlice) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        self.del_value(db, key)
    }

    /// Creates a new cursor in current transaction tied to db
    fn new_cursor(&'a self, db: ffi::MDB_dbi) -> MdbResult<Cursor<'a>> {
        Cursor::new(self, db)
    }

    /// Deletes provided database completely
    fn del_db(&self, db: Database) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        unsafe {
            self.env.drop_db_from_cache(db.handle);
            lift_mdb!(ffi::mdb_drop(self.handle, db.handle, 1))
        }
    }

    /// Empties provided database
    fn clear_db(&self, db: ffi::MDB_dbi) -> MdbResult<()> {
        assert_state_eq!(txn, self.state, TransactionState::Normal);
        unsafe {
            lift_mdb!(ffi::mdb_drop(self.handle, db, 0))
        }
    }

    /// Retrieves provided database's statistics
    fn stat(&self, db: ffi::MDB_dbi) -> MdbResult<ffi::MDB_stat> {
        let mut tmp: ffi::MDB_stat = unsafe { std::mem::zeroed() };
        lift_mdb!(unsafe { ffi::mdb_stat(self.handle, db, &mut tmp)}, tmp)
    }
}

impl<'a> Drop for RawTransaction<'a> {
    fn drop(&mut self) {
        //debug!("Dropping native transaction!");
        self.silent_abort();
    }
}

pub struct Transaction<'a> {
    inner: RawTransaction<'a>,
}

impl<'a> Transaction<'a> {
    fn new_with_native(txn: RawTransaction<'a>) -> Transaction<'a> {
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

    pub fn bind(&self, db_handle: &DbHandle) -> Database {
        Database::new_with_handle(db_handle.handle, &self.inner)
    }
}


pub struct ReadonlyTransaction<'a> {
    inner: RawTransaction<'a>,
}


impl<'a> ReadonlyTransaction<'a> {
    fn new_with_native(txn: RawTransaction<'a>) -> ReadonlyTransaction<'a> {
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

    pub fn bind(&self, db_handle: &DbHandle) -> Database {
        Database::new_with_handle(db_handle.handle, &self.inner)
    }
}

/// Helper to determine the property of "less than or equal to" where
/// the "equal to" part is to be specified at runtime.
trait IsLess {
    fn is_less(&self, or_equal: bool) -> bool;
}

impl IsLess for Ordering {
    fn is_less(&self, or_equal: bool) -> bool {
        match (*self, or_equal) {
            (Ordering::Less, _) => true,
            (Ordering::Equal, true) => true,
            _ => false,
        }
    }
}

impl IsLess for MdbResult<Ordering> {
    fn is_less(&self, or_equal: bool) -> bool {
        match *self {
            Ok(ord) => ord.is_less(or_equal),
            Err(_) => false,
        }
    }
}

pub struct Cursor<'txn> {
    handle: *mut ffi::MDB_cursor,
    data_val: ffi::MDB_val,
    key_val: ffi::MDB_val,
    txn: &'txn RawTransaction<'txn>,
    db: ffi::MDB_dbi,
    valid_key: bool,
}


impl<'txn> Cursor<'txn> {
    fn new(txn: &'txn RawTransaction, db: ffi::MDB_dbi) -> MdbResult<Cursor<'txn>> {
        debug!("Opening cursor in {}", db);
        let mut tmp: *mut ffi::MDB_cursor = std::ptr::null_mut();
        try_mdb!(unsafe { ffi::mdb_cursor_open(txn.handle, db, &mut tmp) });
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
                self.valid_key = op != ffi::MDB_cursor_op::MDB_SET;
                Ok(())
            },
            e => Err(MdbError::new_with_code(e))
        }
    }

    fn move_to<K, V>(&mut self, key: &K, value: Option<&V>, op: ffi::MDB_cursor_op) -> MdbResult<()>
        where K: AsByteSlice, V: AsByteSlice {
        self.key_val = key.as_byte_slice().to_mdb_value().value;
        self.data_val = match value {
            Some(v) => v.as_byte_slice().to_mdb_value().value,
            _ => unsafe {std::mem::zeroed() }
        };

        self.navigate(op)
    }

    /// Moves cursor to first entry
    pub fn to_first(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_FIRST)
    }

    /// Moves cursor to last entry
    pub fn to_last(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_LAST)
    }

    /// Moves cursor to first entry for key if it exists
    pub fn to_key<'k, K: AsByteSlice>(&mut self, key: &'k K) -> MdbResult<()> {
        self.move_to(key, None::<&K>, ffi::MDB_cursor_op::MDB_SET_KEY)
    }

    /// Moves cursor to first entry for key greater than
    /// or equal to key
    pub fn to_gte_key<'k, K: AsByteSlice>(&mut self, key: &'k K) -> MdbResult<()> {
        self.move_to(key, None::<&K>, ffi::MDB_cursor_op::MDB_SET_RANGE)
    }

    /// Moves cursor to specific item (for example, if cursor
    /// already points to a correct key and you need to delete
    /// a specific item through cursor)
    pub fn to_item<K, V>(&mut self, key: &K, value: & V) -> MdbResult<()> where K: AsByteSlice, V: AsByteSlice {
        self.move_to(key, Some(value), ffi::MDB_cursor_op::MDB_GET_BOTH)
    }

    /// Moves cursor to next key, i.e. skip items
    /// with duplicate keys
    pub fn to_next_key(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_NEXT_NODUP)
    }

    /// Moves cursor to next item with the same key as current
    pub fn to_next_item(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_NEXT_DUP)
    }

    /// Moves cursor to prev entry, i.e. skips items
    /// with duplicate keys
    pub fn to_prev_key(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_PREV_NODUP)
    }

    /// Moves cursor to prev item with the same key as current
    pub fn to_prev_item(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_PREV_DUP)
    }

    /// Moves cursor to first item with the same key as current
    pub fn to_first_item(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_FIRST_DUP)
    }

    /// Moves cursor to last item with the same key as current
    pub fn to_last_item(&mut self) -> MdbResult<()> {
        self.navigate(ffi::MDB_cursor_op::MDB_LAST_DUP)
    }

    /// Retrieves current key/value as tuple
    pub fn get<'a, T: FromBytes + 'a, U: FromBytes + 'a>(&'a mut self) -> MdbResult<(T, U)> {
        let (k, v) = try!(self.get_plain());

        Ok((FromBytes::from_bytes(&k.as_ref()),
            FromBytes::from_bytes(&v.as_ref())))
    }

    /// Retrieves current value
    pub fn get_value<'a, V: FromBytes + 'a>(&'a mut self) -> MdbResult<V> {
        let (_, v) = try!(self.get_plain());

        Ok(FromBytes::from_bytes(&v.as_ref()))
    }

    /// Retrieves current key
    pub fn get_key<'a, K: FromBytes + 'a>(&'a mut self) -> MdbResult<K> {
        let (k, _) = try!(self.get_plain());

        Ok(FromBytes::from_bytes(&k.as_ref()))
    }

    /// Compares the cursor's current key with the specified other one.
    #[inline]
    fn cmp_key(&mut self, other: &[u8]) -> MdbResult<Ordering> {
        let (k, _) = try!(self.get_plain());
        let mut kval = k.value;
        let mut other_mdb = other.to_mdb_value();
        let cmp = unsafe {
            ffi::mdb_cmp(self.txn.handle, self.db, &mut kval, &mut other_mdb.value)
        };
        Ok(match cmp {
            n if n < 0 => Ordering::Less,
            n if n > 0 => Ordering::Greater,
            _          => Ordering::Equal,
        })
    }

    #[inline]
    fn ensure_key_valid(&mut self) -> MdbResult<()> {
        // If key might be invalid simply perform cursor get to be sure
        // it points to database memory instead of user one
        if !self.valid_key {
            unsafe {
                try_mdb!(ffi::mdb_cursor_get(self.handle, &mut self.key_val,
                                             ptr::null_mut(),
                                             ffi::MDB_cursor_op::MDB_GET_CURRENT));
            }
            self.valid_key = true;
        }
        Ok(())
    }

    #[inline]
    fn get_plain(&mut self) -> MdbResult<(MdbValue<'txn>, MdbValue<'txn>)> {
        try!(self.ensure_key_valid());
        let k = MdbValue {value: self.key_val, marker: ::std::marker::PhantomData};
        let v = MdbValue {value: self.data_val, marker: ::std::marker::PhantomData};

        Ok((k, v))
    }

    #[allow(dead_code)]
    // This one is used for debugging, so it's to OK to leave it for a while
    fn dump_value(&self, prefix: &str) {
        if self.valid_key {
            println!("{}: key {:?}, data {:?}", prefix,
                     self.key_val,
                     self.data_val);
        }
    }

    fn set_value<V: AsByteSlice>(&mut self, value: &V, flags: c_uint) -> MdbResult<()> {
        try!(self.ensure_key_valid());
        self.data_val = value.as_byte_slice().to_mdb_value().value;
        lift_mdb!(unsafe {ffi::mdb_cursor_put(self.handle, &mut self.key_val, &mut self.data_val, flags)})
    }

    pub fn set<K: AsByteSlice, V: AsByteSlice>(&mut self, key: &K, value: &V, flags: c_uint) -> MdbResult<()> {
        self.key_val = key.as_byte_slice().to_mdb_value().value;
        self.valid_key = true;
        let res = self.set_value(value, flags);
        self.valid_key = false;
        res
    }

    /// Overwrites value for current item
    /// Note: overwrites max cur_value.len() bytes
    pub fn replace<V: AsByteSlice>(&mut self, value: &V) -> MdbResult<()> {
        let res = self.set_value(value, ffi::MDB_CURRENT);
        self.valid_key = false;
        res
    }

    /// Adds a new item when created with allowed duplicates
    pub fn add_item<V: AsByteSlice>(&mut self, value: &V) -> MdbResult<()> {
        let res = self.set_value(value, 0);
        self.valid_key = false;
        res
    }

    fn del_value(&mut self, flags: c_uint) -> MdbResult<()> {
        lift_mdb!(unsafe { ffi::mdb_cursor_del(self.handle, flags) })
    }

    /// Deletes current key
    pub fn del(&mut self) -> MdbResult<()> {
        self.del_all()
    }

    /// Deletes only current item
    ///
    /// Note that it doesn't check anything so it is caller responsibility
    /// to make sure that correct item is deleted if, for example, caller
    /// wants to delete only items of current key
    pub fn del_item(&mut self) -> MdbResult<()> {
        let res = self.del_value(0);
        self.valid_key = false;
        res
    }

    /// Deletes all items with same key as current
    pub fn del_all(&mut self) -> MdbResult<()> {
        self.del_value(ffi::MDB_NODUPDATA)
    }

    /// Returns count of items with the same key as current
    pub fn get_item_count(&self) -> MdbResult<size_t> {
        let mut tmp: size_t = 0;
        lift_mdb!(unsafe {ffi::mdb_cursor_count(self.handle, &mut tmp)}, tmp)
    }

    /// Consumes cursor to provide an accessor to item (i.e. a set of duplicates for one key)
    pub fn get_item<'k, K: AsByteSlice>(self, k: &'k K) -> CursorItemAccessor<'txn, 'k, K> {
        CursorItemAccessor {
            cursor: self,
            key: k
        }
    }
}

impl<'txn> Drop for Cursor<'txn> {
    fn drop(&mut self) {
        unsafe { ffi::mdb_cursor_close(self.handle) };
    }
}

pub struct CursorItemAccessor<'c, 'k, K: 'k> {
    cursor: Cursor<'c>,
    key: &'k K,
}

impl<'k, 'c: 'k, K: AsByteSlice> CursorItemAccessor<'c, 'k, K> {
    pub fn get<'a, V: FromBytes + 'a>(&'a mut self) -> MdbResult<V> {
        try!(self.cursor.to_key(self.key));
        self.cursor.get_value()
    }

    pub fn add<V: AsByteSlice>(&mut self, v: &V) -> MdbResult<()> {
        self.cursor.set(self.key, v, 0)
    }

    pub fn del<V: AsByteSlice>(&mut self, v: &V) -> MdbResult<()> {
        try!(self.cursor.to_item(self.key, v));
        self.cursor.del_item()
    }

    pub fn del_all(&mut self) -> MdbResult<()> {
        try!(self.cursor.to_key(self.key));
        self.cursor.del_all()
    }

    /// Returns number of values in current item
    pub fn count(&self) -> MdbResult<usize> {
        let result = try!(self.cursor.get_item_count());
        Ok(result as usize)
    }

    /// Returns back internal cursor
    pub fn into_inner(self) -> Cursor<'c> {
        let tmp = self;
        tmp.cursor
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
    pub fn get_key<T: FromBytes + 'cursor>(&'cursor self) -> T {
        FromBytes::from_bytes(&self.key.as_ref())
    }

    pub fn get_value<T: FromBytes + 'cursor>(&'cursor self) -> T {
        FromBytes::from_bytes(&self.value.as_ref())
    }

    pub fn get<T: FromBytes + 'cursor, U: FromBytes + 'cursor>(&'cursor self) -> (T, U) {
        (FromBytes::from_bytes(&self.key.as_ref()),
         FromBytes::from_bytes(&self.value.as_ref()))
    }
}

/// This one should once become public and allow to create custom
/// iterators
trait CursorIteratorInner {
    /// Returns true if initialization successful, for example that
    /// the key exists.
    fn init_cursor<'a, 'b: 'a>(&'a self, cursor: &mut Cursor<'b>) -> bool;

    /// Returns true if there is still data and iterator is in correct range
    fn move_to_next<'iter, 'cursor: 'iter>(&'iter self, cursor: &'cursor mut Cursor<'cursor>) -> bool;

    /// Returns size hint considering current state of cursor
    fn get_size_hint(&self, _cursor: &Cursor) -> (usize, Option<usize>) {
        (0, None)
    }
}


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
            cursor: cursor,
        }
    }

    #[allow(dead_code)]
    fn unwrap(self) -> Cursor<'c> {
        self.cursor
    }
}

impl<'c, I: CursorIteratorInner + 'c> Iterator for CursorIterator<'c, I> {
    type Item = CursorValue<'c>;

    fn next(&mut self) -> Option<CursorValue<'c>> {
        if !self.has_data {
            None
        } else {
            match self.cursor.get_plain() {
                Err(_) => None,
                Ok((k, v)) => {
                    self.has_data = unsafe { self.inner.move_to_next(mem::transmute(&mut self.cursor)) };
                    Some(CursorValue {
                        key: k,
                        value: v,
                    })
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.get_size_hint(&self.cursor)
    }
}

pub struct CursorKeyRangeIter<'a, K: AsByteSlice+'a> {
    start_key: &'a K,
    end_key: &'a K,
    end_inclusive: bool,
}

impl<'a, K: AsByteSlice> CursorKeyRangeIter<'a, K> {
    pub fn new(start_key: &'a K, end_key: &'a K, end_inclusive: bool) -> CursorKeyRangeIter<'a, K> {
        CursorKeyRangeIter {
            start_key: start_key,
            end_key: end_key,
            end_inclusive: end_inclusive,
        }
    }
}

impl<'iter, K: AsByteSlice> CursorIteratorInner for CursorKeyRangeIter<'iter, K> {
    fn init_cursor<'a, 'b: 'a>(&'a self, cursor: &mut Cursor<'b>) -> bool {
        cursor.to_gte_key(self.start_key).is_ok() && cursor.cmp_key(self.end_key.as_byte_slice()).is_less(self.end_inclusive)
    }

    fn move_to_next<'i, 'c: 'i>(&'i self, cursor: &'c mut Cursor<'c>) -> bool {
        let moved = cursor.to_next_key().is_ok();
        if !moved {
            false
        } else {
            cursor.cmp_key(self.end_key.as_byte_slice()).is_less(self.end_inclusive)
        }
    }
}


pub struct CursorFromKeyIter<'a, K: AsByteSlice + 'a> {
    start_key: &'a K,
}


impl<'a, K: AsByteSlice> CursorFromKeyIter<'a, K> {
    pub fn new(start_key: &'a K) -> CursorFromKeyIter<'a, K> {
        CursorFromKeyIter {
            start_key: start_key,
        }
    }
}

impl<'iter, K: AsByteSlice> CursorIteratorInner for CursorFromKeyIter<'iter, K> {
    fn init_cursor<'a, 'b: 'a>(&'a self, cursor: & mut Cursor<'b>) -> bool {
        cursor.to_gte_key(self.start_key).is_ok()
    }

    fn move_to_next<'i, 'c: 'i>(&'i self, cursor: &'c mut Cursor<'c>) -> bool {
        cursor.to_next_key().is_ok()
    }
}


pub struct CursorToKeyIter<'a> {
    end_key: &'a [u8],
}


impl<'a> CursorToKeyIter<'a> {
    pub fn new<K: AsByteSlice+'a>(end_key: &'a K) -> CursorToKeyIter<'a> {
        CursorToKeyIter {
            end_key: end_key.as_byte_slice(),
        }
    }
}

impl<'iter> CursorIteratorInner for CursorToKeyIter<'iter> {
    fn init_cursor<'a, 'b: 'a>(&'a self, cursor: & mut Cursor<'b>) -> bool {
        let ok = cursor.to_first().is_ok();
        ok && cursor.cmp_key(&self.end_key).is_less(false)
    }

    fn move_to_next<'i, 'c: 'i>(&'i self, cursor: &'c mut Cursor<'c>) -> bool {
        let moved = cursor.to_next_key().is_ok();
        if !moved {
            false
        } else {
            cursor.cmp_key(&self.end_key).is_less(false)
        }
    }
}

#[allow(missing_copy_implementations)]
pub struct CursorIter;


impl<'iter> CursorIteratorInner for CursorIter {
    fn init_cursor<'a, 'b: 'a>(&'a self, cursor: & mut Cursor<'b>) -> bool {
        cursor.to_first().is_ok()
    }

    fn move_to_next<'i, 'c: 'i>(&'i self, cursor: &'c mut Cursor<'c>) -> bool {
        cursor.to_next_key().is_ok()
    }
}


pub struct CursorItemIter<'a, K: AsByteSlice + 'a> {
    key: &'a K,
}


impl<'a, K: AsByteSlice> CursorItemIter<'a, K> {
    pub fn new(key: &'a K) -> CursorItemIter<'a, K> {
        CursorItemIter {
            key: key,
        }
    }
}

impl<'iter, K: AsByteSlice> CursorIteratorInner for CursorItemIter<'iter, K> {
    fn init_cursor<'a, 'b: 'a>(&'a self, cursor: & mut Cursor<'b>) -> bool {
        cursor.to_key(self.key).is_ok()
    }

    fn move_to_next<'i, 'c: 'i>(&'i self, cursor: &'c mut Cursor<'c>) -> bool {
        cursor.to_next_item().is_ok()
    }

    fn get_size_hint(&self, c: &Cursor) -> (usize, Option<usize>) {
        match c.get_item_count() {
            Err(_) => (0, None),
            Ok(cnt) => (0, Some(cnt as usize))
        }
    }
}


#[derive(Copy, Clone)]
pub struct MdbValue<'a> {
    value: MDB_val,
    marker: ::std::marker::PhantomData<&'a ()>,
}

impl<'a> MdbValue<'a> {
    pub unsafe fn new(data: *const c_void, len: usize) -> MdbValue<'a> {
        MdbValue {
            value: MDB_val {
                mv_data: data,
                mv_size: len as size_t
            },
            marker: ::std::marker::PhantomData
        }
    }

    pub fn new_from_sized<T>(data: &'a T) -> MdbValue<'a> {
        unsafe {
            MdbValue::new(mem::transmute(data), mem::size_of::<T>())
        }
    }

    #[inline(always)]
    pub unsafe fn get_ref(&'a self) -> *const c_void {
        self.value.mv_data
    }

    #[inline(always)]
    pub fn get_size(&self) -> usize {
        self.value.mv_size as usize
    }
}

impl<'t> AsRef<[u8]> for MdbValue<'t> {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        unsafe {slice::from_raw_parts(mem::transmute(self.get_ref()), self.get_size())}
    }
}
