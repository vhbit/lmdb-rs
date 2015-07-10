use std::env;
use std::fs::{self};
use std::path::{PathBuf};
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
use std::sync::{Once, ONCE_INIT};
use std::thread;

use core::{self, EnvBuilder, DbFlags, EnvNoMemInit, EnvNoMetaSync};

const USER_DIR: u32 = 0o777;
static TEST_ROOT_DIR: &'static str = "test-dbs";
static NEXT_ID: AtomicUsize = ATOMIC_USIZE_INIT;
static INIT_DIR_ONCE: Once = ONCE_INIT;

fn next_path() -> PathBuf {
    let out_dir = PathBuf::from(&env::var("OUT_DIR").unwrap());
    let root_dir = out_dir.join(TEST_ROOT_DIR);

    INIT_DIR_ONCE.call_once(|| {
        if let Ok(root_meta) = fs::metadata(root_dir.clone()) {
            if root_meta.is_dir() {
                let _ = fs::remove_dir_all(&root_dir);
            }
        }
        assert!(fs::create_dir_all(&root_dir).is_ok());
    });

    let cur_id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    let res = root_dir.join(&format!("db-{}", cur_id));
    println!("Testing db in {}", res.display());
    res
}

#[test]
fn test_environment() {
    let mut env = EnvBuilder::new()
        .max_readers(33)
        .open(&next_path(), USER_DIR).unwrap();

    env.sync(true).unwrap();

    let test_flags = EnvNoMemInit | EnvNoMetaSync;

    env.set_flags(test_flags, true).unwrap();
    let new_flags = env.get_flags().unwrap();
    assert!((new_flags & test_flags) == test_flags, "Get flags != set flags");

    let db = env.get_default_db(DbFlags::empty()).unwrap();
    let txn = env.new_transaction().unwrap();
    let db = txn.bind(&db);

    let key = "hello";
    let value = "world";

    db.set(&key, &value).unwrap();

    let v = db.get::<&str>(&key).unwrap();
    assert!(v == value, "Written {} and read {}", &value, &v);
}

#[test]
fn test_single_values() {
    let mut env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(DbFlags::empty()).unwrap();
    let txn = env.new_transaction().unwrap();
    let db = txn.bind(&db);

    let test_key1 = "key1";
    let test_data1 = "value1";
    let test_data2 = "value2";

    assert!(db.get::<()>(&test_key1).is_err(), "Key shouldn't exist yet");

    assert!(db.set(&test_key1, &test_data1).is_ok());
    let v = db.get::<&str>(&test_key1).unwrap();
    assert!(v == test_data1, "Data written differs from data read");

    assert!(db.set(&test_key1, &test_data2).is_ok());
    let v = db.get::<&str>(&test_key1).unwrap();
    assert!(v == test_data2, "Data written differs from data read");

    assert!(db.del(&test_key1).is_ok());
    assert!(db.get::<()>(&test_key1).is_err(), "Key should be deleted");
}

#[test]
fn test_multiple_values() {
    let mut env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(core::DbAllowDups).unwrap();
    let txn = env.new_transaction().unwrap();
    let db = txn.bind(&db);

    let test_key1 = "key1";
    let test_data1 = "value1";
    let test_data2 = "value2";

    assert!(db.get::<()>(&test_key1).is_err(), "Key shouldn't exist yet");

    assert!(db.set(&test_key1, &test_data1).is_ok());
    let v = db.get::<&str>(&test_key1).unwrap();
    assert!(v == test_data1, "Data written differs from data read");

    assert!(db.set(&test_key1, &test_data2).is_ok());
    let v = db.get::<&str>(&test_key1).unwrap();
    assert!(v == test_data1, "It should still return first value");

    assert!(db.del_item(&test_key1, &test_data1).is_ok());

    let v = db.get::<&str>(&test_key1).unwrap();
    assert!(v == test_data2, "It should return second value");
    assert!(db.del(&test_key1).is_ok());

    assert!(db.get::<()>(&test_key1).is_err(), "Key shouldn't exist anymore!");
}

#[test]
fn test_stat() {
    let mut env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    // ~ the two dataset; each to end up in its own database
    let dss = [
        // ~ keep the "default db" dataset here at the beginning (see
        // the assertion at the end of this test)
        ("", vec![("default", "db"), ("has", "some"), ("extras", "prepared")]),
        ("db1", vec![("foo", "bar"), ("quux", "qak")]),
        ("db2", vec![("a", "abc"), ("b", "bcd"), ("c", "cde"), ("d", "def")]),
        ("db3", vec![("hip", "hop")])];

    // ~ create each db, populate it, and assert db.stat() for each seperately
    for &(name, ref ds) in &dss {
        let db = env.create_db(name, DbFlags::empty()).unwrap();
        let tx = env.new_transaction().unwrap();
        {
            let db = tx.bind(&db);
            for &(k, v) in ds {
                assert!(db.set(&k, &v).is_ok());
            }
            // ~ verify the expected number of entries (key/value pairs) in the db
            let stat = db.stat().unwrap();
            assert_eq!(ds.len() as u64, stat.ms_entries);
        }
        tx.commit().unwrap();
    }

    // ~ now verify the number of data items in this _environment_ (this
    // is the number key/value pairs in the default database plus the
    // number of other databases)
    let stat = env.stat().unwrap();
    assert_eq!(dss[0].1.len() as u64 + dss[1..].len() as u64, stat.ms_entries);
}


#[test]
fn test_cursors() {
    let mut env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(core::DbAllowDups).unwrap();
    let txn = env.new_transaction().unwrap();
    let db = txn.bind(&db);

    let test_key1 = "key1";
    let test_key2 = "key2";
    let test_values: Vec<&str> = vec!("value1", "value2", "value3", "value4");

    assert!(db.get::<()>(&test_key1).is_err(), "Key shouldn't exist yet");

    for t in test_values.iter() {
        let _ = db.set(&test_key1, t);
        let _ = db.set(&test_key2, t);
    }

    let mut cursor = db.new_cursor().unwrap();
    assert!(cursor.to_first().is_ok());

    assert!(cursor.to_key(&test_key1).is_ok());
    assert!(cursor.item_count().unwrap() == 4);

    assert!(cursor.del_item().is_ok());
    assert!(cursor.item_count().unwrap() == 3);

    assert!(cursor.to_key(&test_key1).is_ok());
    let new_value = "testme";

    assert!(cursor.replace(&new_value).is_ok());
    {
        let (_, v) = cursor.get::<(), &str>().unwrap();
        // NOTE: this asserting will work once new_value is
        // of the same length as it is inplace change
        assert!(v == new_value);
    }

    assert!(cursor.del_all().is_ok());
    assert!(cursor.to_key(&test_key1).is_err());

    assert!(cursor.to_key(&test_key2).is_ok());
}


#[test]
fn test_cursor_item_manip() {
    let mut env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(core::DbAllowDups | core::DbAllowIntDups).unwrap();
    let txn = env.new_transaction().unwrap();
    let db = txn.bind(&db);

    let test_key1 = "key1";

    assert!(db.set(&test_key1, &3u64).is_ok());

    let mut cursor = db.new_cursor().unwrap();
    assert!(cursor.to_key(&test_key1).is_ok());

    let values: Vec<u64> = db.item_iter(&test_key1).unwrap()
        .map(|cv| cv.get_value::<u64>())
        .collect();
    assert_eq!(values, vec![3u64]);

    assert!(cursor.add_item(&4u64).is_ok());
    assert!(cursor.add_item(&5u64).is_ok());

    let values: Vec<u64> = db.item_iter(&test_key1).unwrap()
        .map(|cv| cv.get_value::<u64>())
        .collect();
    assert_eq!(values, vec![3u64, 4, 5]);

    assert!(cursor.replace(&6u64).is_ok());
    let values: Vec<u64> = db.item_iter(&test_key1).unwrap()
        .map(|cv| cv.get_value::<u64>())
        .collect();

    assert_eq!(values, vec![3u64, 4, 6]);
}

fn as_slices(v: &Vec<String>) -> Vec<&str> {
    v.iter().map(|s| &s[..]).collect::<Vec<&str>>()
}

#[test]
fn test_item_iter() {
    let mut env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let db = env.get_default_db(core::DbAllowDups).unwrap();
    let txn = env.new_transaction().unwrap();
    let db = txn.bind(&db);

    let test_key1 = "key1";
    let test_data1 = "value1";
    let test_data2 = "value2";
    let test_key2 = "key2";
    let test_key3 = "key3";

    assert!(db.set(&test_key1, &test_data1).is_ok());
    assert!(db.set(&test_key1, &test_data2).is_ok());
    assert!(db.set(&test_key2, &test_data1).is_ok());

    let iter = db.item_iter(&test_key1).unwrap();
    let values: Vec<String> = iter.map(|cv| cv.get_value::<String>()).collect();
    assert_eq!(as_slices(&values), vec![test_data1, test_data2]);

    let iter = db.item_iter(&test_key2).unwrap();
    let values: Vec<String> = iter.map(|cv| cv.get_value::<String>()).collect();
    assert_eq!(as_slices(&values), vec![test_data1]);

    let iter = db.item_iter(&test_key3).unwrap();
    let values: Vec<String> = iter.map(|cv| cv.get_value::<String>()).collect();
    assert_eq!(values.len(), 0);
}

#[test]
fn test_db_creation() {
    let mut env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();
    assert!(env.create_db("test-db", DbFlags::empty()).is_ok());
}

#[test]
fn test_read_only_txn() {
    let env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();
    env.get_reader().unwrap();
}

#[test]
fn test_cursor_in_txns() {
    let mut env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    {
        let db = env.create_db("test1", core::DbAllowDups | core::DbAllowIntDups).unwrap();
        let txn = env.new_transaction().unwrap();
        {
            let db = txn.bind(&db);

            let cursor = db.new_cursor();
            assert!(cursor.is_ok());
        }
        assert!(txn.commit().is_ok());
    }

    {
        let db = env.create_db("test1", core::DbAllowDups | core::DbAllowIntDups).unwrap();
        let txn = env.new_transaction().unwrap();
        {
            let db = txn.bind(&db);

            let cursor = db.new_cursor();
            assert!(cursor.is_ok());
        }
        assert!(txn.commit().is_ok());
    }
}

#[test]
fn test_multithread_env() {
    let mut env = EnvBuilder::new()
        .max_dbs(5)
        .open(&next_path(), USER_DIR)
        .unwrap();

    let mut shared_env = env.clone();
    let key = "key";
    let value = "value";

    let _ = thread::spawn(move || {
        let db = shared_env.create_db("test1", DbFlags::empty()).unwrap();
        let txn = shared_env.new_transaction().unwrap();
        {
            let db = txn.bind(&db);
            assert!(db.set(&key, &value).is_ok());
        }
        assert!(txn.commit().is_ok());
    }).join();

    let db = env.create_db("test1", DbFlags::empty()).unwrap();
    let txn = env.get_reader().unwrap();
    let db = txn.bind(&db);
    let value2: String = db.get(&key).unwrap();
    assert_eq!(value, value2);
}

#[test]
fn test_keyrange_to() {
    let mut env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(core::DbIntKey).unwrap();
    let keys:   Vec<u64> = vec![1, 2, 3];
    let values: Vec<u64> = vec![5, 6, 7];

    // to avoid problems caused by updates
    assert_eq!(keys.len(), values.len());

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db);
        for (k, v) in keys.iter().zip(values.iter()) {
            assert!(db.set(k, v).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {
        let db = txn.bind(&db);

        let last_idx = keys.len() - 1;
        let last_key: u64 = keys[last_idx];
        // last key is excluded
        let iter = db.keyrange_to(&last_key).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();
        assert_eq!(res, &values[..last_idx]);
    }
}

/// Test that selecting a key range with an upper bound smaller than
/// the smallest key in the db yields an empty range.
#[test]
fn test_keyrange_to_init_cursor() {
    let mut env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(core::DbIntKey).unwrap();
    let recs: Vec<(u64, u64)> = vec![(10, 50), (11, 60), (12, 70)];

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db);
        for &(k, v) in recs.iter() {
            assert!(db.set(&k, &v).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {
        let db = txn.bind(&db);

        // last key is excluded
        let upper_bound: u64 = 1;
        let iter = db.keyrange_to(&upper_bound).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();
        assert_eq!(res, &[]);
    }
}

#[test]
fn test_keyrange_from() {
    let mut env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(core::DbIntKey).unwrap();
    let keys:   Vec<u64> = vec![1, 2, 3];
    let values: Vec<u64> = vec![5, 6, 7];

    // to avoid problems caused by updates
    assert_eq!(keys.len(), values.len());

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db);
        for (k, v) in keys.iter().zip(values.iter()) {
            assert!(db.set(k, v).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {
        let db = txn.bind(&db);

        let start_idx = 1; // second key
        let last_key: u64 = keys[start_idx];
        let iter = db.keyrange_from(&last_key).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();
        assert_eq!(res, &values[start_idx..]);
    }
}

/// Test that selecting a key range with a lower bound greater than
/// the biggest key in the db yields an empty range.
#[test]
fn test_keyrange_from_init_cursor() {
    let mut env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(core::DbIntKey).unwrap();
    let recs: Vec<(u64, u64)> = vec![(10, 50), (11, 60), (12, 70)];

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db);
        for &(k, v) in recs.iter() {
            assert!(db.set(&k, &v).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {
        let db = txn.bind(&db);

        // last key is excluded
        let lower_bound = recs[recs.len()-1].0 + 1;
        let iter = db.keyrange_from(&lower_bound).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();
        assert_eq!(res, &[]);
    }
}

#[test]
fn test_keyrange() {
    let mut env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(core::DbAllowDups | core::DbIntKey).unwrap();
    let keys:   Vec<u64> = vec![ 1,  2,  3,  4,  5,  6];
    let values: Vec<u64> = vec![10, 11, 12, 13, 14, 15];

    // to avoid problems caused by updates
    assert_eq!(keys.len(), values.len());

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db);
        for (k, v) in keys.iter().zip(values.iter()) {
            assert!(db.set(k, v).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {
        let db = txn.bind(&db);

        let start_idx = 1;
        let end_idx = 3;
        let iter = db.keyrange(&keys[start_idx], &keys[end_idx]).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();

         //  +1 as Rust slices do not include end
        assert_eq!(res, &values[start_idx.. end_idx + 1]);
    }
}

/// Test that select a key range outside the available data correctly
/// yields an empty range.
#[test]
fn test_keyrange_init_cursor() {
    let mut env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(core::DbAllowDups | core::DbIntKey).unwrap();
    let keys:   Vec<u64> = vec![ 1,  2,  3,  4,  5,  6];
    let values: Vec<u64> = vec![10, 11, 12, 13, 14, 15];

    // to avoid problems caused by updates
    assert_eq!(keys.len(), values.len());

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db);
        for (k, v) in keys.iter().zip(values.iter()) {
            assert!(db.set(k, v).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    // test the cursor initialization before the available data range
    let txn = env.get_reader().unwrap();
    {
        let db = txn.bind(&db);

        let start_key = 0u64;
        let end_key = 0u64;
        let iter = db.keyrange(&start_key, &end_key).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();
        assert_eq!(res, &[]);
    }

    // test the cursor initialization after the available data range
    {
        let db = txn.bind(&db);

        let start_key = 10;
        let end_key = 20;
        let iter = db.keyrange(&start_key, &end_key).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();
        assert!(res.is_empty());
    }
}

#[test]
fn test_keyrange_from_to() {
    let mut env = EnvBuilder::new().open(&next_path(), USER_DIR).unwrap();
    let db = env.get_default_db(core::DbAllowDups | core::DbIntKey).unwrap();
    let recs: Vec<(u64, u64)> = vec![(10, 11), (20, 21), (30, 31), (40, 41), (50, 51)];

    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db);
        for &(k, v) in recs.iter() {
            assert!(db.set(&k, &v).is_ok());
        }
    }
    assert!(txn.commit().is_ok());

    let txn = env.get_reader().unwrap();
    {
        let db = txn.bind(&db);

        let start_idx = 1;
        let end_idx = 3;
        let iter = db.keyrange_from_to(&recs[start_idx].0, &recs[end_idx].0).unwrap();

        let res: Vec<_> = iter.map(|cv| cv.get_value::<u64>()).collect();
        // ~ end_key must be excluded here
        let exp: Vec<_> = recs[start_idx .. end_idx].iter().map(|x| x.1).collect();
        assert_eq!(res, exp);
    }
}

/*
#[test]
fn test_compilation_of_moved_items() {
    let path = Path::new("dbcom");
    test_db_in_path(&next_path(), || {
        let mut env = EnvBuilder::new()
            .max_dbs(5)
            .open(&next_path(), USER_DIR)
            .unwrap();

        let db = env.get_default_db(DbFlags::empty()).unwrap();
        let mut txn = env.new_transaction().unwrap();

        txn.commit();

        let test_key1 = "key1";
        let test_data1 = "value1";

        assert!(db.get::<()>(&txn, &test_key1).is_err(), "Key shouldn't exist yet"); // ~ERROR: use of moved value
    })
}
*/
