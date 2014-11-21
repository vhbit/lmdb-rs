use std::io::fs::{mod, PathExtensions};
use std::io::USER_DIR;
use rustrt::unwind;
use std::path::Path;

use core::{mod, EnvBuilder, DbFlags, EnvNoMemInit, EnvNoMetaSync};

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
        let mut env = EnvBuilder::new()
            .max_readers(33)
            .open(&path, USER_DIR).unwrap();

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
        assert!(v.as_slice() == value.as_slice(), "Written {} and read {}", value.as_slice(), v.as_slice());
    });
}

#[test]
fn test_single_values() {
    let path = Path::new("single-values");
    test_db_in_path(&path, || {
        let mut env = EnvBuilder::new()
            .max_dbs(5)
            .open(&path, USER_DIR)
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
        assert!(v.as_slice() == test_data1.as_slice(), "Data written differs from data read");

        assert!(db.set(&test_key1, &test_data2).is_ok());
        let v = db.get::<&str>(&test_key1).unwrap();
        assert!(v.as_slice() == test_data2.as_slice(), "Data written differs from data read");

        assert!(db.del(&test_key1).is_ok());
        assert!(db.get::<()>(&test_key1).is_err(), "Key should be deleted");
    });
}

#[test]
fn test_multiple_values() {
    let path = Path::new("multiple-values");
    test_db_in_path(&path, || {
        let mut env = EnvBuilder::new()
            .max_dbs(5)
            .open(&path, USER_DIR)
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
        assert!(v.as_slice() == test_data1.as_slice(), "Data written differs from data read");

        assert!(db.set(&test_key1, &test_data2).is_ok());
        let v = db.get::<&str>(&test_key1).unwrap();
        assert!(v.as_slice() == test_data1.as_slice(), "It should still return first value");

        assert!(db.del_item(&test_key1, &test_data1).is_ok());

        let v = db.get::<&str>(&test_key1).unwrap();
        assert!(v.as_slice() == test_data2.as_slice(), "It should return second value");
        assert!(db.del(&test_key1).is_ok());

        assert!(db.get::<()>(&test_key1).is_err(), "Key shouldn't exist anymore!");
    });
}

#[test]
fn test_cursors() {
    let path = Path::new("cursors");
    test_db_in_path(&path, || {
        let mut env = EnvBuilder::new()
            .max_dbs(5)
            .open(&path, USER_DIR)
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
            assert!(v.as_slice() == new_value.as_slice());
        }

        assert!(cursor.del_all().is_ok());
        assert!(cursor.to_key(&test_key1).is_err());

        assert!(cursor.to_key(&test_key2).is_ok());
    });
}


#[test]
fn test_cursor_item_manip() {
    let path = Path::new("cursors-items");
    test_db_in_path(&path, || {
        let mut env = EnvBuilder::new()
            .max_dbs(5)
            .open(&path, USER_DIR)
            .unwrap();

        let db = env.get_default_db(core::DbAllowDups | core::DbAllowIntDups).unwrap();
        let txn = env.new_transaction().unwrap();
        let db = txn.bind(&db);

        let test_key1 = "key1";

        assert!(db.set(&test_key1, &3u64).is_ok());
        let mut cursor = db.new_cursor().unwrap();
        assert!(cursor.to_key(&test_key1).is_ok());

        let values: Vec<u64> = db.item_iter(&test_key1).unwrap()
            .map(|cv| *cv.get_value::<u64>())
            .collect();
        assert_eq!(values, vec![3u64]);

        assert!(cursor.add_item(&4u64).is_ok());
        assert!(cursor.add_item(&5u64).is_ok());

        let values: Vec<u64> = db.item_iter(&test_key1).unwrap()
            .map(|cv| *cv.get_value::<u64>())
            .collect();
        assert_eq!(values, vec![3u64, 4, 5]);

        assert!(cursor.replace(&6u64).is_ok());
        let values: Vec<u64> = db.item_iter(&test_key1).unwrap()
            .map(|cv| *cv.get_value::<u64>())
            .collect();

        assert_eq!(values, vec![3u64, 4, 6]);
    });
}

fn as_slices(v: &Vec<String>) -> Vec<&str> {
    v.iter().map(|s| s.as_slice()).collect::<Vec<&str>>()
}

#[test]
fn test_item_iter() {
    let path = Path::new("item_iter");
    test_db_in_path(&path, || {
        let mut env = EnvBuilder::new()
            .max_dbs(5)
            .open(&path, USER_DIR)
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
        let values: Vec<String> = iter.map(|cv| cv.get_value::<String>().to_owned()).collect();
        assert_eq!(as_slices(&values).as_slice(), vec![test_data1, test_data2].as_slice());

        let iter = db.item_iter(&test_key2).unwrap();
        let values: Vec<String> = iter.map(|cv| cv.get_value::<String>().to_owned()).collect();
        assert_eq!(as_slices(&values).as_slice(), vec![test_data1].as_slice());

        let iter = db.item_iter(&test_key3).unwrap();
        let values: Vec<String> = iter.map(|cv| cv.get_value::<String>().to_owned()).collect();
        assert_eq!(values.len(), 0);
    });
}

#[test]
fn test_db_creation() {
    let path = Path::new("dbs");
    test_db_in_path(&path, || {
        let mut env = EnvBuilder::new()
            .max_dbs(5)
            .open(&path, USER_DIR)
            .unwrap();
        assert!(env.create_db("test-db", DbFlags::empty()).is_ok());
    });
}

#[test]
fn test_read_only_txn() {
    let path = Path::new("ro_txn");
    test_db_in_path(&path, || {
        let env = EnvBuilder::new()
            .max_dbs(5)
            .open(&path, USER_DIR)
            .unwrap();
        env.get_reader().unwrap();
    });
}

#[test]
fn test_cursor_in_txns() {
    let path = Path::new("cursors-txns");
    test_db_in_path(&path, || {
        let mut env = EnvBuilder::new()
            .max_dbs(5)
            .open(&path, USER_DIR)
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
    });
}


/*
#[test]
fn test_compilation_of_moved_items() {
    let path = Path::new("dbcom");
    test_db_in_path(&path, || {
        let mut env = EnvBuilder::new()
            .max_dbs(5)
            .open(&path, USER_DIR)
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
