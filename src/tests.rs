use std::io::fs::{mod, PathExtensions};
use std::rt::unwind;
use std::path::Path;

use core::{mod, Environment, EnvFlags, DbFlags, EnvNoMemInit, EnvNoMetaSync};

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

                        let test_flags = EnvNoMemInit | EnvNoMetaSync;

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

        let db = env.get_default_db(core::DbAllowDups).unwrap();
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

        let db = env.get_default_db(core::DbAllowDups).unwrap();
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
fn test_item_iter() {
    let path = Path::new("item_iter");
    test_db_in_path(&path, || {
        let mut env = Environment::new().unwrap();
        let _ = env.open(&path, EnvFlags::empty(), 0o755);
        let _ = env.set_maxdbs(5);

        let db = env.get_default_db(core::DbAllowDups).unwrap();
        let txn = env.new_transaction().unwrap();

        let test_key1 = "key1".to_string();
        let test_data1 = "value1".to_string();
        let test_data2 = "value2".to_string();
        let test_key2 = "key2".to_string();
        let test_key3 = "key3".to_string();

        let _ = db.set(&txn, &test_key1, &test_data1);
        let _ = db.set(&txn, &test_key1, &test_data2);
        let _ = db.set(&txn, &test_key2, &test_data1);

        let iter = db.item_iter(&txn, &test_key1).unwrap();
        let values: Vec<String> = iter.map(|cv| cv.get_value()).collect();
        assert_eq!(values.as_slice(), vec![test_data1.clone(), test_data2.clone()].as_slice());

        let iter = db.item_iter(&txn, &test_key2).unwrap();
        let values: Vec<String> = iter.map(|cv| cv.get_value()).collect();
        assert_eq!(values.as_slice(), vec![test_data1.clone()].as_slice());

        let iter = db.item_iter(&txn, &test_key3).unwrap();
        let values: Vec<String> = iter.map(|cv| cv.get_value()).collect();
        assert_eq!(values.len(), 0);
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
