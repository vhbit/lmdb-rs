#![allow(unstable)]

extern crate "lmdb-rs" as lmdb;

use std::io::USER_DIR;
use lmdb::{EnvBuilder, DbFlags};

fn main() {
    let path = Path::new("test-lmdb");
    let mut env = EnvBuilder::new().open(&path, USER_DIR).unwrap();

    let db_handle = env.get_default_db(DbFlags::empty()).unwrap();
    let txn = env.new_transaction().unwrap();
    {
        let db = txn.bind(&db_handle); // get a database bound to this transaction

        let pairs = vec![("Albert", "Einstein",),
                         ("Joe", "Smith",),
                         ("Jack", "Daniels")];

        for &(name, surname) in pairs.iter() {
            db.set(&surname, &name).unwrap();
        }
    }

    // Note: `commit` is choosen to be explicit as
    // in case of failure it is responsibility of
    // the client to handle the error
    match txn.commit() {
        Err(_) => panic!("failed to commit!"),
        Ok(_) => ()
    }

    let reader = env.get_reader().unwrap();
    let db = reader.bind(&db_handle);
    let name = db.get::<&str>(&"Smith").unwrap();
    println!("It's {} Smith", name);
}
