extern crate lmdb_rs as lmdb;

use lmdb::{EnvBuilder, DbFlags};

fn main() {
    let env = EnvBuilder::new().open("test-lmdb", 0o777).unwrap();

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
