extern crate gcc;

fn main() {
    let target = std::env::var("TARGET").unwrap();

    let mut config = gcc::Config::new();
    config.file("mdb/libraries/liblmdb/mdb.c")
          .file("mdb/libraries/liblmdb/midl.c");
    config.opt_level(2);

    if target.contains("dragonfly") {
        config.flag("-DMDB_DSYNC=O_SYNC");
        config.flag("-DMDB_FDATASYNC=fsync");
    }

    config.compile("liblmdb.a");
}
