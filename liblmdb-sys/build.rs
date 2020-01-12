extern crate cc;

fn main() {
    let target = std::env::var("TARGET").unwrap();

    let mut config = cc::Build::new();
    if target.contains("ios") {
        config
            .file("mdb_ios/libraries/liblmdb/mdb.c")
            .file("mdb_ios/libraries/liblmdb/midl.c");
    } else {
        config
            .file("mdb/libraries/liblmdb/mdb.c")
            .file("mdb/libraries/liblmdb/midl.c");
    }
    config.opt_level(2);

    if target.contains("dragonfly") {
        config.flag("-DMDB_DSYNC=O_SYNC");
        config.flag("-DMDB_FDATASYNC=fsync");
    }

    config.compile("liblmdb.a");
}
