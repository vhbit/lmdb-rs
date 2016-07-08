extern crate pkg_config;
extern crate gcc;

fn main() {
    let target = std::env::var("TARGET").unwrap();

    if !pkg_config::find_library("liblmdb").is_ok() {
        let mut config = gcc::Config::new();
        config.file("mdb/libraries/liblmdb/mdb.c")
              .file("mdb/libraries/liblmdb/midl.c");

        if target.contains("dragonfly") {
            config.flag("-DMDB_DSYNC=O_SYNC");
            config.flag("-DMDB_FDATASYNC=fsync");
        }

        config.compile("liblmdb.a");
    }
}
