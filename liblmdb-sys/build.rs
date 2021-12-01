extern crate cc;

fn main() {
    let target = std::env::var("TARGET").unwrap();

    let mut config = cc::Build::new();
    config.file("mdb/libraries/liblmdb/mdb.c")
          .file("mdb/libraries/liblmdb/midl.c");
    config.opt_level(2);
    config.flag("-Wno-unused-parameter");
    config.flag("-Wno-implicit-fallthrough");

    if target.contains("dragonfly") {
        config.flag("-DMDB_DSYNC=O_SYNC");
        config.flag("-DMDB_FDATASYNC=fsync");
    }

    config.compile("liblmdb.a");
}
