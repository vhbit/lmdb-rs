extern crate pkg_config;
extern crate gcc;

fn main() {
    if !pkg_config::find_library("liblmdb").is_ok() {
        gcc::compile_library("liblmdb.a",
                             &["mdb/libraries/liblmdb/mdb.c",
                               "mdb/libraries/liblmdb/midl.c"]);
    }
}
