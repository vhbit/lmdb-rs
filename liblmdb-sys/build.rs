use std::os;
use std::io::{mod, fs, Command};
use std::io::process::InheritFd;

static STATIC_LIB_NAME: &'static str = "liblmdb.a";

fn run(cmd: &mut Command) {
    println!("running: {}", cmd);
    assert!(cmd.stdout(InheritFd(1))
            .stderr(InheritFd(2))
            .status()
            .unwrap()
            .success());
}

fn main() {
    let mut cmd = Command::new("make");

    let root = Path::new(os::getenv("CARGO_MANIFEST_DIR").unwrap());
    let dst = Path::new(os::getenv("OUT_DIR").unwrap());

    let mdb_root = root.join_many(vec!["mdb", "libraries", "liblmdb"].as_slice());
    let lib_dir = dst.clone();

    cmd.arg("-C").arg(mdb_root.clone());

    let mut clean_cmd = cmd.clone();
    clean_cmd.arg("clean");
    run(&mut clean_cmd);

    let mut build_cmd = cmd.clone();
    build_cmd.arg("liblmdb.a");
    run(&mut build_cmd);

    run(Command::new("cp")
        .arg(mdb_root.join(STATIC_LIB_NAME))
        .arg(lib_dir.join(STATIC_LIB_NAME)));

    println!("cargo:rustc-flags=-L {} -l lmdb:static", lib_dir.display());
}
