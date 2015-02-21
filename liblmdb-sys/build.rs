#![feature(core)]
#![feature(env)]
#![feature(process)]
#![feature(path)]

use std::env;
use std::path::{PathBuf};
use std::process::{Command, Stdio};

static STATIC_LIB_NAME: &'static str = "liblmdb.a";

fn run(cmd: &mut Command) {
    println!("running: {:?}", cmd);
    let res = cmd.stdout(Stdio::inherit())
                 .stderr(Stdio::inherit())
                 .status()
                 .unwrap()
                 .success();
    assert!(res);
}

fn ios_cflags(target: &str) -> String {
    let mut cflags = String::new();

    let (sdk_name, sdk_min_ver) = if target.contains("arm") | target.contains("aarch64") {
        ("iphoneos", "ios-version-min")
    } else {
        ("iphonesimulator", "ios-simulator-version-min")
    };

    let sdk_output = Command::new("xcrun").arg("--show-sdk-path")
                                          .arg("--sdk")
                                          .arg(sdk_name)
                                          .stderr(Stdio::inherit())
                                          .output()
                                          .unwrap()
                                          .stdout;
    let sdk_path = String::from_utf8_lossy(sdk_output.as_slice());

    let target = target.replace("aarch64-", "arm64-");

    let flags = format!(" -target {} -isysroot {} -m{}=7.0", target, sdk_path.as_slice().trim(), sdk_min_ver);
    cflags.push_str(flags.as_slice());

    // Actually only "arm" arch requires a little patching
    // other branches simply filter out invalid archs
    if !target.starts_with("arm-")
        && !target.starts_with("armv7-")
        && !target.starts_with("arm64-")
        && !target.starts_with("aarch64-")
        && !target.starts_with("armv7s-")
        && !target.starts_with("x86_64-")
        && !target.starts_with("i386-") {
            panic!("Unsupported target for iOS: `{}`", target)
    }

    if target.starts_with("arm-") {
        cflags.push_str(format!(" -arch armv7").as_slice());
    }

    cflags
}


fn cflags() -> String {
    let mut cflags = env::var("CFLAGS").unwrap_or(String::new());

    let target = env::var("TARGET").unwrap();
    // let profile = os::getenv("PROFILE").unwrap();

    if target.contains("-ios") {
        cflags.push_str(" ");
        cflags.push_str(ios_cflags(target.as_slice()).as_slice());
    } else {
        if target.contains("i686") || target.contains("i386") {
            cflags.push_str(" -m32");
        } else if target.as_slice().contains("x86_64") {
            cflags.push_str(" -m64");
        }

        if !target.contains("i686") {
            cflags.push_str(" -fPIC");
        }
    }

    cflags
}

fn main() {
    let root = PathBuf::new(&env::var("CARGO_MANIFEST_DIR").unwrap());
    let dst = PathBuf::new(&env::var("OUT_DIR").unwrap());

    let mut mdb_root = root.clone();

    for sub_dir in vec!["mdb", "libraries", "liblmdb"].iter() {
        mdb_root.push(sub_dir)
    }

    let lib_dir = dst.clone();

    let mut clean_cmd = Command::new("make");
    clean_cmd.arg("-C").arg(&mdb_root);
    clean_cmd.arg("clean");
    run(&mut clean_cmd);

    let mut build_cmd = Command::new("make");
    build_cmd.arg("-C").arg(&mdb_root);
    build_cmd.arg("liblmdb.a");
    build_cmd.arg(&format!("XCFLAGS={}", cflags()));
    run(&mut build_cmd);

    run(Command::new("cp")
        .arg(&mdb_root.join(STATIC_LIB_NAME))
        .arg(&lib_dir.join(STATIC_LIB_NAME)));

    println!("cargo:rustc-flags=-L {} -l lmdb:static", lib_dir.display());
}
