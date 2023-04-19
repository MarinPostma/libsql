use std::path::PathBuf;
use std::env;

fn main() {
    println!("cargo:rustc-link-search=../.libs");
    println!("cargo:rustc-link-search=../rust-vm/target/debug/");
    println!("cargo:rustc-link-lib=static=sqlite3");
    println!("cargo:rustc-link-lib=static=rusty_vdbe");
    println!("cargo:rerun-if-changed=../sqlite3.h");
    println!("cargo:rerun-if-changed=../.libs/libsql.a");

    let bindings = bindgen::Builder::default()
        .header("../sqlite3.h")
        .clang_args(["-I.."])
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("sqliteInt.rs"))
        .expect("Couldn't write bindings!");
}
