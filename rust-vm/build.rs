use std::env;
use std::path::PathBuf;

fn main() {
    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=../src/sqliteInt.h");

    let bindings = bindgen::Builder::default()
        .header("../src/sqliteInt.h")
        .header("../src/vdbeInt.h")
        .clang_args(["-I.."])
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("sqliteInt.rs"))
        .expect("Couldn't write bindings!");
}
