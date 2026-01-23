use std::env;
use std::path::PathBuf;

fn main() {
    let lib_dir = env::var("CHRONDB_LIB_DIR")
        .unwrap_or_else(|_| "../../target".to_string());

    let lib_path = PathBuf::from(&lib_dir);

    // Tell cargo to look for the shared library
    println!("cargo:rustc-link-search=native={}", lib_path.display());
    println!("cargo:rustc-link-lib=dylib=chrondb");

    // Re-run if the library changes
    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-env-changed=CHRONDB_LIB_DIR");

    // Generate bindings
    let header_path = lib_path.join("libchrondb.h");
    let graal_header = lib_path.join("graal_isolate.h");

    // Only generate bindings if headers exist (they won't during CI without the lib)
    if header_path.exists() && graal_header.exists() {
        let bindings = bindgen::Builder::default()
            .header(header_path.to_str().unwrap())
            .clang_arg(format!("-I{}", lib_path.display()))
            .allowlist_function("chrondb_.*")
            .allowlist_function("graal_create_isolate")
            .allowlist_function("graal_tear_down_isolate")
            .allowlist_type("graal_isolate_t")
            .allowlist_type("graal_isolatethread_t")
            .allowlist_type("graal_create_isolate_params_t")
            .generate()
            .expect("Unable to generate bindings");

        let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
        bindings
            .write_to_file(out_path.join("bindings.rs"))
            .expect("Couldn't write bindings!");
    } else {
        // Generate stub bindings for compilation without the library
        let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
        std::fs::write(
            out_path.join("bindings.rs"),
            include_str!("src/ffi_stub.rs"),
        )
        .expect("Couldn't write stub bindings!");
    }
}
