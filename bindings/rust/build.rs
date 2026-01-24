use std::env;
use std::fs;
use std::path::PathBuf;

fn download_library(lib_dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let target_os = env::var("CARGO_CFG_TARGET_OS")?;
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH")?;
    let pkg_version = env::var("CARGO_PKG_VERSION")?;

    let platform = match (target_os.as_str(), target_arch.as_str()) {
        ("linux", "x86_64") => "linux-x86_64",
        ("linux", "aarch64") => "linux-aarch64",
        ("macos", "x86_64") => "macos-x86_64",
        ("macos", "aarch64") => "macos-aarch64",
        _ => {
            eprintln!(
                "cargo:warning=No pre-built library for {}-{}. Set CHRONDB_LIB_DIR manually.",
                target_os, target_arch
            );
            return Ok(());
        }
    };

    // Map crate version to release tag
    let release_tag = if pkg_version.contains("-dev") {
        "latest".to_string()
    } else {
        format!("v{}", pkg_version)
    };

    let version_label = if pkg_version.contains("-dev") {
        "latest".to_string()
    } else {
        pkg_version.clone()
    };

    let url = format!(
        "https://github.com/moclojer/chrondb/releases/download/{}/libchrondb-{}-{}.tar.gz",
        release_tag, version_label, platform
    );

    eprintln!("cargo:warning=Downloading ChronDB library from {}", url);

    let response = ureq::get(&url).call()?;
    let mut reader = response.into_reader();

    let decoder = flate2::read::GzDecoder::new(&mut reader);
    let mut archive = tar::Archive::new(decoder);

    fs::create_dir_all(lib_dir)?;
    archive.unpack(lib_dir)?;

    // Find the extracted directory and flatten lib/ and include/ into lib_dir
    let entries: Vec<_> = fs::read_dir(lib_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .collect();

    if let Some(extracted) = entries.first() {
        let extracted_path = extracted.path();

        // Move lib/* to lib_dir
        let lib_subdir = extracted_path.join("lib");
        if lib_subdir.exists() {
            for entry in fs::read_dir(&lib_subdir)? {
                let entry = entry?;
                let dest = lib_dir.join(entry.file_name());
                fs::rename(entry.path(), dest)?;
            }
        }

        // Move include/* to lib_dir (headers needed by bindgen)
        let include_subdir = extracted_path.join("include");
        if include_subdir.exists() {
            for entry in fs::read_dir(&include_subdir)? {
                let entry = entry?;
                let dest = lib_dir.join(entry.file_name());
                fs::rename(entry.path(), dest)?;
            }
        }

        // Clean up extracted directory
        fs::remove_dir_all(&extracted_path).ok();
    }

    Ok(())
}

fn main() {
    let lib_dir = match env::var("CHRONDB_LIB_DIR") {
        Ok(dir) => PathBuf::from(dir),
        Err(_) => {
            // No explicit path: download from GitHub Releases
            let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
            let download_dir = out_dir.join("chrondb-lib");

            // Check if already downloaded (avoid re-downloading on rebuild)
            let header_check = download_dir.join("libchrondb.h");
            if !header_check.exists() {
                if let Err(e) = download_library(&download_dir) {
                    eprintln!("cargo:warning=Failed to download ChronDB library: {}", e);
                    eprintln!("cargo:warning=Set CHRONDB_LIB_DIR to the path containing the library.");
                    // Fall through to stub bindings
                    generate_stub_bindings();
                    return;
                }
            }

            download_dir
        }
    };

    // Tell cargo to look for the shared library
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=chrondb");

    // Set rpath so the binary finds the library at runtime
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    match target_os.as_str() {
        "macos" => {
            // Relative: finds lib next to the binary (for distribution)
            println!("cargo:rustc-link-arg=-Wl,-rpath,@executable_path");
            println!("cargo:rustc-link-arg=-Wl,-rpath,@executable_path/../lib");
            // Absolute: finds lib in build dir (for development)
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
        }
        "linux" => {
            // Relative: finds lib next to the binary (for distribution)
            println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN");
            println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/../lib");
            // Absolute: finds lib in build dir (for development)
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
        }
        _ => {}
    }

    // Copy library to target profile dir so `cargo run` works directly
    copy_lib_to_target_dir(&lib_dir, &target_os);

    // Export library path for downstream build scripts
    println!("cargo:root={}", lib_dir.display());

    // Re-run if the library changes
    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-env-changed=CHRONDB_LIB_DIR");

    // Generate bindings
    let header_path = lib_dir.join("libchrondb.h");
    let graal_header = lib_dir.join("graal_isolate.h");

    if header_path.exists() && graal_header.exists() {
        let bindings = bindgen::Builder::default()
            .header(header_path.to_str().unwrap())
            .clang_arg(format!("-I{}", lib_dir.display()))
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
        generate_stub_bindings();
    }
}

fn copy_lib_to_target_dir(lib_dir: &PathBuf, target_os: &str) {
    let lib_name = match target_os {
        "macos" => "libchrondb.dylib",
        "linux" => "libchrondb.so",
        "windows" => "chrondb.dll",
        _ => return,
    };

    let src = lib_dir.join(lib_name);
    if !src.exists() {
        return;
    }

    // OUT_DIR is: target/{profile}/build/{crate}-{hash}/out
    // Target profile dir: target/{profile}/
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    if let Some(target_profile_dir) = out_dir.ancestors().nth(3) {
        let dest = target_profile_dir.join(lib_name);
        if fs::copy(&src, &dest).is_ok() {
            eprintln!("cargo:warning=Copied {} to {}", lib_name, dest.display());
        }
    }
}

fn generate_stub_bindings() {
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    fs::write(
        out_path.join("bindings.rs"),
        include_str!("src/ffi_stub.rs"),
    )
    .expect("Couldn't write stub bindings!");
}
