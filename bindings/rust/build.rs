//! Build script for ChronDB Rust bindings.
//!
//! This script optionally pre-downloads the native library during compilation.
//! The library will also be downloaded at runtime if not found, so this step
//! is not strictly necessary but can speed up the first run.

use std::env;
use std::fs;
use std::path::PathBuf;

/// Standard location for ChronDB shared library: ~/.chrondb/lib/
fn chrondb_home_lib_dir() -> Option<PathBuf> {
    dirs::home_dir().map(|h| h.join(".chrondb").join("lib"))
}

fn get_lib_name(target_os: &str) -> &'static str {
    match target_os {
        "macos" => "libchrondb.dylib",
        "linux" => "libchrondb.so",
        "windows" => "chrondb.dll",
        _ => "libchrondb.so",
    }
}

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
                "cargo:warning=No pre-built library for {}-{}. Will download at runtime.",
                target_os, target_arch
            );
            return Ok(());
        }
    };

    let (release_tag, version_label) = if pkg_version.contains("-dev") {
        ("latest".to_string(), "latest".to_string())
    } else {
        (format!("v{}", pkg_version), pkg_version.clone())
    };

    let url = format!(
        "https://github.com/avelino/chrondb/releases/download/{}/libchrondb-{}-{}.tar.gz",
        release_tag, version_label, platform
    );

    eprintln!("cargo:warning=Pre-downloading ChronDB library from {}", url);
    eprintln!("cargo:warning=Installing to {}", lib_dir.display());

    let response = ureq::get(&url).call()?;
    let mut reader = response.into_reader();

    let decoder = flate2::read::GzDecoder::new(&mut reader);
    let mut archive = tar::Archive::new(decoder);

    let temp_dir = lib_dir.join(".tmp-extract-build");
    fs::create_dir_all(&temp_dir)?;
    archive.unpack(&temp_dir)?;

    let entries: Vec<_> = fs::read_dir(&temp_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .collect();

    fs::create_dir_all(lib_dir)?;

    if let Some(extracted) = entries.first() {
        let extracted_path = extracted.path();

        // Move lib/* to lib_dir
        let lib_subdir = extracted_path.join("lib");
        if lib_subdir.exists() {
            for entry in fs::read_dir(&lib_subdir)? {
                let entry = entry?;
                let dest = lib_dir.join(entry.file_name());
                fs::copy(entry.path(), &dest)?;
            }
        }

        // Move include/* to lib_dir (headers)
        let include_subdir = extracted_path.join("include");
        if include_subdir.exists() {
            for entry in fs::read_dir(&include_subdir)? {
                let entry = entry?;
                let dest = lib_dir.join(entry.file_name());
                fs::copy(entry.path(), &dest)?;
            }
        }
    }

    fs::remove_dir_all(&temp_dir).ok();
    Ok(())
}

fn main() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let lib_name = get_lib_name(&target_os);

    println!("cargo:rerun-if-env-changed=CHRONDB_LIB_DIR");
    println!("cargo:rerun-if-env-changed=CHRONDB_SKIP_DOWNLOAD");

    // Skip download if explicitly requested
    if env::var("CHRONDB_SKIP_DOWNLOAD").is_ok() {
        eprintln!("cargo:warning=Skipping library download (CHRONDB_SKIP_DOWNLOAD set)");
        return;
    }

    // Check if library already exists
    let lib_dir = if let Ok(dir) = env::var("CHRONDB_LIB_DIR") {
        PathBuf::from(dir)
    } else if let Some(home_lib) = chrondb_home_lib_dir() {
        home_lib
    } else {
        eprintln!("cargo:warning=Cannot determine library location, will download at runtime");
        return;
    };

    if lib_dir.join(lib_name).exists() {
        eprintln!(
            "cargo:warning=ChronDB library already exists at {}",
            lib_dir.display()
        );
        return;
    }

    // Try to pre-download the library
    if let Err(e) = download_library(&lib_dir) {
        eprintln!(
            "cargo:warning=Failed to pre-download library: {}. Will download at runtime.",
            e
        );
    } else {
        eprintln!("cargo:warning=ChronDB library pre-downloaded successfully");
    }
}
