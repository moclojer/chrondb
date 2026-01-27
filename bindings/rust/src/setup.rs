//! Runtime library setup for ChronDB.
//!
//! Handles automatic download and installation of the native library
//! when it's not available on the system.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;

use crate::error::{ChronDBError, Result};

static SETUP_RESULT: OnceLock<std::result::Result<(), String>> = OnceLock::new();

/// Standard location for ChronDB shared library: ~/.chrondb/lib/
fn chrondb_home_lib_dir() -> Option<PathBuf> {
    dirs::home_dir().map(|h| h.join(".chrondb").join("lib"))
}

fn get_lib_name() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        "libchrondb.dylib"
    }
    #[cfg(target_os = "linux")]
    {
        "libchrondb.so"
    }
    #[cfg(target_os = "windows")]
    {
        "chrondb.dll"
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        "libchrondb.so"
    }
}

fn get_platform() -> Option<&'static str> {
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        Some("linux-x86_64")
    }
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    {
        Some("linux-aarch64")
    }
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    {
        Some("macos-x86_64")
    }
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        Some("macos-aarch64")
    }
    #[cfg(not(any(
        all(target_os = "linux", target_arch = "x86_64"),
        all(target_os = "linux", target_arch = "aarch64"),
        all(target_os = "macos", target_arch = "x86_64"),
        all(target_os = "macos", target_arch = "aarch64"),
    )))]
    {
        None
    }
}

/// Checks if the library is already installed in the expected locations.
fn library_exists() -> bool {
    let lib_name = get_lib_name();

    // Check CHRONDB_LIB_DIR env var
    if let Ok(dir) = env::var("CHRONDB_LIB_DIR") {
        let path = PathBuf::from(dir).join(lib_name);
        if path.exists() {
            return true;
        }
    }

    // Check ~/.chrondb/lib/
    if let Some(home_lib) = chrondb_home_lib_dir() {
        if home_lib.join(lib_name).exists() {
            return true;
        }
    }

    false
}

/// Downloads the ChronDB native library to ~/.chrondb/lib/
fn download_library() -> std::result::Result<(), String> {
    let platform = get_platform()
        .ok_or_else(|| "No pre-built library available for this platform".to_string())?;

    let lib_dir = chrondb_home_lib_dir()
        .ok_or_else(|| "Cannot determine home directory".to_string())?;

    let version = env!("CARGO_PKG_VERSION");
    let (release_tag, version_label) = if version.contains("-dev") {
        ("latest".to_string(), "latest".to_string())
    } else {
        (format!("v{}", version), version.to_string())
    };

    let url = format!(
        "https://github.com/moclojer/chrondb/releases/download/{}/libchrondb-{}-{}.tar.gz",
        release_tag, version_label, platform
    );

    eprintln!("[chrondb] Native library not found, downloading...");
    eprintln!("[chrondb] URL: {}", url);
    eprintln!("[chrondb] Installing to: {}", lib_dir.display());

    let response = ureq::get(&url)
        .call()
        .map_err(|e| format!("Failed to download library: {}", e))?;

    let mut reader = response.into_reader();
    let decoder = flate2::read::GzDecoder::new(&mut reader);
    let mut archive = tar::Archive::new(decoder);

    // Extract to a temp dir first, then move files
    let temp_dir = lib_dir.join(".tmp-extract-runtime");
    fs::create_dir_all(&temp_dir)
        .map_err(|e| format!("Failed to create temp directory: {}", e))?;

    archive
        .unpack(&temp_dir)
        .map_err(|e| format!("Failed to extract archive: {}", e))?;

    // Find the extracted directory and flatten lib/ and include/ into lib_dir
    let entries: Vec<_> = fs::read_dir(&temp_dir)
        .map_err(|e| format!("Failed to read temp directory: {}", e))?
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .collect();

    fs::create_dir_all(&lib_dir)
        .map_err(|e| format!("Failed to create lib directory: {}", e))?;

    if let Some(extracted) = entries.first() {
        let extracted_path = extracted.path();

        // Move lib/* to lib_dir
        let lib_subdir = extracted_path.join("lib");
        if lib_subdir.exists() {
            for entry in fs::read_dir(&lib_subdir)
                .map_err(|e| format!("Failed to read lib subdir: {}", e))?
            {
                let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
                let dest = lib_dir.join(entry.file_name());
                fs::copy(entry.path(), &dest)
                    .map_err(|e| format!("Failed to copy {}: {}", entry.path().display(), e))?;
            }
        }

        // Move include/* to lib_dir (headers)
        let include_subdir = extracted_path.join("include");
        if include_subdir.exists() {
            for entry in fs::read_dir(&include_subdir)
                .map_err(|e| format!("Failed to read include subdir: {}", e))?
            {
                let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
                let dest = lib_dir.join(entry.file_name());
                fs::copy(entry.path(), &dest)
                    .map_err(|e| format!("Failed to copy {}: {}", entry.path().display(), e))?;
            }
        }
    } else {
        return Err("Archive did not contain expected directory structure".to_string());
    }

    // Clean up temp dir
    fs::remove_dir_all(&temp_dir).ok();

    // Verify library was installed
    let lib_name = get_lib_name();
    if !lib_dir.join(lib_name).exists() {
        return Err(format!(
            "Library {} was not found after extraction",
            lib_name
        ));
    }

    eprintln!("[chrondb] Library installed successfully!");
    Ok(())
}

/// Ensures the native library is installed.
///
/// This function is called automatically by `ChronDB::open()` and will:
/// 1. Check if the library exists in expected locations
/// 2. If not found, download it automatically to ~/.chrondb/lib/
///
/// The setup is performed only once per process execution.
pub fn ensure_library_installed() -> Result<()> {
    let result = SETUP_RESULT.get_or_init(|| {
        if library_exists() {
            Ok(())
        } else {
            download_library()
        }
    });

    match result {
        Ok(()) => Ok(()),
        Err(msg) => Err(ChronDBError::SetupFailed(msg.clone())),
    }
}

/// Returns the library installation directory.
///
/// Priority order:
/// 1. `CHRONDB_LIB_DIR` environment variable
/// 2. `~/.chrondb/lib/`
pub fn get_library_dir() -> Option<PathBuf> {
    if let Ok(dir) = env::var("CHRONDB_LIB_DIR") {
        return Some(PathBuf::from(dir));
    }
    chrondb_home_lib_dir()
}

/// Checks if the library exists in a specific directory.
/// This is a testable version that doesn't rely on global state.
#[cfg(test)]
fn library_exists_in_dir(dir: &PathBuf) -> bool {
    let lib_name = get_lib_name();
    dir.join(lib_name).exists()
}

/// Builds the download URL for a given version and platform.
/// Exposed for testing.
#[cfg(test)]
fn build_download_url(version: &str, platform: &str) -> String {
    let (release_tag, version_label) = if version.contains("-dev") {
        ("latest".to_string(), "latest".to_string())
    } else {
        (format!("v{}", version), version.to_string())
    };

    format!(
        "https://github.com/moclojer/chrondb/releases/download/{}/libchrondb-{}-{}.tar.gz",
        release_tag, version_label, platform
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::fs::File;
    use tempfile::TempDir;

    #[test]
    fn test_get_lib_name() {
        let name = get_lib_name();
        assert!(
            name == "libchrondb.dylib" || name == "libchrondb.so" || name == "chrondb.dll"
        );
    }

    #[test]
    fn test_get_lib_name_is_not_empty() {
        let name = get_lib_name();
        assert!(!name.is_empty());
        assert!(name.contains("chrondb"));
    }

    #[test]
    fn test_get_platform() {
        let platform = get_platform();
        if cfg!(any(
            all(target_os = "linux", target_arch = "x86_64"),
            all(target_os = "linux", target_arch = "aarch64"),
            all(target_os = "macos", target_arch = "x86_64"),
            all(target_os = "macos", target_arch = "aarch64"),
        )) {
            assert!(platform.is_some());
            let p = platform.unwrap();
            assert!(
                p == "linux-x86_64"
                    || p == "linux-aarch64"
                    || p == "macos-x86_64"
                    || p == "macos-aarch64"
            );
        }
    }

    #[test]
    fn test_chrondb_home_lib_dir_returns_path() {
        let dir = chrondb_home_lib_dir();
        // Should return Some on systems with a home directory
        if dirs::home_dir().is_some() {
            assert!(dir.is_some());
            let path = dir.unwrap();
            assert!(path.ends_with(".chrondb/lib") || path.ends_with(".chrondb\\lib"));
        }
    }

    #[test]
    #[serial]
    fn test_get_library_dir_with_env_var() {
        let test_dir = "/tmp/test-chrondb-lib";
        env::set_var("CHRONDB_LIB_DIR", test_dir);

        let result = get_library_dir();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), PathBuf::from(test_dir));

        env::remove_var("CHRONDB_LIB_DIR");
    }

    #[test]
    #[serial]
    fn test_get_library_dir_without_env_var() {
        // Save and clear env var
        let saved = env::var("CHRONDB_LIB_DIR").ok();
        env::remove_var("CHRONDB_LIB_DIR");

        let result = get_library_dir();
        // Should fall back to ~/.chrondb/lib/
        if dirs::home_dir().is_some() {
            assert!(result.is_some());
            let path = result.unwrap();
            // The path should end with .chrondb/lib or contain .chrondb
            assert!(
                path.to_string_lossy().contains(".chrondb"),
                "Path should contain .chrondb: {:?}",
                path
            );
        }

        // Restore env var if it was set
        if let Some(val) = saved {
            env::set_var("CHRONDB_LIB_DIR", val);
        }
    }

    #[test]
    fn test_library_exists_in_dir_when_missing() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        assert!(!library_exists_in_dir(&path));
    }

    #[test]
    fn test_library_exists_in_dir_when_present() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a fake library file
        let lib_name = get_lib_name();
        let lib_path = path.join(lib_name);
        File::create(&lib_path).unwrap();

        assert!(library_exists_in_dir(&path));
    }

    #[test]
    #[serial]
    fn test_library_exists_with_env_var() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a fake library file
        let lib_name = get_lib_name();
        let lib_path = path.join(lib_name);
        File::create(&lib_path).unwrap();

        // Set env var to point to temp dir
        env::set_var("CHRONDB_LIB_DIR", path.to_str().unwrap());

        assert!(library_exists());

        env::remove_var("CHRONDB_LIB_DIR");
    }

    #[test]
    fn test_library_exists_returns_false_in_empty_dir() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Use testable function instead of global one
        // (which may find lib in ~/.chrondb/lib/)
        assert!(!library_exists_in_dir(&path));
    }

    #[test]
    fn test_build_download_url_release_version() {
        let url = build_download_url("0.1.0", "linux-x86_64");
        assert_eq!(
            url,
            "https://github.com/moclojer/chrondb/releases/download/v0.1.0/libchrondb-0.1.0-linux-x86_64.tar.gz"
        );
    }

    #[test]
    fn test_build_download_url_dev_version() {
        let url = build_download_url("0.1.0-dev", "macos-aarch64");
        assert_eq!(
            url,
            "https://github.com/moclojer/chrondb/releases/download/latest/libchrondb-latest-macos-aarch64.tar.gz"
        );
    }

    #[test]
    fn test_build_download_url_contains_platform() {
        let platforms = ["linux-x86_64", "linux-aarch64", "macos-x86_64", "macos-aarch64"];

        for platform in platforms {
            let url = build_download_url("1.0.0", platform);
            assert!(url.contains(platform), "URL should contain platform: {}", platform);
        }
    }
}
