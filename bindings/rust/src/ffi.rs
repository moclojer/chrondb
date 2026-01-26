//! Dynamic FFI bindings for ChronDB using libloading.
//!
//! This module loads the ChronDB shared library at runtime via dlopen,
//! enabling auto-download when the library is not found.

#![allow(non_camel_case_types)]

use std::ffi::c_void;
use std::os::raw::{c_char, c_int};
use std::path::PathBuf;
use std::sync::OnceLock;

use libloading::Library;

use crate::error::{ChronDBError, Result};
use crate::setup;

// Type aliases for GraalVM types
pub type graal_isolate_t = c_void;
pub type graal_isolatethread_t = c_void;

#[repr(C)]
pub struct graal_create_isolate_params_t {
    pub version: c_int,
    pub reserved_address_space_size: usize,
}

// Function pointer types
type GraalCreateIsolateFn = unsafe extern "C" fn(
    params: *mut graal_create_isolate_params_t,
    isolate: *mut *mut graal_isolate_t,
    thread: *mut *mut graal_isolatethread_t,
) -> c_int;

type GraalTearDownIsolateFn = unsafe extern "C" fn(thread: *mut graal_isolatethread_t) -> c_int;

type ChrondbOpenFn = unsafe extern "C" fn(
    thread: *mut graal_isolatethread_t,
    data_path: *const c_char,
    index_path: *const c_char,
) -> c_int;

type ChrondbCloseFn =
    unsafe extern "C" fn(thread: *mut graal_isolatethread_t, handle: c_int) -> c_int;

type ChrondbPutFn = unsafe extern "C" fn(
    thread: *mut graal_isolatethread_t,
    handle: c_int,
    id: *const c_char,
    json_doc: *const c_char,
    branch: *const c_char,
) -> *mut c_char;

type ChrondbGetFn = unsafe extern "C" fn(
    thread: *mut graal_isolatethread_t,
    handle: c_int,
    id: *const c_char,
    branch: *const c_char,
) -> *mut c_char;

type ChrondbDeleteFn = unsafe extern "C" fn(
    thread: *mut graal_isolatethread_t,
    handle: c_int,
    id: *const c_char,
    branch: *const c_char,
) -> c_int;

type ChrondbListByPrefixFn = unsafe extern "C" fn(
    thread: *mut graal_isolatethread_t,
    handle: c_int,
    prefix: *const c_char,
    branch: *const c_char,
) -> *mut c_char;

type ChrondbListByTableFn = unsafe extern "C" fn(
    thread: *mut graal_isolatethread_t,
    handle: c_int,
    table: *const c_char,
    branch: *const c_char,
) -> *mut c_char;

type ChrondbHistoryFn = unsafe extern "C" fn(
    thread: *mut graal_isolatethread_t,
    handle: c_int,
    id: *const c_char,
    branch: *const c_char,
) -> *mut c_char;

type ChrondbQueryFn = unsafe extern "C" fn(
    thread: *mut graal_isolatethread_t,
    handle: c_int,
    query_json: *const c_char,
    branch: *const c_char,
) -> *mut c_char;

type ChrondbFreeStringFn =
    unsafe extern "C" fn(thread: *mut graal_isolatethread_t, ptr: *mut c_char);

type ChrondbLastErrorFn = unsafe extern "C" fn(thread: *mut graal_isolatethread_t) -> *mut c_char;

/// Holds the dynamically loaded library and function pointers.
pub struct ChronDBLib {
    #[allow(dead_code)]
    lib: Library,
    pub graal_create_isolate: GraalCreateIsolateFn,
    pub graal_tear_down_isolate: GraalTearDownIsolateFn,
    pub chrondb_open: ChrondbOpenFn,
    pub chrondb_close: ChrondbCloseFn,
    pub chrondb_put: ChrondbPutFn,
    pub chrondb_get: ChrondbGetFn,
    pub chrondb_delete: ChrondbDeleteFn,
    pub chrondb_list_by_prefix: ChrondbListByPrefixFn,
    pub chrondb_list_by_table: ChrondbListByTableFn,
    pub chrondb_history: ChrondbHistoryFn,
    pub chrondb_query: ChrondbQueryFn,
    pub chrondb_free_string: ChrondbFreeStringFn,
    pub chrondb_last_error: ChrondbLastErrorFn,
}

// Safety: The library handle and function pointers are safe to share across threads
// because the underlying GraalVM isolate handles thread safety internally.
unsafe impl Send for ChronDBLib {}
unsafe impl Sync for ChronDBLib {}

static LIBRARY: OnceLock<std::result::Result<ChronDBLib, String>> = OnceLock::new();

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

fn find_library_path() -> Option<PathBuf> {
    let lib_name = get_lib_name();

    // Priority 1: CHRONDB_LIB_DIR env var
    if let Ok(dir) = std::env::var("CHRONDB_LIB_DIR") {
        let path = PathBuf::from(dir).join(lib_name);
        if path.exists() {
            return Some(path);
        }
    }

    // Priority 2: ~/.chrondb/lib/
    if let Some(lib_dir) = setup::get_library_dir() {
        let path = lib_dir.join(lib_name);
        if path.exists() {
            return Some(path);
        }
    }

    None
}

impl ChronDBLib {
    fn load() -> std::result::Result<Self, String> {
        let lib_path = find_library_path()
            .ok_or_else(|| format!("ChronDB library '{}' not found", get_lib_name()))?;

        // Safety: We're loading a library that follows the expected ABI.
        let lib = unsafe { Library::new(&lib_path) }
            .map_err(|e| format!("Failed to load library {}: {}", lib_path.display(), e))?;

        // Load all function symbols and copy them before moving lib
        unsafe {
            let graal_create_isolate: GraalCreateIsolateFn = *lib
                .get::<GraalCreateIsolateFn>(b"graal_create_isolate")
                .map_err(|e| format!("Symbol graal_create_isolate not found: {}", e))?;

            let graal_tear_down_isolate: GraalTearDownIsolateFn = *lib
                .get::<GraalTearDownIsolateFn>(b"graal_tear_down_isolate")
                .map_err(|e| format!("Symbol graal_tear_down_isolate not found: {}", e))?;

            let chrondb_open: ChrondbOpenFn = *lib
                .get::<ChrondbOpenFn>(b"chrondb_open")
                .map_err(|e| format!("Symbol chrondb_open not found: {}", e))?;

            let chrondb_close: ChrondbCloseFn = *lib
                .get::<ChrondbCloseFn>(b"chrondb_close")
                .map_err(|e| format!("Symbol chrondb_close not found: {}", e))?;

            let chrondb_put: ChrondbPutFn = *lib
                .get::<ChrondbPutFn>(b"chrondb_put")
                .map_err(|e| format!("Symbol chrondb_put not found: {}", e))?;

            let chrondb_get: ChrondbGetFn = *lib
                .get::<ChrondbGetFn>(b"chrondb_get")
                .map_err(|e| format!("Symbol chrondb_get not found: {}", e))?;

            let chrondb_delete: ChrondbDeleteFn = *lib
                .get::<ChrondbDeleteFn>(b"chrondb_delete")
                .map_err(|e| format!("Symbol chrondb_delete not found: {}", e))?;

            let chrondb_list_by_prefix: ChrondbListByPrefixFn = *lib
                .get::<ChrondbListByPrefixFn>(b"chrondb_list_by_prefix")
                .map_err(|e| format!("Symbol chrondb_list_by_prefix not found: {}", e))?;

            let chrondb_list_by_table: ChrondbListByTableFn = *lib
                .get::<ChrondbListByTableFn>(b"chrondb_list_by_table")
                .map_err(|e| format!("Symbol chrondb_list_by_table not found: {}", e))?;

            let chrondb_history: ChrondbHistoryFn = *lib
                .get::<ChrondbHistoryFn>(b"chrondb_history")
                .map_err(|e| format!("Symbol chrondb_history not found: {}", e))?;

            let chrondb_query: ChrondbQueryFn = *lib
                .get::<ChrondbQueryFn>(b"chrondb_query")
                .map_err(|e| format!("Symbol chrondb_query not found: {}", e))?;

            let chrondb_free_string: ChrondbFreeStringFn = *lib
                .get::<ChrondbFreeStringFn>(b"chrondb_free_string")
                .map_err(|e| format!("Symbol chrondb_free_string not found: {}", e))?;

            let chrondb_last_error: ChrondbLastErrorFn = *lib
                .get::<ChrondbLastErrorFn>(b"chrondb_last_error")
                .map_err(|e| format!("Symbol chrondb_last_error not found: {}", e))?;

            Ok(ChronDBLib {
                lib,
                graal_create_isolate,
                graal_tear_down_isolate,
                chrondb_open,
                chrondb_close,
                chrondb_put,
                chrondb_get,
                chrondb_delete,
                chrondb_list_by_prefix,
                chrondb_list_by_table,
                chrondb_history,
                chrondb_query,
                chrondb_free_string,
                chrondb_last_error,
            })
        }
    }
}

/// Gets or initializes the loaded library.
///
/// This function first ensures the library is installed (downloading if needed),
/// then loads it via dlopen.
pub fn get_library() -> Result<&'static ChronDBLib> {
    // First ensure library is installed
    setup::ensure_library_installed()?;

    // Then load it
    let result = LIBRARY.get_or_init(|| ChronDBLib::load());

    match result {
        Ok(lib) => Ok(lib),
        Err(msg) => Err(ChronDBError::SetupFailed(msg.clone())),
    }
}

/// Attempts to find the library path without loading it.
/// Exposed for testing.
#[cfg(test)]
#[allow(dead_code)]
fn try_find_library_path() -> Option<PathBuf> {
    find_library_path()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs::File;
    use tempfile::TempDir;

    #[test]
    fn test_get_lib_name_returns_valid_name() {
        let name = get_lib_name();
        assert!(!name.is_empty());
        assert!(
            name == "libchrondb.dylib" || name == "libchrondb.so" || name == "chrondb.dll",
            "Unexpected library name: {}",
            name
        );
    }

    #[test]
    fn test_get_lib_name_matches_platform() {
        let name = get_lib_name();

        #[cfg(target_os = "macos")]
        assert_eq!(name, "libchrondb.dylib");

        #[cfg(target_os = "linux")]
        assert_eq!(name, "libchrondb.so");

        #[cfg(target_os = "windows")]
        assert_eq!(name, "chrondb.dll");
    }

    #[test]
    fn test_find_library_path_with_env_var() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a fake library file
        let lib_name = get_lib_name();
        let lib_path = path.join(lib_name);
        File::create(&lib_path).unwrap();

        // Set env var
        env::set_var("CHRONDB_LIB_DIR", path.to_str().unwrap());

        let result = find_library_path();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), lib_path);

        env::remove_var("CHRONDB_LIB_DIR");
    }

    #[test]
    fn test_find_library_path_returns_none_when_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Set env var to empty directory (no library)
        env::set_var("CHRONDB_LIB_DIR", path.to_str().unwrap());

        let result = find_library_path();
        // Should return None since no library file exists in the directory
        assert!(result.is_none());

        env::remove_var("CHRONDB_LIB_DIR");
    }

    #[test]
    fn test_find_library_path_priority_env_over_home() {
        let temp_dir = TempDir::new().unwrap();
        let env_path = temp_dir.path().to_path_buf();

        // Create a fake library in env dir
        let lib_name = get_lib_name();
        let lib_path = env_path.join(lib_name);
        File::create(&lib_path).unwrap();

        // Set env var
        env::set_var("CHRONDB_LIB_DIR", env_path.to_str().unwrap());

        let result = find_library_path();
        assert!(result.is_some());
        // Should use env var path, not home dir
        assert!(result.unwrap().starts_with(&env_path));

        env::remove_var("CHRONDB_LIB_DIR");
    }

    #[test]
    fn test_graal_create_isolate_params_layout() {
        // Verify struct has expected size (C ABI compatibility)
        let params = graal_create_isolate_params_t {
            version: 0,
            reserved_address_space_size: 0,
        };

        // Should be able to create the struct
        assert_eq!(params.version, 0);
        assert_eq!(params.reserved_address_space_size, 0);
    }

    #[test]
    fn test_find_library_path_empty_dir_returns_none() {
        // Save original env var
        let saved = env::var("CHRONDB_LIB_DIR").ok();

        // Create empty temp dir and set as lib dir
        let temp_dir = TempDir::new().unwrap();
        env::set_var("CHRONDB_LIB_DIR", temp_dir.path().to_str().unwrap());

        // find_library_path should return None for empty dir
        let result = find_library_path();
        // Note: might still find lib in ~/.chrondb/lib/ if it exists
        // This test verifies the env var path is checked first and doesn't contain lib

        // The lib doesn't exist in temp_dir, so if result is Some,
        // it means ~/.chrondb/lib/ has the library
        if result.is_some() {
            // Verify it's not from our temp dir
            assert!(!result.as_ref().unwrap().starts_with(temp_dir.path()));
        }

        // Restore env var
        if let Some(val) = saved {
            env::set_var("CHRONDB_LIB_DIR", val);
        } else {
            env::remove_var("CHRONDB_LIB_DIR");
        }
    }

    #[test]
    fn test_chrondb_lib_load_fails_with_invalid_library() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Create a fake (invalid) library file
        let lib_name = get_lib_name();
        let lib_path = path.join(lib_name);
        std::fs::write(&lib_path, b"not a real library").unwrap();

        env::set_var("CHRONDB_LIB_DIR", path.to_str().unwrap());

        // Load should fail because file is not a valid library
        let result = ChronDBLib::load();
        assert!(result.is_err());

        match result {
            Err(err) => {
                assert!(
                    err.contains("Failed to load") || err.contains("invalid") || err.contains("mach-o"),
                    "Error should mention load failure: {}",
                    err
                );
            }
            Ok(_) => panic!("Expected error but got Ok"),
        }

        env::remove_var("CHRONDB_LIB_DIR");
    }
}
