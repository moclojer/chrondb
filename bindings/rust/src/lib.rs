//! Rust bindings for ChronDB - a time-traveling key/value database.
//!
//! This crate provides safe Rust wrappers around the ChronDB shared library
//! built with GraalVM native-image.
//!
//! # Example
//!
//! ```no_run
//! use chrondb::ChronDB;
//! use serde_json::json;
//!
//! let db = ChronDB::open("/tmp/chrondb-data", "/tmp/chrondb-index").unwrap();
//! db.put("user:1", &json!({"name": "Alice", "age": 30}), None).unwrap();
//! let doc = db.get("user:1", None).unwrap();
//! println!("{}", doc);
//! ```

mod error;
mod ffi;
mod setup;

pub use error::{ChronDBError, Result};
pub use setup::{ensure_library_installed, get_library_dir};

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

use ffi::graal_isolate_t;
use ffi::graal_isolatethread_t;

/// Stack size for the FFI worker thread (64 MB).
/// GraalVM native-image with Lucene/JGit requires large stack for deep call chains.
const FFI_THREAD_STACK_SIZE: usize = 64 * 1024 * 1024;

/// Commands sent to the FFI worker thread.
enum FfiCommand {
    Put {
        id: String,
        doc: String,
        branch: Option<String>,
        reply: Sender<Result<serde_json::Value>>,
    },
    Get {
        id: String,
        branch: Option<String>,
        reply: Sender<Result<serde_json::Value>>,
    },
    Delete {
        id: String,
        branch: Option<String>,
        reply: Sender<Result<()>>,
    },
    ListByPrefix {
        prefix: String,
        branch: Option<String>,
        reply: Sender<Result<serde_json::Value>>,
    },
    ListByTable {
        table: String,
        branch: Option<String>,
        reply: Sender<Result<serde_json::Value>>,
    },
    History {
        id: String,
        branch: Option<String>,
        reply: Sender<Result<serde_json::Value>>,
    },
    Query {
        query: String,
        branch: Option<String>,
        reply: Sender<Result<serde_json::Value>>,
    },
    LastError {
        reply: Sender<Option<String>>,
    },
    Shutdown,
}

/// Internal state held by the FFI worker thread.
struct FfiWorkerState {
    lib: &'static ffi::ChronDBLib,
    isolate: *mut graal_isolate_t,
    thread: *mut graal_isolatethread_t,
    handle: i32,
}

impl FfiWorkerState {
    fn get_last_error(&self) -> Option<String> {
        let ptr = unsafe { (self.lib.chrondb_last_error)(self.thread) };
        if ptr.is_null() {
            None
        } else {
            let s = unsafe { CStr::from_ptr(ptr) }
                .to_string_lossy()
                .into_owned();
            unsafe { (self.lib.chrondb_free_string)(self.thread, ptr) };
            Some(s)
        }
    }

    fn last_error_or(&self, default: &str) -> ChronDBError {
        let msg = self.get_last_error().unwrap_or_else(|| default.to_string());
        ChronDBError::OperationFailed(msg)
    }

    fn optional_cstring(s: Option<&str>) -> Result<Option<CString>> {
        match s {
            Some(v) => Ok(Some(
                CString::new(v).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?,
            )),
            None => Ok(None),
        }
    }

    fn ptr_or_null(opt: &Option<CString>) -> *mut c_char {
        match opt {
            Some(cs) => cs.as_ptr() as *mut c_char,
            None => ptr::null_mut(),
        }
    }

    fn parse_string_result(&self, ptr: *mut c_char) -> Result<serde_json::Value> {
        if ptr.is_null() {
            return Err(self.last_error_or("null result"));
        }
        let s = unsafe { CStr::from_ptr(ptr) }
            .to_string_lossy()
            .into_owned();
        unsafe { (self.lib.chrondb_free_string)(self.thread, ptr) };
        let val: serde_json::Value = serde_json::from_str(&s)?;
        Ok(val)
    }

    fn handle_put(&self, id: &str, doc: &str, branch: Option<&str>) -> Result<serde_json::Value> {
        let c_id = CString::new(id).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let c_json = CString::new(doc).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let c_branch = Self::optional_cstring(branch)?;

        let result = unsafe {
            (self.lib.chrondb_put)(
                self.thread,
                self.handle,
                c_id.as_ptr() as *mut c_char,
                c_json.as_ptr() as *mut c_char,
                Self::ptr_or_null(&c_branch),
            )
        };

        self.parse_string_result(result)
    }

    fn handle_get(&self, id: &str, branch: Option<&str>) -> Result<serde_json::Value> {
        let c_id = CString::new(id).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let c_branch = Self::optional_cstring(branch)?;

        let result = unsafe {
            (self.lib.chrondb_get)(
                self.thread,
                self.handle,
                c_id.as_ptr() as *mut c_char,
                Self::ptr_or_null(&c_branch),
            )
        };

        if result.is_null() {
            return Err(ChronDBError::NotFound);
        }
        self.parse_string_result(result)
    }

    fn handle_delete(&self, id: &str, branch: Option<&str>) -> Result<()> {
        let c_id = CString::new(id).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let c_branch = Self::optional_cstring(branch)?;

        let ret = unsafe {
            (self.lib.chrondb_delete)(
                self.thread,
                self.handle,
                c_id.as_ptr() as *mut c_char,
                Self::ptr_or_null(&c_branch),
            )
        };

        match ret {
            0 => Ok(()),
            1 => Err(ChronDBError::NotFound),
            _ => Err(self.last_error_or("delete failed")),
        }
    }

    fn handle_list_by_prefix(
        &self,
        prefix: &str,
        branch: Option<&str>,
    ) -> Result<serde_json::Value> {
        let c_prefix =
            CString::new(prefix).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let c_branch = Self::optional_cstring(branch)?;

        let result = unsafe {
            (self.lib.chrondb_list_by_prefix)(
                self.thread,
                self.handle,
                c_prefix.as_ptr() as *mut c_char,
                Self::ptr_or_null(&c_branch),
            )
        };

        if result.is_null() {
            return Ok(serde_json::Value::Array(vec![]));
        }
        self.parse_string_result(result)
    }

    fn handle_list_by_table(&self, table: &str, branch: Option<&str>) -> Result<serde_json::Value> {
        let c_table =
            CString::new(table).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let c_branch = Self::optional_cstring(branch)?;

        let result = unsafe {
            (self.lib.chrondb_list_by_table)(
                self.thread,
                self.handle,
                c_table.as_ptr() as *mut c_char,
                Self::ptr_or_null(&c_branch),
            )
        };

        if result.is_null() {
            return Ok(serde_json::Value::Array(vec![]));
        }
        self.parse_string_result(result)
    }

    fn handle_history(&self, id: &str, branch: Option<&str>) -> Result<serde_json::Value> {
        let c_id = CString::new(id).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let c_branch = Self::optional_cstring(branch)?;

        let result = unsafe {
            (self.lib.chrondb_history)(
                self.thread,
                self.handle,
                c_id.as_ptr() as *mut c_char,
                Self::ptr_or_null(&c_branch),
            )
        };

        if result.is_null() {
            return Ok(serde_json::Value::Array(vec![]));
        }
        self.parse_string_result(result)
    }

    fn handle_query(&self, query: &str, branch: Option<&str>) -> Result<serde_json::Value> {
        let c_query =
            CString::new(query).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let c_branch = Self::optional_cstring(branch)?;

        let result = unsafe {
            (self.lib.chrondb_query)(
                self.thread,
                self.handle,
                c_query.as_ptr() as *mut c_char,
                Self::ptr_or_null(&c_branch),
            )
        };

        if result.is_null() {
            return Err(self.last_error_or("query failed"));
        }
        self.parse_string_result(result)
    }

    fn close(&mut self) {
        if self.handle >= 0 {
            unsafe {
                (self.lib.chrondb_close)(self.thread, self.handle);
            }
            self.handle = -1;
        }
        if !self.thread.is_null() {
            unsafe {
                (self.lib.graal_tear_down_isolate)(self.thread);
            }
            self.thread = ptr::null_mut();
            self.isolate = ptr::null_mut();
        }
    }
}

/// A connection to a ChronDB database instance.
///
/// All FFI calls are executed in a dedicated thread with a large stack (64MB)
/// to accommodate GraalVM's stack requirements for Lucene and JGit operations.
///
/// The database is automatically closed when this struct is dropped.
pub struct ChronDB {
    sender: Sender<FfiCommand>,
    worker: Option<JoinHandle<()>>,
}

// ChronDB is safe to send across threads because communication
// happens via channels and the FFI worker manages the isolate.
unsafe impl Send for ChronDB {}

impl ChronDB {
    /// Opens a ChronDB database at the given paths.
    ///
    /// If the native library is not installed, this function will automatically
    /// download and install it to `~/.chrondb/lib/`.
    ///
    /// # Arguments
    /// * `data_path` - Path for the Git repository (data storage)
    /// * `index_path` - Path for the Lucene index
    pub fn open(data_path: &str, index_path: &str) -> Result<Self> {
        let (tx, rx): (Sender<FfiCommand>, Receiver<FfiCommand>) = mpsc::channel();

        let data_path = data_path.to_string();
        let index_path = index_path.to_string();

        // Channel to receive initialization result from worker
        let (init_tx, init_rx) = mpsc::channel::<Result<()>>();

        let worker = thread::Builder::new()
            .name("chrondb-ffi-worker".to_string())
            .stack_size(FFI_THREAD_STACK_SIZE)
            .spawn(move || {
                // Initialize in the worker thread (which has large stack)
                let init_result = Self::init_worker(&data_path, &index_path);

                match init_result {
                    Ok(mut state) => {
                        let _ = init_tx.send(Ok(()));
                        Self::run_worker_loop(&mut state, rx);
                        state.close();
                    }
                    Err(e) => {
                        let _ = init_tx.send(Err(e));
                    }
                }
            })
            .map_err(|_| ChronDBError::IsolateCreationFailed)?;

        // Wait for initialization result
        init_rx
            .recv()
            .map_err(|_| ChronDBError::IsolateCreationFailed)??;

        Ok(ChronDB {
            sender: tx,
            worker: Some(worker),
        })
    }

    fn init_worker(data_path: &str, index_path: &str) -> Result<FfiWorkerState> {
        let lib = ffi::get_library()?;

        let mut isolate: *mut graal_isolate_t = ptr::null_mut();
        let mut thread: *mut graal_isolatethread_t = ptr::null_mut();

        let ret = unsafe { (lib.graal_create_isolate)(ptr::null_mut(), &mut isolate, &mut thread) };
        if ret != 0 {
            return Err(ChronDBError::IsolateCreationFailed);
        }

        let c_data =
            CString::new(data_path).map_err(|e| ChronDBError::OpenFailed(e.to_string()))?;
        let c_index =
            CString::new(index_path).map_err(|e| ChronDBError::OpenFailed(e.to_string()))?;

        let handle = unsafe {
            (lib.chrondb_open)(
                thread,
                c_data.as_ptr() as *mut c_char,
                c_index.as_ptr() as *mut c_char,
            )
        };

        if handle < 0 {
            let err_ptr = unsafe { (lib.chrondb_last_error)(thread) };
            let err = if err_ptr.is_null() {
                String::new()
            } else {
                let s = unsafe { CStr::from_ptr(err_ptr) }
                    .to_string_lossy()
                    .into_owned();
                unsafe { (lib.chrondb_free_string)(thread, err_ptr) };
                s
            };
            unsafe { (lib.graal_tear_down_isolate)(thread) };
            return Err(ChronDBError::OpenFailed(err));
        }

        Ok(FfiWorkerState {
            lib,
            isolate,
            thread,
            handle,
        })
    }

    fn run_worker_loop(state: &mut FfiWorkerState, rx: Receiver<FfiCommand>) {
        while let Ok(cmd) = rx.recv() {
            match cmd {
                FfiCommand::Put {
                    id,
                    doc,
                    branch,
                    reply,
                } => {
                    let result = state.handle_put(&id, &doc, branch.as_deref());
                    let _ = reply.send(result);
                }
                FfiCommand::Get { id, branch, reply } => {
                    let result = state.handle_get(&id, branch.as_deref());
                    let _ = reply.send(result);
                }
                FfiCommand::Delete { id, branch, reply } => {
                    let result = state.handle_delete(&id, branch.as_deref());
                    let _ = reply.send(result);
                }
                FfiCommand::ListByPrefix {
                    prefix,
                    branch,
                    reply,
                } => {
                    let result = state.handle_list_by_prefix(&prefix, branch.as_deref());
                    let _ = reply.send(result);
                }
                FfiCommand::ListByTable {
                    table,
                    branch,
                    reply,
                } => {
                    let result = state.handle_list_by_table(&table, branch.as_deref());
                    let _ = reply.send(result);
                }
                FfiCommand::History { id, branch, reply } => {
                    let result = state.handle_history(&id, branch.as_deref());
                    let _ = reply.send(result);
                }
                FfiCommand::Query {
                    query,
                    branch,
                    reply,
                } => {
                    let result = state.handle_query(&query, branch.as_deref());
                    let _ = reply.send(result);
                }
                FfiCommand::LastError { reply } => {
                    let _ = reply.send(state.get_last_error());
                }
                FfiCommand::Shutdown => break,
            }
        }
    }

    /// Saves a document with the given ID.
    ///
    /// Returns the saved document as a JSON value.
    pub fn put(
        &self,
        id: &str,
        doc: &serde_json::Value,
        branch: Option<&str>,
    ) -> Result<serde_json::Value> {
        let json_str = serde_json::to_string(doc)?;
        let (reply_tx, reply_rx) = mpsc::channel();

        self.sender
            .send(FfiCommand::Put {
                id: id.to_string(),
                doc: json_str,
                branch: branch.map(|s| s.to_string()),
                reply: reply_tx,
            })
            .map_err(|_| ChronDBError::OperationFailed("worker thread died".to_string()))?;

        reply_rx
            .recv()
            .map_err(|_| ChronDBError::OperationFailed("worker thread died".to_string()))?
    }

    /// Gets a document by ID.
    ///
    /// Returns `Err(NotFound)` if the document does not exist.
    pub fn get(&self, id: &str, branch: Option<&str>) -> Result<serde_json::Value> {
        let (reply_tx, reply_rx) = mpsc::channel();

        self.sender
            .send(FfiCommand::Get {
                id: id.to_string(),
                branch: branch.map(|s| s.to_string()),
                reply: reply_tx,
            })
            .map_err(|_| ChronDBError::OperationFailed("worker thread died".to_string()))?;

        reply_rx
            .recv()
            .map_err(|_| ChronDBError::OperationFailed("worker thread died".to_string()))?
    }

    /// Deletes a document by ID.
    ///
    /// Returns `Ok(())` on success, `Err(NotFound)` if the document doesn't exist.
    pub fn delete(&self, id: &str, branch: Option<&str>) -> Result<()> {
        let (reply_tx, reply_rx) = mpsc::channel();

        self.sender
            .send(FfiCommand::Delete {
                id: id.to_string(),
                branch: branch.map(|s| s.to_string()),
                reply: reply_tx,
            })
            .map_err(|_| ChronDBError::OperationFailed("worker thread died".to_string()))?;

        reply_rx
            .recv()
            .map_err(|_| ChronDBError::OperationFailed("worker thread died".to_string()))?
    }

    /// Lists documents by ID prefix.
    pub fn list_by_prefix(&self, prefix: &str, branch: Option<&str>) -> Result<serde_json::Value> {
        let (reply_tx, reply_rx) = mpsc::channel();

        self.sender
            .send(FfiCommand::ListByPrefix {
                prefix: prefix.to_string(),
                branch: branch.map(|s| s.to_string()),
                reply: reply_tx,
            })
            .map_err(|_| ChronDBError::OperationFailed("worker thread died".to_string()))?;

        reply_rx
            .recv()
            .map_err(|_| ChronDBError::OperationFailed("worker thread died".to_string()))?
    }

    /// Lists documents by table name.
    pub fn list_by_table(&self, table: &str, branch: Option<&str>) -> Result<serde_json::Value> {
        let (reply_tx, reply_rx) = mpsc::channel();

        self.sender
            .send(FfiCommand::ListByTable {
                table: table.to_string(),
                branch: branch.map(|s| s.to_string()),
                reply: reply_tx,
            })
            .map_err(|_| ChronDBError::OperationFailed("worker thread died".to_string()))?;

        reply_rx
            .recv()
            .map_err(|_| ChronDBError::OperationFailed("worker thread died".to_string()))?
    }

    /// Gets the history of changes for a document.
    pub fn history(&self, id: &str, branch: Option<&str>) -> Result<serde_json::Value> {
        let (reply_tx, reply_rx) = mpsc::channel();

        self.sender
            .send(FfiCommand::History {
                id: id.to_string(),
                branch: branch.map(|s| s.to_string()),
                reply: reply_tx,
            })
            .map_err(|_| ChronDBError::OperationFailed("worker thread died".to_string()))?;

        reply_rx
            .recv()
            .map_err(|_| ChronDBError::OperationFailed("worker thread died".to_string()))?
    }

    /// Executes a query against the index.
    ///
    /// The query should be a JSON object matching the Lucene AST format.
    pub fn query(
        &self,
        query: &serde_json::Value,
        branch: Option<&str>,
    ) -> Result<serde_json::Value> {
        let query_str = serde_json::to_string(query)?;
        let (reply_tx, reply_rx) = mpsc::channel();

        self.sender
            .send(FfiCommand::Query {
                query: query_str,
                branch: branch.map(|s| s.to_string()),
                reply: reply_tx,
            })
            .map_err(|_| ChronDBError::OperationFailed("worker thread died".to_string()))?;

        reply_rx
            .recv()
            .map_err(|_| ChronDBError::OperationFailed("worker thread died".to_string()))?
    }

    /// Returns the last error message from the native library, if any.
    pub fn last_error(&self) -> Option<String> {
        let (reply_tx, reply_rx) = mpsc::channel();

        if self
            .sender
            .send(FfiCommand::LastError { reply: reply_tx })
            .is_err()
        {
            return None;
        }

        reply_rx.recv().ok().flatten()
    }
}

impl Drop for ChronDB {
    fn drop(&mut self) {
        // Send shutdown command to worker
        let _ = self.sender.send(FfiCommand::Shutdown);

        // Wait for worker to finish
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::env;
    use tempfile::TempDir;

    #[test]
    fn test_error_display() {
        let err = ChronDBError::NotFound;
        assert_eq!(err.to_string(), "document not found");
    }

    #[test]
    fn test_error_open_failed() {
        let err = ChronDBError::OpenFailed("path invalid".to_string());
        assert_eq!(err.to_string(), "failed to open database: path invalid");
    }

    #[test]
    fn test_error_setup_failed() {
        let err = ChronDBError::SetupFailed("download failed".to_string());
        assert_eq!(err.to_string(), "library setup failed: download failed");
    }

    #[test]
    fn test_error_isolate_creation_failed() {
        let err = ChronDBError::IsolateCreationFailed;
        assert_eq!(err.to_string(), "failed to create GraalVM isolate");
    }

    #[test]
    fn test_error_close_failed() {
        let err = ChronDBError::CloseFailed;
        assert_eq!(err.to_string(), "failed to close database");
    }

    #[test]
    fn test_error_operation_failed() {
        let err = ChronDBError::OperationFailed("timeout".to_string());
        assert_eq!(err.to_string(), "operation failed: timeout");
    }

    #[test]
    fn test_error_json_error() {
        let err = ChronDBError::JsonError("invalid json".to_string());
        assert_eq!(err.to_string(), "JSON error: invalid json");
    }

    #[test]
    fn test_error_from_serde_json() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let err: ChronDBError = json_err.into();

        match err {
            ChronDBError::JsonError(msg) => {
                assert!(!msg.is_empty());
            }
            _ => panic!("Expected JsonError variant"),
        }
    }

    #[test]
    #[serial]
    fn test_open_with_env_var_fallback_to_home() {
        // Set env var to empty directory - should still find lib in ~/.chrondb/lib/
        let temp_dir = TempDir::new().unwrap();
        env::set_var("CHRONDB_LIB_DIR", temp_dir.path().to_str().unwrap());

        // This test verifies the fix: even if CHRONDB_LIB_DIR points to an empty dir,
        // the library should be found in ~/.chrondb/lib/ if it exists there.
        // The result depends on whether the library is installed on the system.
        let result = ChronDB::open("/tmp/data", "/tmp/index");

        // We can't assert success or failure here because it depends on
        // whether the library exists in ~/.chrondb/lib/
        // But we verify that IF it fails, it's with an expected error type
        if let Err(err) = result {
            match err {
                ChronDBError::SetupFailed(_) | ChronDBError::IsolateCreationFailed => {
                    // Expected error types when library is not found or fails to load
                }
                other => panic!("Unexpected error type: {}", other),
            }
        }

        env::remove_var("CHRONDB_LIB_DIR");
    }

    #[test]
    fn test_ffi_worker_state_optional_cstring_with_some() {
        let result = FfiWorkerState::optional_cstring(Some("test"));
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn test_ffi_worker_state_optional_cstring_with_none() {
        let result = FfiWorkerState::optional_cstring(None);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_ffi_worker_state_optional_cstring_with_null_byte() {
        // String with embedded null byte should fail
        let result = FfiWorkerState::optional_cstring(Some("test\0string"));
        assert!(result.is_err());

        match result {
            Err(ChronDBError::OperationFailed(msg)) => {
                assert!(msg.contains("nul"));
            }
            Err(other) => panic!("Expected OperationFailed, got: {}", other),
            Ok(_) => panic!("Expected error but got Ok"),
        }
    }

    #[test]
    fn test_ffi_worker_state_ptr_or_null_with_some() {
        let cstring = std::ffi::CString::new("test").unwrap();
        let opt = Some(cstring);
        let ptr = FfiWorkerState::ptr_or_null(&opt);
        assert!(!ptr.is_null());
    }

    #[test]
    fn test_ffi_worker_state_ptr_or_null_with_none() {
        let opt: Option<std::ffi::CString> = None;
        let ptr = FfiWorkerState::ptr_or_null(&opt);
        assert!(ptr.is_null());
    }

    #[test]
    fn test_ensure_library_installed_exported() {
        // Verify the function is exported and callable
        // It will fail without the library, but should not panic
        let _ = ensure_library_installed();
    }

    #[test]
    fn test_get_library_dir_exported() {
        // Verify the function is exported and returns a valid path
        let dir = get_library_dir();
        // Should return Some on systems with home directory
        if dirs::home_dir().is_some() {
            assert!(dir.is_some());
        }
    }

    #[test]
    fn test_ffi_thread_stack_size() {
        // Verify the constant is set to 64MB
        assert_eq!(FFI_THREAD_STACK_SIZE, 64 * 1024 * 1024);
    }
}
