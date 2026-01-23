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

pub use error::{ChronDBError, Result};

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

/// A connection to a ChronDB database instance.
///
/// Manages a GraalVM isolate and a database handle.
/// The database is automatically closed when this struct is dropped.
pub struct ChronDB {
    isolate: *mut ffi::graal_isolate_t,
    thread: *mut ffi::graal_isolatethread_t,
    handle: i32,
}

// ChronDB is safe to send across threads because the GraalVM isolate
// thread is managed internally and all operations are synchronized.
unsafe impl Send for ChronDB {}

impl ChronDB {
    /// Opens a ChronDB database at the given paths.
    ///
    /// # Arguments
    /// * `data_path` - Path for the Git repository (data storage)
    /// * `index_path` - Path for the Lucene index
    pub fn open(data_path: &str, index_path: &str) -> Result<Self> {
        let mut isolate: *mut ffi::graal_isolate_t = ptr::null_mut();
        let mut thread: *mut ffi::graal_isolatethread_t = ptr::null_mut();

        let ret = unsafe {
            ffi::graal_create_isolate(ptr::null_mut(), &mut isolate, &mut thread)
        };
        if ret != 0 {
            return Err(ChronDBError::IsolateCreationFailed);
        }

        let c_data = CString::new(data_path).map_err(|e| ChronDBError::OpenFailed(e.to_string()))?;
        let c_index = CString::new(index_path).map_err(|e| ChronDBError::OpenFailed(e.to_string()))?;

        let handle = unsafe {
            ffi::chrondb_open(thread, c_data.as_ptr(), c_index.as_ptr())
        };

        if handle < 0 {
            let err = Self::get_last_error_raw(thread);
            unsafe { ffi::graal_tear_down_isolate(thread) };
            return Err(ChronDBError::OpenFailed(err.unwrap_or_default()));
        }

        Ok(ChronDB { isolate, thread, handle })
    }

    /// Saves a document with the given ID.
    ///
    /// Returns the saved document as a JSON value.
    pub fn put(&self, id: &str, doc: &serde_json::Value, branch: Option<&str>) -> Result<serde_json::Value> {
        let c_id = CString::new(id).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let json_str = serde_json::to_string(doc)?;
        let c_json = CString::new(json_str).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let c_branch = Self::optional_cstring(branch)?;

        let result = unsafe {
            ffi::chrondb_put(
                self.thread,
                self.handle,
                c_id.as_ptr(),
                c_json.as_ptr(),
                Self::ptr_or_null(&c_branch),
            )
        };

        self.parse_string_result(result)
    }

    /// Gets a document by ID.
    ///
    /// Returns `None` if the document is not found.
    pub fn get(&self, id: &str, branch: Option<&str>) -> Result<serde_json::Value> {
        let c_id = CString::new(id).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let c_branch = Self::optional_cstring(branch)?;

        let result = unsafe {
            ffi::chrondb_get(
                self.thread,
                self.handle,
                c_id.as_ptr(),
                Self::ptr_or_null(&c_branch),
            )
        };

        if result.is_null() {
            return Err(ChronDBError::NotFound);
        }
        self.parse_string_result(result)
    }

    /// Deletes a document by ID.
    ///
    /// Returns `Ok(())` on success, `Err(NotFound)` if the document doesn't exist.
    pub fn delete(&self, id: &str, branch: Option<&str>) -> Result<()> {
        let c_id = CString::new(id).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let c_branch = Self::optional_cstring(branch)?;

        let ret = unsafe {
            ffi::chrondb_delete(
                self.thread,
                self.handle,
                c_id.as_ptr(),
                Self::ptr_or_null(&c_branch),
            )
        };

        match ret {
            0 => Ok(()),
            1 => Err(ChronDBError::NotFound),
            _ => Err(self.last_error_or("delete failed")),
        }
    }

    /// Lists documents by ID prefix.
    pub fn list_by_prefix(&self, prefix: &str, branch: Option<&str>) -> Result<serde_json::Value> {
        let c_prefix = CString::new(prefix).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let c_branch = Self::optional_cstring(branch)?;

        let result = unsafe {
            ffi::chrondb_list_by_prefix(
                self.thread,
                self.handle,
                c_prefix.as_ptr(),
                Self::ptr_or_null(&c_branch),
            )
        };

        if result.is_null() {
            return Ok(serde_json::Value::Array(vec![]));
        }
        self.parse_string_result(result)
    }

    /// Lists documents by table name.
    pub fn list_by_table(&self, table: &str, branch: Option<&str>) -> Result<serde_json::Value> {
        let c_table = CString::new(table).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let c_branch = Self::optional_cstring(branch)?;

        let result = unsafe {
            ffi::chrondb_list_by_table(
                self.thread,
                self.handle,
                c_table.as_ptr(),
                Self::ptr_or_null(&c_branch),
            )
        };

        if result.is_null() {
            return Ok(serde_json::Value::Array(vec![]));
        }
        self.parse_string_result(result)
    }

    /// Gets the history of changes for a document.
    pub fn history(&self, id: &str, branch: Option<&str>) -> Result<serde_json::Value> {
        let c_id = CString::new(id).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let c_branch = Self::optional_cstring(branch)?;

        let result = unsafe {
            ffi::chrondb_history(
                self.thread,
                self.handle,
                c_id.as_ptr(),
                Self::ptr_or_null(&c_branch),
            )
        };

        if result.is_null() {
            return Ok(serde_json::Value::Array(vec![]));
        }
        self.parse_string_result(result)
    }

    /// Executes a query against the index.
    ///
    /// The query should be a JSON object matching the Lucene AST format.
    pub fn query(&self, query: &serde_json::Value, branch: Option<&str>) -> Result<serde_json::Value> {
        let query_str = serde_json::to_string(query)?;
        let c_query = CString::new(query_str).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?;
        let c_branch = Self::optional_cstring(branch)?;

        let result = unsafe {
            ffi::chrondb_query(
                self.thread,
                self.handle,
                c_query.as_ptr(),
                Self::ptr_or_null(&c_branch),
            )
        };

        if result.is_null() {
            return Err(self.last_error_or("query failed"));
        }
        self.parse_string_result(result)
    }

    /// Returns the last error message from the native library, if any.
    pub fn last_error(&self) -> Option<String> {
        Self::get_last_error_raw(self.thread)
    }

    // --- Private helpers ---

    fn get_last_error_raw(thread: *mut ffi::graal_isolatethread_t) -> Option<String> {
        let ptr = unsafe { ffi::chrondb_last_error(thread) };
        if ptr.is_null() {
            None
        } else {
            let s = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned();
            unsafe { ffi::chrondb_free_string(thread, ptr) };
            Some(s)
        }
    }

    fn last_error_or(&self, default: &str) -> ChronDBError {
        let msg = self.last_error().unwrap_or_else(|| default.to_string());
        ChronDBError::OperationFailed(msg)
    }

    fn optional_cstring(s: Option<&str>) -> Result<Option<CString>> {
        match s {
            Some(v) => Ok(Some(CString::new(v).map_err(|e| ChronDBError::OperationFailed(e.to_string()))?)),
            None => Ok(None),
        }
    }

    fn ptr_or_null(opt: &Option<CString>) -> *const c_char {
        match opt {
            Some(cs) => cs.as_ptr(),
            None => ptr::null(),
        }
    }

    fn parse_string_result(&self, ptr: *mut c_char) -> Result<serde_json::Value> {
        if ptr.is_null() {
            return Err(self.last_error_or("null result"));
        }
        let s = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned();
        unsafe { ffi::chrondb_free_string(self.thread, ptr) };
        let val: serde_json::Value = serde_json::from_str(&s)?;
        Ok(val)
    }
}

impl Drop for ChronDB {
    fn drop(&mut self) {
        if self.handle >= 0 {
            unsafe {
                ffi::chrondb_close(self.thread, self.handle);
            }
            self.handle = -1;
        }
        if !self.thread.is_null() {
            unsafe {
                ffi::graal_tear_down_isolate(self.thread);
            }
            self.thread = ptr::null_mut();
            self.isolate = ptr::null_mut();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
