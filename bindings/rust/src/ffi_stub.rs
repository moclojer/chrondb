// Stub FFI bindings for compilation without the native library.
// These are replaced by bindgen-generated bindings when the library is available.

use std::os::raw::{c_char, c_int, c_void};

pub type graal_isolate_t = c_void;
pub type graal_isolatethread_t = c_void;

#[repr(C)]
pub struct graal_create_isolate_params_t {
    pub version: c_int,
    pub reserved_address_space_size: usize,
}

extern "C" {
    pub fn graal_create_isolate(
        params: *mut graal_create_isolate_params_t,
        isolate: *mut *mut graal_isolate_t,
        thread: *mut *mut graal_isolatethread_t,
    ) -> c_int;

    pub fn graal_tear_down_isolate(thread: *mut graal_isolatethread_t) -> c_int;

    pub fn chrondb_open(
        thread: *mut graal_isolatethread_t,
        data_path: *const c_char,
        index_path: *const c_char,
    ) -> c_int;

    pub fn chrondb_close(
        thread: *mut graal_isolatethread_t,
        handle: c_int,
    ) -> c_int;

    pub fn chrondb_put(
        thread: *mut graal_isolatethread_t,
        handle: c_int,
        id: *const c_char,
        json_doc: *const c_char,
        branch: *const c_char,
    ) -> *mut c_char;

    pub fn chrondb_get(
        thread: *mut graal_isolatethread_t,
        handle: c_int,
        id: *const c_char,
        branch: *const c_char,
    ) -> *mut c_char;

    pub fn chrondb_delete(
        thread: *mut graal_isolatethread_t,
        handle: c_int,
        id: *const c_char,
        branch: *const c_char,
    ) -> c_int;

    pub fn chrondb_list_by_prefix(
        thread: *mut graal_isolatethread_t,
        handle: c_int,
        prefix: *const c_char,
        branch: *const c_char,
    ) -> *mut c_char;

    pub fn chrondb_list_by_table(
        thread: *mut graal_isolatethread_t,
        handle: c_int,
        table: *const c_char,
        branch: *const c_char,
    ) -> *mut c_char;

    pub fn chrondb_history(
        thread: *mut graal_isolatethread_t,
        handle: c_int,
        id: *const c_char,
        branch: *const c_char,
    ) -> *mut c_char;

    pub fn chrondb_query(
        thread: *mut graal_isolatethread_t,
        handle: c_int,
        query_json: *const c_char,
        branch: *const c_char,
    ) -> *mut c_char;

    pub fn chrondb_free_string(
        thread: *mut graal_isolatethread_t,
        ptr: *mut c_char,
    );

    pub fn chrondb_last_error(
        thread: *mut graal_isolatethread_t,
    ) -> *mut c_char;
}
