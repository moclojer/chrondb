"""Low-level ctypes FFI bindings for the ChronDB shared library."""

import ctypes
import os
import platform
import sys
from ctypes import (
    POINTER,
    c_char_p,
    c_int,
    c_void_p,
)


def _find_library():
    """Locate the ChronDB shared library."""
    # Check explicit env var first
    lib_path = os.environ.get("CHRONDB_LIB_PATH")
    if lib_path and os.path.exists(lib_path):
        return lib_path

    # Determine the library filename for this platform
    system = platform.system()
    if system == "Darwin":
        lib_name = "libchrondb.dylib"
    elif system == "Linux":
        lib_name = "libchrondb.so"
    elif system == "Windows":
        lib_name = "chrondb.dll"
    else:
        lib_name = "libchrondb.so"

    # Search in common locations (bundled lib first for pip installs)
    search_paths = [
        # Bundled with the package (pip install)
        os.path.join(os.path.dirname(__file__), "lib"),
        # Relative to this file (development)
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "target"),
        # System paths
        "/usr/local/lib",
        "/usr/lib",
    ]

    # CHRONDB_LIB_DIR takes priority over bundled
    lib_dir = os.environ.get("CHRONDB_LIB_DIR")
    if lib_dir:
        search_paths.insert(0, lib_dir)

    for path in search_paths:
        full_path = os.path.join(path, lib_name)
        if os.path.exists(full_path):
            return full_path

    raise OSError(
        f"Cannot find {lib_name}. Set CHRONDB_LIB_PATH to the full path "
        f"or CHRONDB_LIB_DIR to the directory containing the library."
    )


def load_library():
    """Load the ChronDB shared library and configure function signatures."""
    lib_path = _find_library()
    lib = ctypes.CDLL(lib_path)

    # --- GraalVM Isolate Management ---

    # graal_create_isolate(params*, isolate**, thread**) -> int
    lib.graal_create_isolate.argtypes = [c_void_p, POINTER(c_void_p), POINTER(c_void_p)]
    lib.graal_create_isolate.restype = c_int

    # graal_tear_down_isolate(thread*) -> int
    lib.graal_tear_down_isolate.argtypes = [c_void_p]
    lib.graal_tear_down_isolate.restype = c_int

    # --- ChronDB Functions ---

    # chrondb_open(thread, data_path, index_path) -> int handle
    lib.chrondb_open.argtypes = [c_void_p, c_char_p, c_char_p]
    lib.chrondb_open.restype = c_int

    # chrondb_close(thread, handle) -> int
    lib.chrondb_close.argtypes = [c_void_p, c_int]
    lib.chrondb_close.restype = c_int

    # chrondb_put(thread, handle, id, json, branch) -> char*
    lib.chrondb_put.argtypes = [c_void_p, c_int, c_char_p, c_char_p, c_char_p]
    lib.chrondb_put.restype = c_char_p

    # chrondb_get(thread, handle, id, branch) -> char*
    lib.chrondb_get.argtypes = [c_void_p, c_int, c_char_p, c_char_p]
    lib.chrondb_get.restype = c_char_p

    # chrondb_delete(thread, handle, id, branch) -> int
    lib.chrondb_delete.argtypes = [c_void_p, c_int, c_char_p, c_char_p]
    lib.chrondb_delete.restype = c_int

    # chrondb_list_by_prefix(thread, handle, prefix, branch) -> char*
    lib.chrondb_list_by_prefix.argtypes = [c_void_p, c_int, c_char_p, c_char_p]
    lib.chrondb_list_by_prefix.restype = c_char_p

    # chrondb_list_by_table(thread, handle, table, branch) -> char*
    lib.chrondb_list_by_table.argtypes = [c_void_p, c_int, c_char_p, c_char_p]
    lib.chrondb_list_by_table.restype = c_char_p

    # chrondb_history(thread, handle, id, branch) -> char*
    lib.chrondb_history.argtypes = [c_void_p, c_int, c_char_p, c_char_p]
    lib.chrondb_history.restype = c_char_p

    # chrondb_query(thread, handle, query_json, branch) -> char*
    lib.chrondb_query.argtypes = [c_void_p, c_int, c_char_p, c_char_p]
    lib.chrondb_query.restype = c_char_p

    # chrondb_free_string(thread, ptr) -> void
    lib.chrondb_free_string.argtypes = [c_void_p, c_char_p]
    lib.chrondb_free_string.restype = None

    # chrondb_last_error(thread) -> char*
    lib.chrondb_last_error.argtypes = [c_void_p]
    lib.chrondb_last_error.restype = c_char_p

    return lib
