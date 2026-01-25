"""Low-level ctypes FFI bindings for the ChronDB shared library."""

import ctypes
import os
import platform
import shutil
import sys
from ctypes import (
    POINTER,
    c_char_p,
    c_int,
    c_void_p,
)
from pathlib import Path


def _chrondb_home_lib_dir() -> Path:
    """Standard location for ChronDB shared library: ~/.chrondb/lib/"""
    return Path.home() / ".chrondb" / "lib"


def _get_lib_name() -> str:
    """Get platform-specific library filename."""
    system = platform.system()
    if system == "Darwin":
        return "libchrondb.dylib"
    elif system == "Linux":
        return "libchrondb.so"
    elif system == "Windows":
        return "chrondb.dll"
    return "libchrondb.so"


def _install_bundled_to_home(bundled_path: str, home_lib_dir: Path) -> str:
    """Copy bundled library to ~/.chrondb/lib/ for system-wide use."""
    try:
        home_lib_dir.mkdir(parents=True, exist_ok=True)
        lib_name = _get_lib_name()
        dest = home_lib_dir / lib_name
        shutil.copy2(bundled_path, dest)
        return str(dest)
    except (OSError, shutil.Error):
        # If we can't copy, just use the bundled path directly
        return bundled_path


def _find_library():
    """Locate the ChronDB shared library.

    Search order:
    1. CHRONDB_LIB_PATH env var (explicit full path)
    2. CHRONDB_LIB_DIR env var (directory containing the lib)
    3. ~/.chrondb/lib/ (standard location)
    4. Bundled with package (pip install) - also installs to ~/.chrondb/lib/
    5. Development paths
    6. System paths
    """
    lib_name = _get_lib_name()
    home_lib_dir = _chrondb_home_lib_dir()

    # 1. Check explicit env var (full path)
    lib_path = os.environ.get("CHRONDB_LIB_PATH")
    if lib_path and os.path.exists(lib_path):
        return lib_path

    # 2. Check explicit env var (directory)
    lib_dir = os.environ.get("CHRONDB_LIB_DIR")
    if lib_dir:
        full_path = os.path.join(lib_dir, lib_name)
        if os.path.exists(full_path):
            return full_path

    # 3. Check standard location ~/.chrondb/lib/
    home_lib_path = home_lib_dir / lib_name
    if home_lib_path.exists():
        return str(home_lib_path)

    # 4. Check bundled with package, install to ~/.chrondb/lib/
    bundled_path = os.path.join(os.path.dirname(__file__), "lib", lib_name)
    if os.path.exists(bundled_path):
        return _install_bundled_to_home(bundled_path, home_lib_dir)

    # 5. Development paths
    dev_paths = [
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "target"),
    ]
    for path in dev_paths:
        full_path = os.path.join(path, lib_name)
        if os.path.exists(full_path):
            return full_path

    # 6. System paths
    system_paths = ["/usr/local/lib", "/usr/lib"]
    for path in system_paths:
        full_path = os.path.join(path, lib_name)
        if os.path.exists(full_path):
            return full_path

    raise OSError(
        f"Cannot find {lib_name}. Install options:\n"
        f"  1. pip install chrondb (recommended)\n"
        f"  2. Download from GitHub Releases to ~/.chrondb/lib/\n"
        f"  3. Set CHRONDB_LIB_PATH to the full library path\n"
        f"  4. Set CHRONDB_LIB_DIR to the directory containing the library"
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
