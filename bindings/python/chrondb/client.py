"""High-level Python client for ChronDB."""

import json
import os
import threading
from ctypes import c_void_p, pointer
from typing import Any, Dict, List, Optional, Tuple

from chrondb._ffi import load_library


class ChronDBError(Exception):
    """Base exception for ChronDB errors."""
    pass


class DocumentNotFoundError(ChronDBError):
    """Raised when a document is not found."""
    pass


# Global registry for shared isolates per path pair.
# This ensures multiple ChronDB instances for the same paths share
# the same GraalVM isolate, avoiding file lock conflicts.
_isolate_registry: Dict[Tuple[str, str], "_SharedIsolate"] = {}
_registry_lock = threading.Lock()


class _SharedIsolate:
    """Shared isolate that can be used by multiple ChronDB instances."""

    def __init__(self, lib, data_path: str, index_path: str):
        self._lib = lib
        self.data_path = data_path
        self.index_path = index_path
        self._ref_count = 1
        self._lock = threading.Lock()

        # Create isolate
        self._isolate = c_void_p()
        self._thread = c_void_p()

        ret = lib.graal_create_isolate(
            None, pointer(self._isolate), pointer(self._thread)
        )
        if ret != 0:
            raise ChronDBError("Failed to create GraalVM isolate")

        # Open database
        self._handle = lib.chrondb_open(
            self._thread,
            data_path.encode("utf-8"),
            index_path.encode("utf-8"),
        )
        if self._handle < 0:
            err = self._get_last_error()
            lib.graal_tear_down_isolate(self._thread)
            raise ChronDBError(f"Failed to open database: {err}")

    def _get_last_error(self) -> str:
        """Get the last error message from the native library."""
        err = self._lib.chrondb_last_error(self._thread)
        if err is None:
            return "unknown error"
        return err.decode("utf-8")

    def add_ref(self):
        """Increment reference count."""
        with self._lock:
            self._ref_count += 1

    def release(self) -> bool:
        """Decrement reference count. Returns True if isolate was closed."""
        with self._lock:
            self._ref_count -= 1
            if self._ref_count <= 0:
                self._close()
                return True
            return False

    def _close(self):
        """Close the isolate and release resources."""
        if self._handle >= 0:
            self._lib.chrondb_close(self._thread, self._handle)
            self._handle = -1
        if self._thread:
            self._lib.graal_tear_down_isolate(self._thread)
            self._thread = c_void_p()
            self._isolate = c_void_p()

    @property
    def lib(self):
        return self._lib

    @property
    def thread(self):
        return self._thread

    @property
    def handle(self):
        return self._handle


def _normalize_path(path: str) -> str:
    """Normalize path for consistent registry keys."""
    try:
        return os.path.realpath(path)
    except OSError:
        return path


def _get_or_create_isolate(data_path: str, index_path: str) -> _SharedIsolate:
    """Get existing isolate for path pair, or create new one."""
    key = (_normalize_path(data_path), _normalize_path(index_path))

    with _registry_lock:
        if key in _isolate_registry:
            isolate = _isolate_registry[key]
            isolate.add_ref()
            return isolate

        # Create new isolate
        lib = load_library()
        isolate = _SharedIsolate(lib, data_path, index_path)
        _isolate_registry[key] = isolate
        return isolate


def _release_isolate(data_path: str, index_path: str):
    """Release reference to isolate. Closes if no more references."""
    key = (_normalize_path(data_path), _normalize_path(index_path))

    with _registry_lock:
        if key in _isolate_registry:
            isolate = _isolate_registry[key]
            if isolate.release():
                del _isolate_registry[key]


class ChronDB:
    """A connection to a ChronDB database instance.

    Use as a context manager for automatic cleanup:

        with ChronDB("/tmp/data", "/tmp/index") as db:
            db.put("user:1", {"name": "Alice"})
            doc = db.get("user:1")

    Multiple instances opening the same paths share a single GraalVM isolate,
    ensuring thread-safe concurrent access without file lock conflicts.
    """

    def __init__(self, data_path: str, index_path: str):
        """Open a ChronDB database.

        Args:
            data_path: Path for the Git repository (data storage).
            index_path: Path for the Lucene index.

        Raises:
            ChronDBError: If the database cannot be opened.
        """
        self._data_path = data_path
        self._index_path = index_path
        self._shared = _get_or_create_isolate(data_path, index_path)
        self._closed = False

    @property
    def _lib(self):
        return self._shared.lib

    @property
    def _thread(self):
        return self._shared.thread

    @property
    def _handle(self):
        return self._shared.handle

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self):
        """Close the database connection and release resources.

        The underlying GraalVM isolate is only closed when all ChronDB
        instances using the same paths have been closed.
        """
        if not self._closed:
            self._closed = True
            _release_isolate(self._data_path, self._index_path)

    def put(self, id: str, doc: Dict[str, Any], branch: Optional[str] = None) -> Dict[str, Any]:
        """Save a document.

        Args:
            id: Document ID (e.g., "user:1").
            doc: Document data as a dictionary.
            branch: Optional branch name (None for default).

        Returns:
            The saved document as a dictionary.

        Raises:
            ChronDBError: If the operation fails.
        """
        json_str = json.dumps(doc)
        result = self._lib.chrondb_put(
            self._thread,
            self._handle,
            id.encode("utf-8"),
            json_str.encode("utf-8"),
            branch.encode("utf-8") if branch else None,
        )
        if result is None:
            raise ChronDBError(f"Failed to put document: {self._get_last_error()}")
        return json.loads(result.decode("utf-8"))

    def get(self, id: str, branch: Optional[str] = None) -> Dict[str, Any]:
        """Get a document by ID.

        Args:
            id: Document ID.
            branch: Optional branch name.

        Returns:
            The document as a dictionary.

        Raises:
            DocumentNotFoundError: If the document doesn't exist.
            ChronDBError: If the operation fails.
        """
        result = self._lib.chrondb_get(
            self._thread,
            self._handle,
            id.encode("utf-8"),
            branch.encode("utf-8") if branch else None,
        )
        if result is None:
            raise DocumentNotFoundError(f"Document '{id}' not found")
        return json.loads(result.decode("utf-8"))

    def delete(self, id: str, branch: Optional[str] = None) -> bool:
        """Delete a document by ID.

        Args:
            id: Document ID.
            branch: Optional branch name.

        Returns:
            True if the document was deleted.

        Raises:
            DocumentNotFoundError: If the document doesn't exist.
            ChronDBError: If the operation fails.
        """
        ret = self._lib.chrondb_delete(
            self._thread,
            self._handle,
            id.encode("utf-8"),
            branch.encode("utf-8") if branch else None,
        )
        if ret == 0:
            return True
        elif ret == 1:
            raise DocumentNotFoundError(f"Document '{id}' not found")
        else:
            raise ChronDBError(f"Failed to delete document: {self._get_last_error()}")

    def list_by_prefix(self, prefix: str, branch: Optional[str] = None) -> List[Dict[str, Any]]:
        """List documents by ID prefix.

        Args:
            prefix: ID prefix to match.
            branch: Optional branch name.

        Returns:
            List of matching documents.
        """
        result = self._lib.chrondb_list_by_prefix(
            self._thread,
            self._handle,
            prefix.encode("utf-8"),
            branch.encode("utf-8") if branch else None,
        )
        if result is None:
            return []
        return json.loads(result.decode("utf-8"))

    def list_by_table(self, table: str, branch: Optional[str] = None) -> List[Dict[str, Any]]:
        """List documents by table name.

        Args:
            table: Table name.
            branch: Optional branch name.

        Returns:
            List of matching documents.
        """
        result = self._lib.chrondb_list_by_table(
            self._thread,
            self._handle,
            table.encode("utf-8"),
            branch.encode("utf-8") if branch else None,
        )
        if result is None:
            return []
        return json.loads(result.decode("utf-8"))

    def history(self, id: str, branch: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get the change history of a document.

        Args:
            id: Document ID.
            branch: Optional branch name.

        Returns:
            List of history entries.
        """
        result = self._lib.chrondb_history(
            self._thread,
            self._handle,
            id.encode("utf-8"),
            branch.encode("utf-8") if branch else None,
        )
        if result is None:
            return []
        return json.loads(result.decode("utf-8"))

    def query(self, query: Dict[str, Any], branch: Optional[str] = None) -> Dict[str, Any]:
        """Execute a query against the index.

        Args:
            query: Query map (Lucene AST format).
            branch: Optional branch name.

        Returns:
            Query results with 'results', 'total', 'limit', 'offset'.

        Raises:
            ChronDBError: If the query fails.
        """
        query_str = json.dumps(query)
        result = self._lib.chrondb_query(
            self._thread,
            self._handle,
            query_str.encode("utf-8"),
            branch.encode("utf-8") if branch else None,
        )
        if result is None:
            raise ChronDBError(f"Query failed: {self._get_last_error()}")
        return json.loads(result.decode("utf-8"))

    def _get_last_error(self) -> str:
        """Get the last error message from the native library."""
        err = self._lib.chrondb_last_error(self._thread)
        if err is None:
            return "unknown error"
        return err.decode("utf-8")
