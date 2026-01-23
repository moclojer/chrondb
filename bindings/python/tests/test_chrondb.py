"""Tests for the ChronDB Python bindings.

These tests require the shared library to be built and available.
Set CHRONDB_LIB_PATH to the path of the shared library before running.
"""

import json
import os
import tempfile

import pytest

# Skip all tests if library is not available
pytestmark = pytest.mark.skipif(
    not os.environ.get("CHRONDB_LIB_PATH") and not os.path.exists(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "target", "libchrondb.dylib")
    ) and not os.path.exists(
        os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "target", "libchrondb.so")
    ),
    reason="ChronDB shared library not available",
)


from chrondb import ChronDB
from chrondb.client import ChronDBError, DocumentNotFoundError


class TestChronDB:
    """Integration tests for ChronDB Python bindings."""

    @pytest.fixture
    def db(self, tmp_path):
        """Create a temporary ChronDB instance."""
        data_path = str(tmp_path / "data")
        index_path = str(tmp_path / "index")
        with ChronDB(data_path, index_path) as db:
            yield db

    def test_put_and_get(self, db):
        """Test saving and retrieving a document."""
        doc = {"name": "Alice", "age": 30}
        saved = db.put("user:1", doc)
        assert saved["name"] == "Alice"
        assert saved["id"] == "user:1"

        retrieved = db.get("user:1")
        assert retrieved["name"] == "Alice"

    def test_get_not_found(self, db):
        """Test getting a non-existent document."""
        with pytest.raises(DocumentNotFoundError):
            db.get("nonexistent:999")

    def test_delete(self, db):
        """Test deleting a document."""
        db.put("user:2", {"name": "Bob"})
        assert db.delete("user:2") is True

        with pytest.raises(DocumentNotFoundError):
            db.get("user:2")

    def test_delete_not_found(self, db):
        """Test deleting a non-existent document."""
        with pytest.raises(DocumentNotFoundError):
            db.delete("nonexistent:999")

    def test_list_by_prefix(self, db):
        """Test listing documents by prefix."""
        db.put("user:1", {"name": "Alice"})
        db.put("user:2", {"name": "Bob"})
        db.put("product:1", {"name": "Widget"})

        users = db.list_by_prefix("user:")
        assert len(users) >= 2

    def test_list_by_table(self, db):
        """Test listing documents by table."""
        db.put("user:1", {"name": "Alice"})
        db.put("user:2", {"name": "Bob"})

        users = db.list_by_table("user")
        assert len(users) >= 2

    def test_history(self, db):
        """Test getting document history."""
        db.put("user:1", {"name": "Alice", "version": 1})
        db.put("user:1", {"name": "Alice Updated", "version": 2})

        history = db.history("user:1")
        assert len(history) >= 1

    def test_context_manager(self, tmp_path):
        """Test using ChronDB as a context manager."""
        data_path = str(tmp_path / "data")
        index_path = str(tmp_path / "index")

        with ChronDB(data_path, index_path) as db:
            db.put("test:1", {"value": "hello"})
            result = db.get("test:1")
            assert result["value"] == "hello"
