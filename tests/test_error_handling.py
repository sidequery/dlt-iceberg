"""
Tests for error handling and classification.

Tests retryable vs non-retryable errors, error messages, and logging.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

from pyiceberg.exceptions import (
    CommitFailedException,
    CommitStateUnknownException,
    WaitingForLockException,
    ServiceUnavailableError,
    ServerError,
    AuthorizationExpiredError,
    NoSuchTableError,
    NoSuchNamespaceError,
    TableAlreadyExistsError,
    ValidationError,
    BadRequestError,
    ForbiddenError,
    UnauthorizedError,
    OAuthError,
    NotInstalledError,
    RESTError,
)

from dlt_iceberg.error_handling import (
    is_retryable_error,
    get_error_category,
    log_error_with_context,
    get_user_friendly_error_message,
    ErrorCategory,
)


class TestErrorClassification:
    """Test error classification logic."""

    def test_retryable_transient_errors(self):
        """Test that transient errors are classified as retryable."""
        retryable_errors = [
            CommitFailedException("commit failed"),
            CommitStateUnknownException("state unknown"),
            WaitingForLockException("waiting for lock"),
            ServiceUnavailableError("service unavailable"),
            ServerError("server error"),
        ]

        for error in retryable_errors:
            assert is_retryable_error(error), f"{type(error).__name__} should be retryable"
            category = get_error_category(error)
            assert category == ErrorCategory.RETRYABLE_TRANSIENT

    def test_retryable_auth_errors(self):
        """Test that auth errors are classified as retryable."""
        auth_errors = [
            AuthorizationExpiredError("authorization expired"),
        ]

        for error in auth_errors:
            assert is_retryable_error(error), f"{type(error).__name__} should be retryable"
            category = get_error_category(error)
            assert category == ErrorCategory.RETRYABLE_AUTH

    def test_non_retryable_client_errors(self):
        """Test that client errors are non-retryable."""
        non_retryable_errors = [
            NoSuchTableError("table not found"),
            NoSuchNamespaceError("namespace not found"),
            TableAlreadyExistsError("table exists"),
            ValidationError("validation failed"),
            BadRequestError("bad request"),
        ]

        for error in non_retryable_errors:
            assert not is_retryable_error(error), f"{type(error).__name__} should not be retryable"
            category = get_error_category(error)
            assert category == ErrorCategory.NON_RETRYABLE_CLIENT

    def test_non_retryable_server_errors(self):
        """Test that server auth errors are non-retryable."""
        non_retryable_errors = [
            ForbiddenError("forbidden"),
            UnauthorizedError("unauthorized"),
            OAuthError("oauth failed"),
        ]

        for error in non_retryable_errors:
            assert not is_retryable_error(error), f"{type(error).__name__} should not be retryable"
            category = get_error_category(error)
            assert category == ErrorCategory.NON_RETRYABLE_SERVER

    def test_non_retryable_config_errors(self):
        """Test that config errors are non-retryable."""
        error = NotInstalledError("dependency not installed")
        assert not is_retryable_error(error)
        category = get_error_category(error)
        assert category == ErrorCategory.NON_RETRYABLE_CONFIG

    def test_unknown_rest_error(self):
        """Test that unknown RESTError is conservatively retryable."""
        error = RESTError("unknown rest error")
        # RESTError is a catch-all, so we retry it
        assert is_retryable_error(error)
        category = get_error_category(error)
        assert category == "unknown_rest_error"

    def test_truly_unknown_error(self):
        """Test that truly unknown errors are non-retryable."""
        error = RuntimeError("some random error")
        assert not is_retryable_error(error), "Unknown errors should not be retried"
        category = get_error_category(error)
        assert category == "unknown_error"


class TestErrorMessages:
    """Test user-friendly error messages."""

    def test_no_such_table_message(self):
        """Test error message for NoSuchTableError."""
        error = NoSuchTableError("table foo not found")
        message = get_user_friendly_error_message(error, "write data")
        assert "foo not found" in message
        assert "Hint" in message
        assert "does not exist" in message

    def test_no_such_namespace_message(self):
        """Test error message for NoSuchNamespaceError."""
        error = NoSuchNamespaceError("namespace bar not found")
        message = get_user_friendly_error_message(error, "create table")
        assert "bar not found" in message
        assert "Hint" in message

    def test_validation_error_message(self):
        """Test error message for ValidationError."""
        error = ValidationError("schema mismatch")
        message = get_user_friendly_error_message(error, "write data")
        assert "schema mismatch" in message
        assert "Hint" in message
        assert "schema validation" in message

    def test_unauthorized_message(self):
        """Test error message for UnauthorizedError."""
        error = UnauthorizedError("no permission")
        message = get_user_friendly_error_message(error, "load table")
        assert "no permission" in message
        assert "Hint" in message
        assert "credentials" in message

    def test_commit_failed_message(self):
        """Test error message for CommitFailedException."""
        error = CommitFailedException("concurrent write")
        message = get_user_friendly_error_message(error, "append data")
        assert "concurrent write" in message
        assert "Note" in message
        assert "retried" in message


class TestErrorLogging:
    """Test error logging functionality."""

    def test_log_retryable_error(self, caplog):
        """Test logging for retryable errors."""
        import logging
        caplog.set_level(logging.WARNING)

        error = CommitFailedException("test commit failure")
        log_error_with_context(
            error,
            operation="write data",
            table_name="test_table",
            attempt=1,
            max_attempts=5,
            include_traceback=False,
        )

        assert "Retryable error" in caplog.text
        assert "test commit failure" in caplog.text
        assert "test_table" in caplog.text
        assert "1/5" in caplog.text

    def test_log_non_retryable_error(self, caplog):
        """Test logging for non-retryable errors."""
        import logging
        caplog.set_level(logging.ERROR)

        error = NoSuchTableError("table not found")
        log_error_with_context(
            error,
            operation="load table",
            table_name="missing_table",
            include_traceback=True,
        )

        assert "Non-retryable error" in caplog.text
        assert "table not found" in caplog.text
        assert "missing_table" in caplog.text


class TestDestinationErrorHandling:
    """Test error handling in the destination handler."""

    def create_test_parquet(self, temp_dir: str) -> str:
        """Create a test parquet file."""
        data = pa.table({
            "id": pa.array([1, 2, 3]),
            "name": pa.array(["a", "b", "c"]),
        })
        parquet_path = Path(temp_dir) / "test.parquet"
        pq.write_table(data, str(parquet_path))
        return str(parquet_path)

    def test_non_retryable_error_fails_immediately(self):
        """Test that non-retryable errors fail without retrying."""
        from dlt_iceberg.destination import _iceberg_rest_handler
        import dlt

        temp_dir = tempfile.mkdtemp()
        try:
            parquet_path = self.create_test_parquet(temp_dir)

            # Mock table schema
            table_schema = {
                "name": "test_table",
                "columns": {
                    "id": {"data_type": "bigint"},
                    "name": {"data_type": "text"},
                },
                "write_disposition": "append",
            }

            # Test with ValidationError (non-retryable)
            with patch("dlt_iceberg.destination.load_catalog") as mock_catalog:
                mock_catalog_instance = MagicMock()
                mock_catalog.return_value = mock_catalog_instance

                # Namespace operations succeed
                mock_catalog_instance.list_namespaces.return_value = [("default",)]

                # Table load raises ValidationError
                mock_catalog_instance.load_table.side_effect = ValidationError("invalid schema")

                # Should fail immediately without retrying
                with pytest.raises(RuntimeError) as exc_info:
                    _iceberg_rest_handler(
                        items=parquet_path,
                        table=table_schema,
                        catalog_uri="sqlite:///test.db",
                        warehouse="/tmp/warehouse",
                        namespace="default",
                        max_retries=5,
                    )

                # Check that it failed with the right error
                assert "invalid schema" in str(exc_info.value).lower()
                assert "validation" in str(exc_info.value).lower()

                # Verify load_table was called only once (no retries)
                assert mock_catalog_instance.load_table.call_count == 1

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_retryable_error_is_retried_multiple_times(self):
        """Test that retryable errors are retried multiple times with backoff."""
        from dlt_iceberg.destination import _iceberg_rest_handler

        temp_dir = tempfile.mkdtemp()
        try:
            parquet_path = self.create_test_parquet(temp_dir)

            table_schema = {
                "name": "test_table",
                "columns": {
                    "id": {"data_type": "bigint"},
                    "name": {"data_type": "text"},
                },
                "write_disposition": "append",
            }

            with patch("dlt_iceberg.destination.load_catalog") as mock_catalog:
                mock_catalog_instance = MagicMock()
                mock_catalog.return_value = mock_catalog_instance

                mock_catalog_instance.list_namespaces.return_value = [("default",)]

                # Simulate CommitFailedException on first 2 attempts, then succeed
                attempt_count = [0]

                # Just keep raising CommitFailedException - we'll patch the whole operation
                mock_catalog_instance.load_table.side_effect = CommitFailedException("concurrent write")

                # Mock time.sleep to speed up test
                with patch("dlt_iceberg.destination.time.sleep"):
                    # Should fail after retries (since we always raise)
                    with pytest.raises(RuntimeError) as exc_info:
                        _iceberg_rest_handler(
                            items=parquet_path,
                            table=table_schema,
                            catalog_uri="sqlite:///test.db",
                            warehouse="/tmp/warehouse",
                            namespace="default",
                            max_retries=3,
                            retry_backoff_base=2.0,
                        )

                    # Should have error message about retries
                    assert "concurrent write" in str(exc_info.value).lower()

                # Verify it was retried multiple times
                assert mock_catalog_instance.load_table.call_count == 3, "Should have tried 3 times"

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_retryable_error_exhausts_retries(self):
        """Test that retryable errors eventually fail after max retries."""
        from dlt_iceberg.destination import _iceberg_rest_handler

        temp_dir = tempfile.mkdtemp()
        try:
            parquet_path = self.create_test_parquet(temp_dir)

            table_schema = {
                "name": "test_table",
                "columns": {
                    "id": {"data_type": "bigint"},
                    "name": {"data_type": "text"},
                },
                "write_disposition": "append",
            }

            with patch("dlt_iceberg.destination.load_catalog") as mock_catalog:
                mock_catalog_instance = MagicMock()
                mock_catalog.return_value = mock_catalog_instance

                mock_catalog_instance.list_namespaces.return_value = [("default",)]

                # Always raise CommitFailedException
                mock_catalog_instance.load_table.side_effect = CommitFailedException("always fails")

                # Mock time.sleep
                with patch("dlt_iceberg.destination.time.sleep"):
                    # Should fail after max_retries
                    with pytest.raises(RuntimeError) as exc_info:
                        _iceberg_rest_handler(
                            items=parquet_path,
                            table=table_schema,
                            catalog_uri="sqlite:///test.db",
                            warehouse="/tmp/warehouse",
                            namespace="default",
                            max_retries=3,
                            retry_backoff_base=2.0,
                        )

                    # Check error message
                    assert "always fails" in str(exc_info.value).lower()
                    assert "3 attempts" in str(exc_info.value).lower()

                    # Verify it tried max_retries times
                    assert mock_catalog_instance.load_table.call_count == 3

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
