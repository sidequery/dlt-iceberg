"""
Error handling and classification for PyIceberg operations.

Classifies exceptions into retryable and non-retryable categories
and provides utilities for structured error logging.
"""

import logging
import traceback
from typing import Optional, Type
from pyiceberg.exceptions import (
    CommitFailedException,
    CommitStateUnknownException,
    WaitingForLockException,
    ServiceUnavailableError,
    ServerError,
    AuthorizationExpiredError,
    UnauthorizedError,
    OAuthError,
    NoSuchTableError,
    NoSuchNamespaceError,
    NoSuchIdentifierError,
    TableAlreadyExistsError,
    ValidationError,
    BadRequestError,
    ForbiddenError,
    NotInstalledError,
    RESTError,
)

logger = logging.getLogger(__name__)


class ErrorCategory:
    """Categories for error classification."""
    RETRYABLE_TRANSIENT = "retryable_transient"
    RETRYABLE_AUTH = "retryable_auth"
    NON_RETRYABLE_CLIENT = "non_retryable_client"
    NON_RETRYABLE_SERVER = "non_retryable_server"
    NON_RETRYABLE_CONFIG = "non_retryable_config"


# Mapping of exception types to error categories
ERROR_CLASSIFICATION = {
    # Retryable transient errors - these indicate temporary issues
    CommitFailedException: ErrorCategory.RETRYABLE_TRANSIENT,
    WaitingForLockException: ErrorCategory.RETRYABLE_TRANSIENT,
    CommitStateUnknownException: ErrorCategory.RETRYABLE_TRANSIENT,
    ServiceUnavailableError: ErrorCategory.RETRYABLE_TRANSIENT,
    ServerError: ErrorCategory.RETRYABLE_TRANSIENT,

    # Retryable auth errors - credentials may have refreshed
    AuthorizationExpiredError: ErrorCategory.RETRYABLE_AUTH,

    # Non-retryable client errors - these indicate bugs or bad requests
    NoSuchTableError: ErrorCategory.NON_RETRYABLE_CLIENT,
    NoSuchNamespaceError: ErrorCategory.NON_RETRYABLE_CLIENT,
    NoSuchIdentifierError: ErrorCategory.NON_RETRYABLE_CLIENT,
    TableAlreadyExistsError: ErrorCategory.NON_RETRYABLE_CLIENT,
    ValidationError: ErrorCategory.NON_RETRYABLE_CLIENT,
    BadRequestError: ErrorCategory.NON_RETRYABLE_CLIENT,

    # Non-retryable server errors - these indicate configuration issues
    ForbiddenError: ErrorCategory.NON_RETRYABLE_SERVER,
    UnauthorizedError: ErrorCategory.NON_RETRYABLE_SERVER,
    OAuthError: ErrorCategory.NON_RETRYABLE_SERVER,

    # Non-retryable config errors - missing dependencies
    NotInstalledError: ErrorCategory.NON_RETRYABLE_CONFIG,
}


def is_retryable_error(exception: Exception) -> bool:
    """
    Determine if an exception should be retried.

    Args:
        exception: The exception to classify

    Returns:
        True if the error is retryable, False otherwise
    """
    exception_type = type(exception)
    category = ERROR_CLASSIFICATION.get(exception_type)

    if category is None:
        # Unknown error - check if it's a RESTError or generic exception
        if isinstance(exception, RESTError):
            # RESTError is a catch-all, so we conservatively retry
            logger.warning(f"Unknown RESTError encountered: {exception}")
            return True
        # For truly unknown errors, don't retry (fail fast)
        return False

    return category in {
        ErrorCategory.RETRYABLE_TRANSIENT,
        ErrorCategory.RETRYABLE_AUTH,
    }


def get_error_category(exception: Exception) -> str:
    """
    Get the category of an exception.

    Args:
        exception: The exception to classify

    Returns:
        Error category string
    """
    exception_type = type(exception)
    category = ERROR_CLASSIFICATION.get(exception_type)

    if category is None:
        if isinstance(exception, RESTError):
            return "unknown_rest_error"
        return "unknown_error"

    return category


def log_error_with_context(
    exception: Exception,
    operation: str,
    table_name: Optional[str] = None,
    attempt: Optional[int] = None,
    max_attempts: Optional[int] = None,
    include_traceback: bool = False,
) -> None:
    """
    Log an error with structured context.

    Args:
        exception: The exception that occurred
        operation: Description of the operation being performed
        table_name: Name of the table being operated on
        attempt: Current attempt number (if retrying)
        max_attempts: Maximum number of attempts
        include_traceback: Whether to include full stack trace
    """
    error_type = type(exception).__name__
    category = get_error_category(exception)
    retryable = is_retryable_error(exception)

    context = {
        "error_type": error_type,
        "error_category": category,
        "retryable": retryable,
        "operation": operation,
    }

    if table_name:
        context["table_name"] = table_name

    if attempt is not None and max_attempts is not None:
        context["attempt"] = f"{attempt}/{max_attempts}"

    # Format context for logging
    context_str = ", ".join(f"{k}={v}" for k, v in context.items())

    if retryable:
        if attempt is not None and attempt >= max_attempts:
            logger.error(
                f"Operation failed after max retries: {exception} ({context_str})"
            )
            if include_traceback:
                logger.error(f"Stack trace:\n{traceback.format_exc()}")
        else:
            logger.warning(
                f"Retryable error encountered: {exception} ({context_str})"
            )
    else:
        logger.error(
            f"Non-retryable error encountered: {exception} ({context_str})"
        )
        if include_traceback:
            logger.error(f"Stack trace:\n{traceback.format_exc()}")


def get_user_friendly_error_message(exception: Exception, operation: str) -> str:
    """
    Generate a user-friendly error message.

    Args:
        exception: The exception that occurred
        operation: Description of the operation being performed

    Returns:
        User-friendly error message
    """
    error_type = type(exception).__name__
    category = get_error_category(exception)

    # Base message
    message = f"Failed to {operation}: {exception}"

    # Add helpful hints based on category
    if category == ErrorCategory.NON_RETRYABLE_CLIENT:
        if isinstance(exception, NoSuchTableError):
            message += "\nHint: The table does not exist. Create it first or use write_disposition='append' with auto-create."
        elif isinstance(exception, NoSuchNamespaceError):
            message += "\nHint: The namespace does not exist. It will be created automatically on first write."
        elif isinstance(exception, ValidationError):
            message += "\nHint: There is a schema validation error. Check that your data types match the table schema."
        elif isinstance(exception, TableAlreadyExistsError):
            message += "\nHint: The table already exists. Use write_disposition='append' or 'merge' instead of 'replace'."

    elif category == ErrorCategory.NON_RETRYABLE_SERVER:
        if isinstance(exception, (UnauthorizedError, ForbiddenError)):
            message += "\nHint: Check your credentials and permissions."
        elif isinstance(exception, OAuthError):
            message += "\nHint: OAuth authentication failed. Verify oauth2_server_uri and credentials."

    elif category == ErrorCategory.NON_RETRYABLE_CONFIG:
        if isinstance(exception, NotInstalledError):
            message += "\nHint: A required optional dependency is missing. Install it with: uv add <package>"

    elif category == ErrorCategory.RETRYABLE_TRANSIENT:
        if isinstance(exception, CommitFailedException):
            message += "\nNote: This is typically caused by concurrent writes. The operation will be retried."
        elif isinstance(exception, ServiceUnavailableError):
            message += "\nNote: The service is temporarily unavailable. The operation will be retried."

    return message
