import logging
import sys
import traceback
from functools import wraps
from typing import Any, Callable

from google.api_core.exceptions import GoogleAPIError, PermissionDenied

from src import notification


def handle_google_api_exception(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except GoogleAPIError as exc:
            if isinstance(exc, PermissionDenied):
                logging.error(f"Permission denied. {exc.message}")
            else:
                message = getattr(exc, "message", "Unknown error occurred")
                logging.error(f"An error occurred: {message}")

    return wrapper


def handle_global_exception(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except (Exception, BaseException) as exc:
            logging.error(f"An unexpected error occurred: {exc}")
            error_message = traceback.format_exc()
            notification.send_message(
                f"An unexpected error occurred in Google Ads process:\n{error_message}"
            )
            sys.exit(1)

    return wrapper
