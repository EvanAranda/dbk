from functools import wraps
from typing import Callable, NamedTuple

from textual.app import App as TextualApp
from textual.notifications import SeverityLevel
from textual.widget import Widget as TextualWidget


class ErrorInfo(NamedTuple):
    message: str
    title: str | None
    severity: SeverityLevel


ErrorInterpreter = Callable[[Exception], ErrorInfo]


class Message(Exception):
    def __init__(self, info: ErrorInfo):
        self.info = info

    @classmethod
    def inform(cls, message: str, title: str = ""):
        return cls(ErrorInfo(message, title, "information"))

    @classmethod
    def warn(cls, message: str, title: str = ""):
        return cls(ErrorInfo(message, title, "warning"))

    @classmethod
    def error(cls, message: str, title: str = ""):
        return cls(ErrorInfo(message, title, "error"))


def _extract_info(e: Exception) -> ErrorInfo:
    match e:
        case Message() as m:
            return m.info
        case _:
            return ErrorInfo(str(e), None, "error")


def use_error_handler(
    f=None,
    /,
    *,
    error_interpreter: ErrorInterpreter = _extract_info,
    app: TextualApp | None = None,
):
    def handle_error(f, *args, **kwargs):
        nonlocal app

        try:
            return f(*args, **kwargs)
        except Exception as e:
            if app is None:
                match args[0]:
                    case TextualApp() as app:
                        pass
                    case TextualWidget() as widget:
                        app = widget.app
                    case _:
                        raise RuntimeError("Could not find app to handle error.") from e
            err = error_interpreter(e)
            app.notify(err.message, title=err.title or "", severity=err.severity)

    def wrapper(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            return handle_error(f, *args, **kwargs)

        return wrapped

    if f is None:
        return wrapper

    return wrapper(f)
