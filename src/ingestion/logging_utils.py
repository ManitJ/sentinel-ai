"""Structured logging: human-readable console output plus JSON-lines file."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path


def log_event(logger, level, event, **fields):
    """Log ``event`` with structured key/value ``fields``."""
    logger.log(level, event, extra={"fields": fields})


class JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        entry.update(getattr(record, "fields", {}))
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


class ConsoleFormatter(logging.Formatter):
    def format(self, record):
        fields = getattr(record, "fields", {})
        kv = " ".join(f"{key}={value}" for key, value in fields.items())
        ts = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        return f"{ts} {record.levelname:<7} {record.getMessage()} {kv}".rstrip()


def configure_logging(log_dir=None, level="INFO"):
    """Configure root logging; returns the JSON log file path, if any."""
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # handlers filter; the JSON file keeps DEBUG events
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    for handler in list(root.handlers):
        root.removeHandler(handler)

    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(ConsoleFormatter())
    root.addHandler(console)

    if log_dir is None:
        return None

    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = log_dir / f"ingest_{stamp}.jsonl"
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(JsonFormatter())
    root.addHandler(file_handler)
    return log_path
