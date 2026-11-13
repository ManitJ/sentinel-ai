"""Persistent JSON checkpoint recording completed ingestion windows."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

CHECKPOINT_VERSION = 1


class CheckpointError(Exception):
    """Raised when a checkpoint file is unreadable or incompatible."""


class Checkpoint:
    """Tracks completed windows and the date_received watermark.

    Saved atomically (temp file + ``os.replace``) after every window so an
    interrupted run never leaves a half-written checkpoint.
    """

    def __init__(self, path):
        self.path = Path(path)
        self.data = {"version": CHECKPOINT_VERSION, "watermark": None, "windows": {}}
        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text())
            except (OSError, json.JSONDecodeError) as error:
                raise CheckpointError(f"Cannot read checkpoint {self.path}: {error}") from error
            if loaded.get("version") != CHECKPOINT_VERSION:
                raise CheckpointError(f"Unsupported checkpoint version in {self.path}")
            self.data = loaded

    @property
    def watermark(self):
        return self.data.get("watermark")

    def is_complete(self, window_key):
        return window_key in self.data["windows"]

    def mark_complete(self, window_key, records, file, max_date_received, duration_s):
        self.data["windows"][window_key] = {
            "records": records,
            "file": file,
            "max_date_received": max_date_received,
            "duration_s": duration_s,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        if max_date_received and (self.watermark is None or max_date_received > self.watermark):
            self.data["watermark"] = max_date_received
        self.save()

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".tmp")
        with open(tmp, "w") as handle:
            json.dump(self.data, handle, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.path)
