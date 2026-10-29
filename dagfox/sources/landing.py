"""A local landing zone standing in for the vendor's SFTP/S3 drop.

On MWAA this becomes an S3 prefix; only this module would change.
"""

from __future__ import annotations

import csv
import time
from pathlib import Path

from dagfox.db import ENTITY_FIELDS


class LandingZone:
    def __init__(self, root: Path):
        self.root = Path(root)

    def _marker(self, name: str) -> Path:
        return self.root / f".{name}.deliver_at"

    def deliver(self, name: str, rows: list[dict], deliver_at: float | None = None) -> Path:
        """Write a file. If ``deliver_at`` is set, it only becomes visible at that time."""
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / name
        if deliver_at is not None:
            self._marker(name).write_text(str(deliver_at))
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=ENTITY_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def poll(self, name: str) -> Path | None:
        """Return the file path if it has arrived, else None."""
        path, marker = self.root / name, self._marker(name)
        if not path.exists():
            return None
        if marker.exists():
            if time.time() < float(marker.read_text()):
                return None
            marker.unlink(missing_ok=True)
        return path

    @staticmethod
    def read(path: Path) -> list[dict]:
        with path.open(newline="") as f:
            return [{k: (v if v != "" else None) for k, v in r.items()} for r in csv.DictReader(f)]
