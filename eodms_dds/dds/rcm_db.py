from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime
from .db_utils import to_iso, md5_file, json_dumps_safe

_EXPECTED_QUERY_TABLE = "queries"
_EXPECTED_QUERY_COLS = {"id", "constellation", "geometry_wkt", "start_date", "end_date", "parameters", "created_at"}

_EXPECTED_DL_TABLE = "downloads"
_EXPECTED_DL_COLS = {
    "product_id",
    "query_id",
    "constellation",
    "sensor_mode",
    "product_type",
    "processing_level",
    "status",
    "acqusition_time",
    "publication_time",
    "latency",
    "coordinates",
    "latitude",
    "longitude",
    "name",
    "quicklook",
    "file_path",
    "file_size_mb",
    "checksum",
    "created_at",
    "updated_at",
}


def _subset_present(conn: sqlite3.Connection, table: str, need: set[str]) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    cols = {row[1] for row in cur.fetchall()}
    return need.issubset(cols)


class OptionalRCMDB:
    """
    Optional SQLite helper. Writes only if DB file + expected tables exist.
    Otherwise every method no-ops.
    """

    def __init__(self, db_path: Optional[str | Path], logger=None):
        self.logger = logger
        self.enabled = False
        self.conn: Optional[sqlite3.Connection] = None
        if not db_path:
            return
        p = Path(db_path)
        if not p.exists():
            return
        try:
            conn = sqlite3.connect(str(p))
            conn.row_factory = sqlite3.Row
            if not _subset_present(conn, _EXPECTED_QUERY_TABLE, _EXPECTED_QUERY_COLS):
                if self.logger:
                    self.logger.debug("RCM DB disabled: queries table missing/invalid")
                conn.close()
                return
            if not _subset_present(conn, _EXPECTED_DL_TABLE, _EXPECTED_DL_COLS):
                if self.logger:
                    self.logger.debug("RCM DB disabled: downloads table missing/invalid")
                conn.close()
                return
            self.conn = conn
            self.enabled = True
            if self.logger:
                self.logger.debug(f"RCM DB enabled at {p}")
        except Exception as e:
            if self.logger:
                self.logger.debug(f"RCM DB disabled: cannot open sqlite: {e}")

    def record_query(self, payload: Dict[str, Any]) -> Optional[int]:
        if not self.enabled:
            return None
        try:
            cur = self.conn.execute(
                f"""INSERT INTO {_EXPECTED_QUERY_TABLE}
                    (constellation, geometry_wkt, start_date, end_date, parameters, created_at)
                    VALUES (?,?,?,?,?,datetime('now'))""",
                (
                    payload.get("constellation", "RCM"),
                    payload.get("geometry_wkt"),
                    payload.get("start_date"),
                    payload.get("end_date"),
                    json_dumps_safe(payload.get("parameters", {})),
                ),
            )
            self.conn.commit()
            return int(cur.lastrowid)
        except Exception as e:
            if self.logger:
                self.logger.debug(f"record_query skipped: {e}")
            return None

    def mark_in_progress(self, *, product_id: str, query_id: Optional[int], meta: Dict[str, Any], file_path: str) -> None:
        if not self.enabled:
            return
        try:
            self.conn.execute(
                f"""INSERT INTO {_EXPECTED_DL_TABLE} (
                    product_id, query_id, constellation, sensor_mode, product_type, processing_level, status,
                    acqusition_time, publication_time, latency, coordinates, latitude, longitude, name, quicklook,
                    file_path, file_size_mb, checksum, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),datetime('now'))
                ON CONFLICT(product_id) DO UPDATE SET
                    status=excluded.status, file_path=excluded.file_path, updated_at=datetime('now')""",
                (
                    product_id,
                    query_id,
                    meta.get("constellation", "RCM"),
                    meta.get("sensor_mode"),
                    meta.get("product_type"),
                    meta.get("processing_level"),
                    "in_progress",
                    to_iso(meta.get("acquisition_time")),
                    to_iso(meta.get("publication_time")),
                    None,
                    json_dumps_safe(meta.get("coordinates")),
                    meta.get("latitude"),
                    meta.get("longitude"),
                    meta.get("name"),
                    meta.get("quicklook"),
                    file_path,
                    None,
                    None,
                ),
            )
            self.conn.commit()
        except Exception as e:
            if self.logger:
                self.logger.debug(f"mark_in_progress skipped: {e}")

    def mark_success(self, *, product_id: str, query_id: Optional[int], final_path: str, meta: Dict[str, Any]) -> None:
        if not self.enabled:
            return
        try:
            p = Path(final_path)
            size_mb = p.stat().st_size / (1024 * 1024) if p.exists() else None
            checksum = md5_file(p) if p.exists() else None
            acq = meta.get("acquisition_time")
            pub = meta.get("publication_time")
            latency = None
            try:
                if isinstance(acq, datetime) and isinstance(pub, datetime):
                    latency = (pub - acq).total_seconds() / 60.0
            except Exception:
                pass

            self.conn.execute(
                f"""INSERT INTO {_EXPECTED_DL_TABLE} (
                    product_id, query_id, constellation, sensor_mode, product_type, processing_level, status,
                    acqusition_time, publication_time, latency, coordinates, latitude, longitude, name, quicklook,
                    file_path, file_size_mb, checksum, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),datetime('now'))
                ON CONFLICT(product_id) DO UPDATE SET
                    status='completed', file_path=excluded.file_path, file_size_mb=excluded.file_size_mb,
                    checksum=excluded.checksum, publication_time=excluded.publication_time,
                    acqusition_time=excluded.acqusition_time, latency=excluded.latency, updated_at=datetime('now')""",
                (
                    product_id,
                    query_id,
                    meta.get("constellation", "RCM"),
                    meta.get("sensor_mode"),
                    meta.get("product_type"),
                    meta.get("processing_level"),
                    "completed",
                    to_iso(acq),
                    to_iso(pub),
                    latency,
                    json_dumps_safe(meta.get("coordinates")),
                    meta.get("latitude"),
                    meta.get("longitude"),
                    meta.get("name"),
                    meta.get("quicklook"),
                    str(p),
                    size_mb,
                    checksum,
                ),
            )
            self.conn.commit()
        except Exception as e:
            if self.logger:
                self.logger.debug(f"mark_success skipped: {e}")

    def mark_failed(self, *, product_id: str, query_id: Optional[int], final_path: str, meta: Dict[str, Any], err: Exception) -> None:
        if not self.enabled:
            return
        try:
            self.conn.execute(
                f"""INSERT INTO {_EXPECTED_DL_TABLE} (
                    product_id, query_id, constellation, sensor_mode, product_type, processing_level, status,
                    acqusition_time, publication_time, latency, coordinates, latitude, longitude, name, quicklook,
                    file_path, file_size_mb, checksum, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),datetime('now'))
                ON CONFLICT(product_id) DO UPDATE SET
                    status='failed', file_path=excluded.file_path, updated_at=datetime('now')""",
                (
                    product_id,
                    query_id,
                    meta.get("constellation", "RCM"),
                    meta.get("sensor_mode"),
                    meta.get("product_type"),
                    meta.get("processing_level"),
                    "failed",
                    to_iso(meta.get("acquisition_time")),
                    to_iso(meta.get("publication_time")),
                    None,
                    json_dumps_safe(meta.get("coordinates")),
                    meta.get("latitude"),
                    meta.get("longitude"),
                    meta.get("name"),
                    meta.get("quicklook"),
                    final_path,
                    None,
                    None,
                ),
            )
            self.conn.commit()
        except Exception as e:
            if self.logger:
                self.logger.debug(f"mark_failed skipped: {e}")
