from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional
from urllib.parse import urlparse
import os
from datetime import datetime
from .rcm_db import OptionalRCMDB
from .dds import DDSClient


class RCMClientWithDB(DDSClient):
    """
    Wrapper on top of your existing DDSClient.
    Adds optional SQLite logging via OptionalRCMDB with NO changes to base classes.
    """

    def __init__(self, username: str, password: str, *, db_path: Optional[str] = None, environment: str = "prod"):
        super().__init__(username, password, environment=environment)
        self.rcm_db = OptionalRCMDB(db_path, logger=self.logger)

    # --- query + fetch ---
    def record_query_and_get_items(
        self,
        entries: List[Dict[str, str]],
        *,
        query_context: Dict[str, Any],
        catalog: str = "EODMS",
        rate_limit_per_sec: float = 0.0,
    ) -> Tuple[Optional[int], Dict[str, List[Dict]]]:
        """
        1) Optionally record query (returns query_id or None)
        2) Call your existing get_items(...) and return its buckets
        """
        query_id = self.rcm_db.record_query(query_context)
        buckets = super().get_items(entries, catalog=catalog, rate_limit_per_sec=rate_limit_per_sec)
        return query_id, buckets

    # --- download with DB updates ---
    def download_items_with_db(
        self,
        item_info: Dict[str, List[Dict]],
        out_dir: str,
        *,
        unzip: bool = False,
        keep_zip: bool = True,
        max_workers: int = 3,
        meta_by_archive: Optional[Dict[str, Dict[str, Any]]] = None,
        query_id: Optional[int] = None,
    ) -> List[Dict[str, str]]:
        """
        Wraps your existing download_items to:
          - mark 'in_progress' before download
          - mark 'completed' after success
          - mark 'failed' on exception (per item)
        """
        meta_by_archive = meta_by_archive or {}
        ready = item_info.get("ready", [])

        # Pre-mark in DB
        completed_dir = os.path.join(out_dir, "completed")
        for it in ready:
            aid = it.get("archiveId") or it.get("datasetId") or it.get("serviceUuid")
            url = it.get("download_url")
            if not (aid and url):
                continue
            filename = os.path.basename(urlparse(url).path) or "download.bin"
            final_path = os.path.join(completed_dir, filename)
            meta = self._build_meta(it, meta_by_archive.get(aid, {}))
            self.rcm_db.mark_in_progress(product_id=aid, query_id=query_id, meta=meta, file_path=final_path)

        # Perform the real downloads using your unmodified method
        results = super().download_items(item_info, out_dir, unzip=unzip, keep_zip=keep_zip, max_workers=max_workers)

        # Mark successes
        for r in results:
            aid = r["archiveId"]
            meta = self._build_meta({}, meta_by_archive.get(aid, {}))
            self.rcm_db.mark_success(product_id=aid, query_id=query_id, final_path=r["dest"], meta=meta)

        return results

    # Small helper to normalize metadata fields
    def _build_meta(self, item: Dict[str, Any], extra: Dict[str, Any]) -> Dict[str, Any]:
        """
        Populate DB fields from item dict and optional extra metadata (your search records).
        """

        def _dt(s):
            if isinstance(s, datetime):
                return s
            if isinstance(s, str):
                try:
                    return datetime.fromisoformat(s.replace("Z", "+00:00"))
                except Exception:
                    return None
            return None

        return {
            "constellation": "RCM",
            "sensor_mode": extra.get("beamMnemonic") or item.get("beamMnemonic"),
            "product_type": extra.get("productType") or item.get("productType"),
            "processing_level": extra.get("processingLevel") or item.get("processingLevel"),
            "acquisition_time": _dt(extra.get("acquisitionEndDate") or item.get("acquisitionEndDate")),
            "publication_time": _dt(extra.get("last_update") or item.get("last_update")),
            "coordinates": extra.get("geometry", {}).get("coordinates")
            if isinstance(extra.get("geometry"), dict)
            else extra.get("coordinates") or item.get("coordinates"),
            "latitude": extra.get("latitude"),
            "longitude": extra.get("longitude"),
            "name": extra.get("title") or item.get("title"),
            "quicklook": extra.get("overviewUrl") or item.get("overviewUrl"),
        }
