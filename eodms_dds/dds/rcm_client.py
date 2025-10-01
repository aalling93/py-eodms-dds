from __future__ import annotations
from typing import Dict, List, Optional
from urllib.parse import urlparse
from datetime import datetime
import os

from .dds import DDSClient
from .rcm_db import OptionalRCMDB


class RCMClientWithDB(DDSClient):
    """
    Drop-in replacement for DDSClient that auto-writes to DB.
    If DB is missing/invalid, all DB ops no-op.
    """

    def __init__(self, username: str, password: str, *, db_path: Optional[str] = None, environment: str = "prod"):
        super().__init__(username, password, environment=environment)
        self.rcm_db = OptionalRCMDB(db_path, logger=self.logger)

    def download_items(
        self,
        item_info: Dict[str, List[Dict]],
        out_dir: str,
        *,
        unzip: bool = False,
        keep_zip: bool = True,
        max_workers: int = 3,
    ) -> List[Dict[str, str]]:
        """
        Wrap parent download_items:
          - pre-mark in DB as 'in_progress'
          - call parent
          - mark successes
          - mark failures (best-effort: those not returned)
        """
        ready = item_info.get("ready", [])
        completed_dir = os.path.join(out_dir, "completed")

        def _dt(s):
            if isinstance(s, datetime):
                return s
            if isinstance(s, str):
                try:
                    return datetime.fromisoformat(s.replace("Z", "+00:00"))
                except Exception:
                    return None
            return None

        for it in ready:
            aid = it.get("archiveId") or it.get("datasetId") or it.get("serviceUuid")
            url = it.get("download_url")
            if not (aid and url):
                continue
            filename = os.path.basename(urlparse(url).path) or "download.bin"
            final_path = os.path.join(completed_dir, filename)
            meta = {
                "constellation": "RCM",
                "sensor_mode": it.get("beamMnemonic"),
                "product_type": it.get("productType"),
                "processing_level": it.get("processingLevel"),
                "acquisition_time": _dt(it.get("acquisitionEndDate")),
                "publication_time": _dt(it.get("last_update")),
                "coordinates": it.get("geometry", {}).get("coordinates") if isinstance(it.get("geometry"), dict) else it.get("coordinates"),
                "latitude": None,
                "longitude": None,
                "name": it.get("title"),
                "quicklook": it.get("overviewUrl"),
            }
            self.rcm_db.mark_in_progress(product_id=aid, query_id=None, meta=meta, file_path=final_path)

        # Call the original downloader
        results = super().download_items(item_info, out_dir, unzip=unzip, keep_zip=keep_zip, max_workers=max_workers)

        # Mark successes
        succeeded = set()
        for r in results:
            aid = r["archiveId"]
            succeeded.add(aid)
            #
            meta = {
                "constellation": "RCM",
                "sensor_mode": None,
                "product_type": None,
                "processing_level": None,
                "acquisition_time": None,
                "publication_time": None,
                "coordinates": None,
                "latitude": None,
                "longitude": None,
                "name": None,
                "quicklook": None,
            }
            self.rcm_db.mark_success(product_id=aid, query_id=None, final_path=r["dest"], meta=meta)

        # Best-effort mark failures (those we pre-marked but didn’t succeed)
        for it in ready:
            aid = it.get("archiveId") or it.get("datasetId") or it.get("serviceUuid")
            if not aid or aid in succeeded:
                continue
            url = it.get("download_url") or ""
            filename = os.path.basename(urlparse(url).path) or "download.bin"
            final_path = os.path.join(completed_dir, filename)
            meta = {"constellation": "RCM"}  # minimal
            self.rcm_db.mark_failed(product_id=aid, query_id=None, final_path=final_path, meta=meta, err=Exception("download failed"))

        return results
