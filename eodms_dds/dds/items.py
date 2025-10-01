import time

from typing import Protocol, runtime_checkable, Optional, Dict, Any, Iterable, Tuple, List
from datetime import timedelta
from time import perf_counter
from tqdm.auto import tqdm

from .base import BaseDDSClient


@runtime_checkable
class QueryResultLike(Protocol):
    uuid: Optional[str]
    query_data: Optional[Dict[str, Any]]


class DDSGetItems(BaseDDSClient):
    """
    Item retrieval: single + batch with ETA and optional rate limiting.
    """

    def get_item(self, collection: str, item_uuid: str, catalog: str = "EODMS") -> Optional[Dict[str, Any]]:
        """
        Get single item JSON. Returns None on failure.
        """
        url = f"{self.domain}/dds/v1/item/{catalog}/{collection}/{item_uuid}"
        access_token = self.aaa.get_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}
        resp = self._request_with_retries(url, headers=headers)

        if resp is None:
            self.logger.error("No response from DDS API")
            return None

        if resp.status_code == 200:
            try:
                self.img_info = resp.json()
                return self.img_info
            except Exception:
                return None
        if resp.status_code == 202:
            try:
                self.img_info = resp.json()
                return self.img_info
            except Exception:
                return None

        # else: log server error if JSON
        try:
            ej = resp.json()
            self.logger.error(f"Failed to get item: {ej.get('error')}: {ej.get('message')}")
        except Exception:
            self.logger.error("Failed to get item (non-JSON error).")
        return None

    def get_items_from_results(
        self,
        results: Iterable[QueryResultLike],  # <- no import of QueryResult needed
        catalog: str = "EODMS",
        rate_limit_per_sec: float = 0.0,
    ) -> Dict[str, List[Dict]]:
        items = tuple(results)
        n_total = len(items)

        ready: List[Dict] = []
        queued: List[Dict] = []
        unknown: List[Dict] = []

        def _resolve_ids(qr: QueryResultLike) -> Optional[Tuple[str, str]]:
            qd = qr.query_data or {}
            coll = None
            for k in ("collectionId", "collectionID", "collection", "collection_name"):
                if qd.get(k):
                    coll = str(qd[k])
                    break
            arch = None
            for k in ("archiveId", "datasetId", "recordId", "featureId"):
                if qd.get(k):
                    arch = str(qd[k])
                    break
            if arch is None and qr.uuid:
                arch = str(qr.uuid)
            return (coll, arch) if (coll and arch) else None

        total_elapsed = 0.0
        n_done = 0
        min_interval = 1.0 / rate_limit_per_sec if rate_limit_per_sec > 0 else 0.0
        last_ts = 0.0

        def _fmt_eta(s: float) -> str:
            return str(timedelta(seconds=int(max(0, s))))

        with tqdm(total=n_total, desc="DDS get_items", unit="item", leave=True) as pbar:
            for qr in items:
                if min_interval > 0:
                    now = time.perf_counter()
                    wait = (last_ts + min_interval) - now
                    if wait > 0:
                        time.sleep(wait)

                t0 = perf_counter()
                try:
                    ids = _resolve_ids(qr)
                    if not ids:
                        if hasattr(self, "logger"):
                            self.logger.warning(f"Skipping item (missing collectionId/archiveId): {getattr(qr, 'uuid', 'NO-UUID')}")
                        continue

                    collection_id, archive_id = ids
                    data = self.get_item(collection_id, archive_id, catalog=catalog)

                    if data is not None:
                        st = data.get("status")
                        if st == "Available":
                            ready.append(data)
                        elif st == "Queued":
                            queued.append(data)
                        else:
                            unknown.append(data)
                    else:
                        if hasattr(self, "logger"):
                            self.logger.warning(f"Skipping ({collection_id}, {archive_id}) (fetch failure).")
                finally:
                    elapsed = perf_counter() - t0
                    total_elapsed += elapsed
                    n_done += 1
                    last_ts = time.perf_counter()

                    avg = total_elapsed / max(1, n_done)
                    eta = avg * (n_total - n_done)
                    pbar.set_postfix({"ready": len(ready), "queued": len(queued), "unknown": len(unknown), "eta": _fmt_eta(eta)})
                    pbar.update(1)

        return {"ready": ready, "queued": queued, "unknown": unknown}

    def get_items(
        self,
        entries: Iterable[Dict[str, str]],
        catalog: str = "EODMS",
        rate_limit_per_sec: float = 0.0,
    ) -> Dict[str, List[Dict]]:
        """
        Fetch many items serially with one tqdm bar + ETA.
        Entries must be dicts with 'collectionId' and 'archiveId'.
        Returns buckets: {'ready': [...], 'queued': [...], 'unknown': [...]}

        TODO: make this parallel to faster get items.. 
        """
        items = list(entries)
        n_total = len(items)

        ready: List[Dict] = []
        queued: List[Dict] = []
        unknown: List[Dict] = []

        total_elapsed = 0.0
        n_done = 0
        min_interval = 1.0 / rate_limit_per_sec if rate_limit_per_sec > 0 else 0.0
        last_ts = 0.0

        def _fmt_eta(s: float) -> str:
            return str(timedelta(seconds=int(max(0, s))))

        with tqdm(total=n_total, desc="DDS get_items", unit="item", leave=True) as pbar:
            for e in items:
                if min_interval > 0:
                    now = time.perf_counter()
                    wait = (last_ts + min_interval) - now
                    if wait > 0:
                        time.sleep(wait)

                t0 = perf_counter()
                try:
                    data = self.get_item(e["collectionId"], e["archiveId"], catalog=catalog)
                    if data is not None:
                        st = data.get("status")
                        if st == "Available":
                            ready.append(data)
                        elif st == "Queued":
                            queued.append(data)
                        else:
                            unknown.append(data)
                    else:
                        self.logger.warning(f"Skipping {e} (fetch failure).")
                finally:
                    elapsed = perf_counter() - t0
                    total_elapsed += elapsed
                    n_done += 1
                    last_ts = time.perf_counter()

                    avg = total_elapsed / max(1, n_done)
                    eta = avg * (n_total - n_done)
                    pbar.set_postfix({"ready": len(ready), "queued": len(queued), "unknown": len(unknown), "eta": _fmt_eta(eta)})
                    pbar.update(1)

        return {"ready": ready, "queued": queued, "unknown": unknown}
