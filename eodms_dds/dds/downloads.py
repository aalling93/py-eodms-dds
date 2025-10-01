import os
import time
import zipfile
import random
import requests
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm.auto import tqdm
from .base import BaseDDSClient


class DDSDownload(BaseDDSClient):
    """
    Parallel downloader: in_progress -> completed; optional unzip.
    """

    def _download_with_retries(
        self,
        url: str,
        dest: str,
        *,
        max_retries: int = 5,
        backoff_factor: float = 1.0,
        max_backoff: float = 20.0,
        chunk: int = 1 << 20,
        pbar: Optional[tqdm] = None,
    ) -> str:
        """
        Stream download to dest.part then rename to dest. Returns dest on success.
        """
        tmp = dest + ".part"
        for attempt in range(1, max_retries + 1):
            try:
                with requests.get(url, stream=True, timeout=60, verify=False) as r:
                    if r.status_code == 429 or 500 <= r.status_code < 600:
                        if attempt < max_retries:
                            delay = min(max_backoff, backoff_factor * (2 ** (attempt - 1)))
                            delay *= 0.5 + 0.5 * random.random()
                            time.sleep(delay)
                            continue
                    r.raise_for_status()

                    expected = self._parse_expected_size(url, r)
                    if pbar is not None and expected and not pbar.total:
                        pbar.total = expected
                        pbar.refresh()

                    with open(tmp, "wb") as f:
                        for chunk_bytes in r.iter_content(chunk_size=chunk):
                            if not chunk_bytes:
                                continue
                            f.write(chunk_bytes)
                            if pbar is not None:
                                pbar.update(len(chunk_bytes))

                #  size check
                exp = self._parse_expected_size(url, None)
                if exp is not None:
                    actual = os.path.getsize(tmp)
                    if actual != exp:
                        raise IOError(f"Size mismatch: expected {exp}, got {actual}")

                os.replace(tmp, dest)
                return dest
            except Exception:
                # cleanup partial
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except Exception:
                        pass
                if attempt >= max_retries:
                    raise
                delay = min(max_backoff, backoff_factor * (2 ** (attempt - 1)))
                delay *= 0.5 + 0.5 * random.random()
                time.sleep(delay)
        return dest  # unreachable

    def _maybe_unzip(self, file_path: str, out_dir: str, *, keep_zip: bool = True) -> Optional[str]:
        """Unzip to out_dir/name/, optionally delete zip."""
        if not file_path.lower().endswith(".zip"):
            return None
        extract_dir = os.path.join(out_dir, os.path.splitext(os.path.basename(file_path))[0])
        os.makedirs(extract_dir, exist_ok=True)
        with zipfile.ZipFile(file_path, "r") as zf:
            zf.extractall(extract_dir)
        if not keep_zip:
            try:
                os.remove(file_path)
            except Exception:
                pass
        return extract_dir

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
        Download all 'Available' items from get_items() into:
          out_dir/in_progress/<file>  -> on success -> out_dir/completed/<file>
        Optionally unzip inside 'completed'. Returns list of {'archiveId','dest','extracted_dir'}.
        """
        ready = item_info.get("ready", [])
        tasks: List[Tuple[str, str]] = []
        for it in ready:
            url = it.get("download_url")
            aid = it.get("archiveId") or it.get("datasetId") or it.get("serviceUuid") or "unknown"
            if url:
                tasks.append((url, aid))
        if not tasks:
            self.logger.info("No downloadable items in 'ready'.")
            return []

        # prepare dirs
        in_progress_dir = os.path.join(out_dir, "in_progress")
        completed_dir = os.path.join(out_dir, "completed")
        os.makedirs(in_progress_dir, exist_ok=True)
        os.makedirs(completed_dir, exist_ok=True)

        bars: Dict[int, tqdm] = {}
        results: List[Dict[str, str]] = []

        def _expected_size(url: str) -> Optional[int]:
            try:
                qs = parse_qs(urlparse(url).query)
                if "size" in qs and qs["size"]:
                    return int(qs["size"][0])
            except Exception:
                pass
            return None

        def job(idx: int, url: str, aid: str):
            filename = os.path.basename(urlparse(url).path) or "download.bin"
            tmp_path = os.path.join(in_progress_dir, filename)
            fin_path = os.path.join(completed_dir, filename)

            exp = _expected_size(url)
            # skip if already complete
            try:
                if exp is not None and os.path.exists(fin_path) and os.path.getsize(fin_path) == exp:
                    extracted_dir = None
                    if unzip and fin_path.lower().endswith(".zip"):
                        extracted_dir = self._maybe_unzip(fin_path, completed_dir, keep_zip=keep_zip)
                    return {"archiveId": aid, "dest": fin_path, "extracted_dir": extracted_dir}
            except Exception:
                pass

            bars[idx] = tqdm(
                total=exp or 0,
                unit="B",
                unit_scale=True,
                desc=f"[{idx+1}/{len(tasks)}] {filename}",
                position=idx,
                leave=False,
                dynamic_ncols=True,
            )

            try:
                # download to in_progress
                final_tmp = self._download_with_retries(url, tmp_path, pbar=bars[idx])
                # move to completed
                os.replace(final_tmp, fin_path)

                extracted_dir = None
                if unzip and fin_path.lower().endswith(".zip"):
                    extracted_dir = self._maybe_unzip(fin_path, completed_dir, keep_zip=keep_zip)
                return {"archiveId": aid, "dest": fin_path, "extracted_dir": extracted_dir}
            finally:
                try:
                    bars[idx].close()
                except Exception:
                    pass

        futures = []
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for i, (url, aid) in enumerate(tasks):
                futures.append(ex.submit(job, i, url, aid))
            for fut in as_completed(futures):
                try:
                    res = fut.result()
                    if res:
                        results.append(res)
                except Exception as e:
                    self.logger.error(f"Download failed: {e}")

        tqdm.write(f"Downloaded {len(results)}/{len(tasks)} items to {os.path.join(out_dir, 'completed')}")
        return results