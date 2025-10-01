import os
import time
import random
import requests
from typing import Dict, Optional, Any
from urllib.parse import urlparse, parse_qs
from requests.packages import urllib3
import ssl


from .. import aaa
from .. import log

ssl._create_default_https_context = ssl._create_unverified_context
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class BaseDDSClient:
    """
    Base class: AAA auth, logging, domain, HTTP retries, and shared helpers.
    """

    def __init__(self, username: str, password: str, environment: str = "prod"):
        self.domain = "https://www.eodms-sgdot.nrcan-rncan.gc.ca"
        if environment == "staging":
            # honour DOMAIN env for staging
            self.domain = os.environ.get("DOMAIN", self.domain)

        self.logger = log._EODMSLogger("EODMS_DSS", log.eodms_logger)
        self.aaa = aaa.AAA_API(username, password, environment)

        self.img_info: Optional[Dict[str, Any]] = None

    # ---------- retry/backoff ----------
    def _sleep_for_retry(self, resp, attempt: int, backoff_factor: float, max_backoff: float) -> None:
        """
        Sleep using Retry-After (seconds) if present on 429; else exponential backoff + jitter.
        """
        delay: Optional[float] = None
        if resp is not None and getattr(resp, "status_code", None) == 429:
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    delay = float(ra)
                except ValueError:
                    delay = None
        if delay is None:
            delay = min(max_backoff, backoff_factor * (2 ** (attempt - 1)))
        delay *= 0.5 + 0.5 * random.random()  # jitter
        time.sleep(delay)

    def _request_with_retries(
        self,
        url: str,
        headers: Dict[str, str],
        *,
        max_retries: int = 5,
        backoff_factor: float = 1.0,
        max_backoff: float = 30.0,
    ):
        """
        Perform GET via AAA.prepare_request with retries on 429/5xx.
        """
        resp = None
        for attempt in range(1, max_retries + 1):
            resp = self.aaa.prepare_request(url, headers=headers)
            code = getattr(resp, "status_code", None)
            if code in (200, 202):
                return resp
            if code == 429 or (code and 500 <= code < 600):
                if attempt < max_retries:
                    self.logger.debug(f"Retrying {url} (attempt {attempt}, status {code})")
                    self._sleep_for_retry(resp, attempt, backoff_factor, max_backoff)
                    continue
            return resp
        return resp

    # ---------- helpers ----------
    def _parse_expected_size(self, url: str, resp: Optional[requests.Response]) -> Optional[int]:
        """Get expected size from '?size=' or Content-Length."""
        try:
            qs = parse_qs(urlparse(url).query)
            if "size" in qs and qs["size"]:
                return int(qs["size"][0])
        except Exception:
            pass
        try:
            if resp is not None:
                cl = resp.headers.get("Content-Length")
                if cl:
                    return int(cl)
        except Exception:
            pass
        return None

    def _safe_filename(self, url: str, out_dir: str) -> str:
        """Dest path from URL; ensures directory exists."""
        os.makedirs(out_dir, exist_ok=True)
        name = os.path.basename(urlparse(url).path) or "download.bin"
        return os.path.join(out_dir, name)