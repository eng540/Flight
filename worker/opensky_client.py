"""OpenSky Network API client – geo bounding box + robust retry logic."""
import httpx
import logging
import time
import os
from typing import List, Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class OpenSkyClient:
    """Client for the OpenSky Network REST API."""

    BASE_URL = "https://opensky-network.org/api"

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        rate_limit_delay: float = 10.0,
        max_retries: int = 3,
    ):
        self.username = username or os.getenv("OPENSKY_USERNAME")
        self.password = password or os.getenv("OPENSKY_PASSWORD")
        self.client_id = client_id or os.getenv("OPENSKY_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("OPENSKY_CLIENT_SECRET")
        self.max_retries = max_retries
        self.last_request_time: float = 0
        # Authenticated users get shorter delay
        self.rate_limit_delay = 2.0 if (self.username and self.password) else rate_limit_delay
        is_auth = bool(self.username and self.password)
        logger.info(f"OpenSkyClient initialised (authenticated={is_auth}, delay={self.rate_limit_delay}s)")

    def _auth(self):
        if self.username and self.password:
            return (self.username, self.password)
        return None

    def _wait(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()

    def _request(self, endpoint: str, params: Dict[str, Any]) -> Optional[Any]:
        url = f"{self.BASE_URL}/{endpoint}"
        for attempt in range(1, self.max_retries + 1):
            self._wait()
            try:
                logger.debug(f"GET {url} params={params} (attempt {attempt})")
                with httpx.Client(timeout=60.0) as client:
                    resp = client.get(url, params=params, auth=self._auth())

                if resp.status_code == 200:
                    data = resp.json()
                    logger.debug(f"OK {url} → {type(data)}")
                    return data

                if resp.status_code == 404:
                    logger.info(f"No data for {endpoint} (404)")
                    return None

                if resp.status_code == 401:
                    logger.warning("OpenSky: Unauthorised – check credentials")
                    return None

                if resp.status_code == 429:
                    wait = 60 * attempt
                    logger.warning(f"OpenSky rate limited – sleeping {wait}s")
                    time.sleep(wait)
                    continue

                logger.warning(f"HTTP {resp.status_code} for {url} attempt {attempt}")
                time.sleep(5 * attempt)

            except httpx.TimeoutException:
                logger.warning(f"Timeout attempt {attempt}: {url}")
                time.sleep(10 * attempt)
            except httpx.ConnectError as e:
                logger.warning(f"Connection error attempt {attempt}: {e}")
                time.sleep(10 * attempt)
            except Exception as e:
                logger.error(f"Unexpected error attempt {attempt}: {e}")
                time.sleep(5 * attempt)

        logger.error(f"All {self.max_retries} retries failed for {endpoint}")
        return None

    # ── Public methods ────────────────────────────────────────────────────────

    def get_flights_by_bounding_box(
        self,
        begin: int, end: int,
        lamin: float, lomin: float,
        lamax: float, lomax: float,
    ) -> List[Dict[str, Any]]:
        """
        Flights inside a bounding box for a time window.
        Uses /flights/area – max 2h (7200s) per call.
        """
        if end - begin > 7200:
            logger.warning("Window >2h passed to get_flights_by_bounding_box – trimming")
            end = begin + 7200

        params = {
            "begin": begin, "end": end,
            "lamin": lamin, "lomin": lomin,
            "lamax": lamax, "lomax": lomax,
        }
        logger.info(f"[opensky] /flights/area begin={begin} end={end} box=({lamin},{lomin},{lamax},{lomax})")
        data = self._request("flights/area", params)
        if data is None:
            return []
        result = data if isinstance(data, list) else []
        logger.info(f"[opensky] /flights/area → {len(result)} flights")
        return result

    def get_all_flights(self, begin: int, end: int) -> List[Dict[str, Any]]:
        """Global flights for a ≤2h window (no geo filter)."""
        if end - begin > 7200:
            end = begin + 7200
        logger.info(f"[opensky] /flights/all begin={begin} end={end}")
        data = self._request("flights/all", {"begin": begin, "end": end})
        result = data if isinstance(data, list) else []
        logger.info(f"[opensky] /flights/all → {len(result)} flights")
        return result

    def get_state_vectors(
        self,
        icao24: Optional[List[str]] = None,
        lamin: Optional[float] = None, lomin: Optional[float] = None,
        lamax: Optional[float] = None, lomax: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Current state vectors, optionally filtered by bounding box."""
        params: Dict[str, Any] = {}
        if icao24:
            params["icao24"] = [i.lower() for i in icao24]
        if all(v is not None for v in [lamin, lomin, lamax, lomax]):
            params.update({"lamin": lamin, "lomin": lomin,
                           "lamax": lamax, "lomax": lomax})
        result = self._request("states/all", params)
        return result if result else {}

    def get_flights_by_aircraft(self, icao24: str, begin: int, end: int
                                 ) -> List[Dict[str, Any]]:
        if end - begin > 172800:
            end = begin + 172800
        data = self._request(
            "flights/aircraft",
            {"icao24": icao24.lower(), "begin": begin, "end": end})
        return data if isinstance(data, list) else []

    def get_recent_flights(self, hours: int = 2) -> List[Dict[str, Any]]:
        end = int(datetime.utcnow().timestamp())
        begin = end - hours * 3600
        return self.get_all_flights(begin, end)

    def test_connection(self) -> bool:
        """Test connectivity to OpenSky API. Returns True if reachable."""
        try:
            with httpx.Client(timeout=15.0) as client:
                resp = client.get(f"{self.BASE_URL}/states/all",
                                  params={"lamin": 24, "lomin": 44, "lamax": 25, "lomax": 45},
                                  auth=self._auth())
            ok = resp.status_code in (200, 404)
            logger.info(f"[opensky] Connection test: HTTP {resp.status_code} → {'OK' if ok else 'FAIL'}")
            return ok
        except Exception as e:
            logger.error(f"[opensky] Connection test failed: {e}")
            return False
