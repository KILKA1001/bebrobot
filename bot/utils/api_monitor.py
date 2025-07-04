import logging
import time
from collections import deque
from typing import Deque


class APIMonitor:
    """Tracks API request counts and rate limit hits."""

    def __init__(self, window: int = 60) -> None:
        self.window = window
        self.requests: Deque[float] = deque()
        self.ratelimited = 0

    def _trim(self) -> None:
        cutoff = time.time() - self.window
        while self.requests and self.requests[0] < cutoff:
            self.requests.popleft()

    def record_request(self, status: int) -> None:
        """Record a request and its status code."""
        self.requests.append(time.time())
        if status == 429:
            self.ratelimited += 1
            logging.warning("API rate limited (HTTP 429)")
        self._trim()

    def request_rate(self) -> int:
        """Return number of requests in the monitoring window."""
        self._trim()
        return len(self.requests)


monitor = APIMonitor()
