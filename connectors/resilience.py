"""
connectors/resilience.py — Retry with exponential backoff and circuit breaker.
Used as decorators / wrappers on every external API call.
"""

import time
import logging
import functools
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from datetime import datetime, timezone

logger = logging.getLogger("weatherbet.resilience")


# ═══════════════════════════════════════════════════════════
# RETRY WITH EXPONENTIAL BACKOFF
# ═══════════════════════════════════════════════════════════

def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exceptions: tuple = (Exception,),
):
    """
    Decorator that retries a function on failure with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts.
        base_delay: Initial delay in seconds.
        max_delay: Maximum delay cap in seconds.
        exceptions: Tuple of exception types to catch and retry.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        logger.warning(
                            "[RETRY] %s attempt %d/%d failed: %s — waiting %.1fs",
                            func.__name__, attempt + 1, max_retries, e, delay,
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            "[RETRY] %s exhausted %d retries: %s",
                            func.__name__, max_retries, e,
                        )
            raise last_exc  # type: ignore[misc]
        return wrapper
    return decorator


# ═══════════════════════════════════════════════════════════
# CIRCUIT BREAKER
# ═══════════════════════════════════════════════════════════

class CircuitBreaker:
    """
    Simple circuit breaker. Opens after `failure_threshold` consecutive failures,
    stays open for `recovery_timeout` seconds, then moves to half-open
    (allows one probe request). If the probe succeeds the circuit closes;
    if it fails the circuit opens again.

    Usage:
        cb = CircuitBreaker("polymarket_gamma", failure_threshold=5, recovery_timeout=120)

        if not cb.can_execute():
            return None  # circuit is open, skip call

        try:
            result = call_api()
            cb.record_success()
            return result
        except Exception as e:
            cb.record_failure()
            raise
    """

    STATE_CLOSED = "closed"
    STATE_OPEN = "open"
    STATE_HALF_OPEN = "half_open"

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 120.0,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout

        self._state = self.STATE_CLOSED
        self._failure_count = 0
        self._last_failure_time: float | None = None

    @property
    def state(self) -> str:
        if self._state == self.STATE_OPEN and self._last_failure_time:
            elapsed = time.time() - self._last_failure_time
            if elapsed >= self.recovery_timeout:
                self._state = self.STATE_HALF_OPEN
                logger.info("[CB:%s] half-open after %.0fs cooldown", self.name, elapsed)
        return self._state

    def can_execute(self) -> bool:
        s = self.state
        return s in (self.STATE_CLOSED, self.STATE_HALF_OPEN)

    def record_success(self):
        if self._state in (self.STATE_HALF_OPEN, self.STATE_OPEN):
            logger.info("[CB:%s] closed — probe succeeded", self.name)
        self._failure_count = 0
        self._state = self.STATE_CLOSED

    def record_failure(self):
        self._failure_count += 1
        self._last_failure_time = time.time()
        if self._failure_count >= self.failure_threshold:
            self._state = self.STATE_OPEN
            logger.warning(
                "[CB:%s] OPEN — %d consecutive failures (cooldown %.0fs)",
                self.name, self._failure_count, self.recovery_timeout,
            )


# ═══════════════════════════════════════════════════════════
# PRE-BUILT CIRCUIT BREAKERS
# ═══════════════════════════════════════════════════════════

gamma_cb = CircuitBreaker("gamma_api", failure_threshold=5, recovery_timeout=120)
openmeteo_cb = CircuitBreaker("open_meteo", failure_threshold=5, recovery_timeout=120)
metar_cb = CircuitBreaker("metar", failure_threshold=5, recovery_timeout=180)
clob_cb = CircuitBreaker("clob_api", failure_threshold=3, recovery_timeout=300)


# ═══════════════════════════════════════════════════════════
# SHARED HTTP SESSIONS (connection pooling + keep-alive)
# ═══════════════════════════════════════════════════════════

_sessions: dict[str, requests.Session] = {}


def get_http_session(service: str = "default") -> requests.Session:
    """
    Return a shared requests.Session per service.
    Reuses TCP connections and reduces handshake overhead.
    """
    sess = _sessions.get(service)
    if sess is not None:
        return sess

    session = requests.Session()
    retry = Retry(
        total=1,
        connect=1,
        read=1,
        backoff_factor=0.15,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        pool_connections=32,
        pool_maxsize=64,
        max_retries=retry,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": "weatherbet/3.0",
            "Accept": "application/json,text/plain,*/*",
            "Connection": "keep-alive",
        }
    )

    _sessions[service] = session
    return session
