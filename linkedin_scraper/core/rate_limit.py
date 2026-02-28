"""
Global rate limiter and backoff state for person-profile scraping.

Enforces a minimum delay between profile requests and persists backoff state
when LinkedIn returns a rate limit so separate runs do not immediately retry.
Also enforces a per-account daily cap on profiles scraped.
"""

import asyncio
import json
import logging
import time
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

# Default: at least 45 seconds between person-profile requests per account
MIN_DELAY_BETWEEN_PROFILES_SEC = 15
# Max profiles to scrape per account per calendar day (fail fast when hit)
MAX_PROFILES_PER_DAY = 100
# Backoff state file (next to session.json)
STATE_DIR = Path(__file__).resolve().parent.parent
RATE_LIMIT_STATE_FILE = STATE_DIR / ".rate_limit_state.json"


def _load_state() -> dict:
    """Load persisted rate-limit state (global; keyed by account in the future)."""
    if not RATE_LIMIT_STATE_FILE.exists():
        return {}
    try:
        with open(RATE_LIMIT_STATE_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("Could not load rate-limit state: %s", e)
        return {}


def _save_state(state: dict) -> None:
    """Persist rate-limit state."""
    try:
        RATE_LIMIT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(RATE_LIMIT_STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.warning("Could not save rate-limit state: %s", e)


def _account_key(session_path: str | Path | None) -> str:
    """Key for per-account state (default single account)."""
    if session_path:
        return str(Path(session_path).resolve())
    return "default"


async def wait_if_needed_before_profile(
    session_path: str | Path | None = None,
    min_delay_sec: float = MIN_DELAY_BETWEEN_PROFILES_SEC,
) -> None:
    """
    Call before starting a person-profile scrape. If the last profile request was
    too recent, sleeps until min_delay_sec has elapsed. If we are in backoff
    (after a RateLimitError), sleeps until backoff_until or raises.
    """
    key = _account_key(session_path)
    state = _load_state()
    now = time.time()

    backoff_until = state.get("backoff_until") or 0
    if backoff_until and now < backoff_until:
        wait_sec = backoff_until - now
        logger.warning(
            "Rate-limit backoff active: waiting %.0f s before next profile (account=%s)",
            wait_sec,
            key[:32],
        )
        await asyncio.sleep(wait_sec)
        # Re-load in case of concurrent update
        state = _load_state()
        backoff_until = state.get("backoff_until") or 0
        if backoff_until and time.time() < backoff_until:
            await asyncio.sleep(backoff_until - time.time())

    last_at = state.get("last_profile_at") or 0
    if last_at:
        elapsed = now - last_at
        if elapsed < min_delay_sec:
            sleep_sec = min_delay_sec - elapsed
            logger.debug("Throttle: waiting %.1f s before next profile", sleep_sec)
            await asyncio.sleep(sleep_sec)

    # Caller must call record_profile_started() when the profile scrape actually starts


def record_profile_started(session_path: str | Path | None = None) -> None:
    """Record that a profile scrape has started (for throttle and daily cap accounting)."""
    state = _load_state()
    now = time.time()
    state["last_profile_at"] = now
    today_str = date.today().isoformat()
    if state.get("date_today") != today_str:
        state["date_today"] = today_str
        state["profiles_today"] = 0
        state["rate_limit_count_today"] = 0
        state["degradation_mode"] = "normal"
    state["profiles_today"] = state.get("profiles_today", 0) + 1
    _save_state(state)


def _end_of_today_epoch() -> float:
    """Return Unix timestamp for end of current day (23:59:59 local)."""
    from datetime import datetime
    today = date.today()
    end = datetime(today.year, today.month, today.day, 23, 59, 59)
    return end.timestamp()


def record_rate_limit_error(
    suggested_wait_time: int = 900,
    session_path: str | Path | None = None,
    endpoint: str | None = None,
) -> None:
    """
    Call when RateLimitError is caught. Persists backoff and degradation state.
    First rate limit of the day -> reduced data mode. Second+ -> stop for the rest of the day.
    """
    key = _account_key(session_path)
    state = _load_state()
    now = time.time()
    today_str = date.today().isoformat()
    if state.get("date_today") != today_str:
        state["date_today"] = today_str
        state["rate_limit_count_today"] = 0
        state["degradation_mode"] = "normal"
    state["rate_limit_count_today"] = state.get("rate_limit_count_today", 0) + 1
    state["rate_limit_count"] = state.get("rate_limit_count", 0) + 1
    state["last_rate_limit_at"] = now

    count_today = state["rate_limit_count_today"]
    if count_today == 1:
        state["degradation_mode"] = "reduced"
        state["backoff_until"] = now + suggested_wait_time
        logger.warning(
            "Rate limit (first today): timestamp=%s endpoint=%s account=%s -> reduced data mode, backoff %ds",
            time.ctime(now),
            endpoint or "profile",
            key[:48] if len(key) > 48 else key,
            suggested_wait_time,
        )
    else:
        state["degradation_mode"] = "stopped"
        state["backoff_until"] = _end_of_today_epoch()
        logger.warning(
            "Rate limit (repeated today): timestamp=%s endpoint=%s account=%s -> stop scraping for today",
            time.ctime(now),
            endpoint or "profile",
            key[:48] if len(key) > 48 else key,
        )
    _save_state(state)


def get_backoff_remaining_sec(session_path: str | Path | None = None) -> float:
    """Return seconds remaining in backoff, or 0 if not in backoff."""
    state = _load_state()
    backoff_until = state.get("backoff_until") or 0
    if backoff_until <= 0:
        return 0.0
    return max(0.0, backoff_until - time.time())


def is_in_backoff(session_path: str | Path | None = None) -> bool:
    """Return True if we are currently in backoff (do not start new profile scrapes)."""
    return get_backoff_remaining_sec(session_path) > 0


def get_profiles_scraped_today(session_path: str | Path | None = None) -> int:
    """Return number of profiles recorded today (same account key as other state)."""
    state = _load_state()
    today_str = date.today().isoformat()
    if state.get("date_today") != today_str:
        return 0
    return state.get("profiles_today", 0)


def would_exceed_daily_cap(session_path: str | Path | None = None) -> bool:
    """Return True if scraping one more profile would exceed the daily cap."""
    return get_profiles_scraped_today(session_path) >= MAX_PROFILES_PER_DAY


def get_daily_cap_remaining(session_path: str | Path | None = None) -> int:
    """Return how many profiles can still be scraped today before hitting the cap."""
    return max(0, MAX_PROFILES_PER_DAY - get_profiles_scraped_today(session_path))


def get_degradation_mode(session_path: str | Path | None = None) -> str:
    """
    Return current degradation mode: "normal", "reduced", or "stopped".
    "reduced" = first rate limit today -> scrape only name/headline/current experience.
    "stopped" = repeated rate limit -> do not scrape more profiles today.
    """
    state = _load_state()
    today_str = date.today().isoformat()
    if state.get("date_today") != today_str:
        return "normal"
    return state.get("degradation_mode", "normal")


def get_rate_limit_metrics(session_path: str | Path | None = None) -> dict:
    """Return simple metrics for logging: profiles_today, rate_limit_count_today, degradation_mode."""
    state = _load_state()
    today_str = date.today().isoformat()
    if state.get("date_today") != today_str:
        return {
            "profiles_today": 0,
            "rate_limit_count_today": 0,
            "degradation_mode": "normal",
        }
    return {
        "profiles_today": state.get("profiles_today", 0),
        "rate_limit_count_today": state.get("rate_limit_count_today", 0),
        "degradation_mode": state.get("degradation_mode", "normal"),
    }
