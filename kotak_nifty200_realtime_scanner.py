#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kotak_nifty200_realtime_scanner.py
==================================

Real-time Nifty 200 executed-volume (order-flow) and momentum scanner built on the
official Kotak Neo Trade API Python SDK (import name ``neo_api_client``).

Threads
-------
    T1  FeedProducer     WebSocket -> on_message() -> tick_queue.put_nowait()  (never blocks)
    T2  QuantEngine      tick_queue -> Lee-Ready/EMO trade classification -> 1-second buckets
                         -> rolling 1m / 5m / 15m candles -> alert triggers -> alert_queue
    T3  AlertDispatcher  alert_queue -> console + log file + Telegram (requests.Session)

SDK compatibility (auto-detected at runtime)
--------------------------------------------
    * kotakneoapi >= 2.2 / 3.x (current, ``pip install kotakneoapi``): async SFeed WebSocket via
      ``client.create_websocket()``. Kotak removed the callback ``client.subscribe()`` API in 2.2.0,
      so this backend runs an asyncio loop inside T1 and wires ``ws.on_message`` to the producer.
    * Kotak-neo-api-v2 2.0.x (legacy): callback WebSocket via ``client.subscribe()`` and
      ``client.on_message``.

Install
-------
    pip install kotakneoapi requests pyotp python-dotenv

.env (next to this script or in the working directory)
------------------------------------------------------
    KOTAK_CONSUMER_KEY=...            # NEO app -> More -> Trade API -> application token
    KOTAK_CONSUMER_SECRET=...         # optional: only passed if the installed SDK accepts it
    KOTAK_MOBILE_NUMBER=+91XXXXXXXXXX # 10-digit numbers are normalised to +91XXXXXXXXXX
    KOTAK_UCC=...
    KOTAK_MPIN=...
    KOTAK_TOTP_SECRET=...             # base32 secret shown during TOTP registration
    TELEGRAM_BOT_TOKEN=...
    TELEGRAM_CHAT_ID=...
  Optional tuning: KOTAK_ENVIRONMENT (prod), KOTAK_NEO_FIN_KEY, MARKET_OPEN (09:15),
  MARKET_CLOSE (15:30), EXIT_AFTER_CLOSE (1), WS_SUBSCRIBE_BATCH_SIZE (50),
  FEED_STALE_SEC (30), TELEGRAM_MIN_INTERVAL_SEC (1.05 private chat / 3.1 group).

Run
---
    python kotak_nifty200_realtime_scanner.py            # live scanner
    python kotak_nifty200_realtime_scanner.py --check    # login + token map + Telegram test, exit

Notes on the feed fields
------------------------
    * Kotak's level-1 best bid/ask are ``bp``/``sp`` (legacy feed) and ``buy[0]``/``sell[0]``
      (SFeed). In the legacy depth feed ``bp1``/``sp1`` are the SECOND depth level, so this
      scanner uses ``bp``/``sp`` as "best bid/ask" for the quote rule.
    * The quote rule compares a print against the quote that prevailed BEFORE it (the previous
      update for that stock). The post-trade quote often already shows the consumed level on
      the other side, which would misclassify the print.
    * Volume deltas come from the exchange's cumulative day volume, so a dropped or conflated
      tick never loses volume: it is attributed to the next update.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import csv
import html
import inspect
import io
import json
import logging
import logging.handlers
import os
import queue
import random
import re
import signal
import sys
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Keep the SDK's per-tick INFO logging off unless the user explicitly enables it.
os.environ.setdefault("NEO_LOG_LEVEL", "WARNING")
os.environ.setdefault("NEO_LOG_FILE_LEVEL", "WARNING")

MISSING_DEPENDENCIES: List[str] = []
try:
    import requests
except ImportError:  # pragma: no cover - environment check
    requests = None  # type: ignore[assignment]
    MISSING_DEPENDENCIES.append("requests")
try:
    import pyotp
except ImportError:  # pragma: no cover - environment check
    pyotp = None  # type: ignore[assignment]
    MISSING_DEPENDENCIES.append("pyotp")
try:
    import neo_api_client
    from neo_api_client import NeoAPI
except ImportError:  # pragma: no cover - environment check
    neo_api_client = None  # type: ignore[assignment]
    NeoAPI = None  # type: ignore[assignment]
    MISSING_DEPENDENCIES.append("kotakneoapi (neo_api_client)")

try:  # Current SDK (kotakneoapi >= 2.2): async SFeed WebSocket client.
    from neo_api_client.websocket.feed import WsToken as SFeedWsToken
    from neo_api_client.websocket.feed.exceptions import AuthenticationError as SFeedAuthError
    from neo_api_client.websocket.feed.exceptions import NotConnectedError as SFeedNotConnectedError
except Exception:  # Legacy SDK (or SDK missing): the SFeed backend is simply not used.
    SFeedWsToken = None

    class SFeedAuthError(Exception):
        """Stand-in so except-clauses stay valid when the SFeed client is unavailable."""

    class SFeedNotConnectedError(Exception):
        """Stand-in so except-clauses stay valid when the SFeed client is unavailable."""


# ════════════════════════════════════════════════════════════════════════════════════════════
# Constants
# ════════════════════════════════════════════════════════════════════════════════════════════
IST_OFFSET_SEC = 19_800  # IST = UTC+05:30, no DST, so a fixed offset is exact.
SEGMENT = "nse_cm"

NSE_NIFTY200_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty200list.csv"
NSE_HOME_URL = "https://www.nseindia.com/"
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"
    ),
    "Accept": "text/csv,text/plain,application/octet-stream,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}
MIN_MAPPED_INSTRUMENTS = 150

TICK_QUEUE_MAXSIZE = 100_000
ALERT_QUEUE_MAXSIZE = 10_000
HISTORY_SECONDS = 900  # deque(maxlen=900) of 1-second buckets per stock
WIN_1M, WIN_5M, WIN_15M = 60, 300, 900

# Session timing (IST). MARKET_OPEN / MARKET_CLOSE in .env shift everything (e.g. Muhurat).
WARMUP_SEC = 120  # 09:15-09:17 warm-up: buckets build, no alerts
BREAKOUT_DELAY_SEC = 600  # fresh day high/low breakouts only from 09:25
PRE_OPEN_CONNECT_SEC = 900  # reconnect window opens at 09:00 so the feed is live by 09:15
WATCHDOG_GRACE_SEC = 30  # no stale-feed checks during the first 30 s of the session
SESSION_ROLLOVER_SEC = 600  # recycle a socket that outlives close + 10 min (daemon mode)

# Liquidity filter, cooldown and pulse cadence
MIN_TURNOVER_5M_CR = 0.20  # ₹20 lakh rolling 5-minute turnover
COOLDOWN_SEC = 180
PULSE_EVERY_SEC = 300

# Trigger thresholds
FAST_BODY_5M_PCT = 0.50
FAST_BODY_1M_PCT = 0.30
FAST_FLOW_PCT = 62.0
FAST_CLV_UP = 0.72
FAST_CLV_DOWN = 0.28
SLOW_MIN_MOVE_PCT = 0.75
SLOW_FLOW_PCT = 55.0
ABSORB_VOL_MULT = 2.2
ABSORB_FLOW_PCT = 55.0
ABSORB_BUYER_MIN_BODY_PCT = -0.10
ABSORB_BUYER_MIN_CLV = 0.65
ABSORB_SELLER_MAX_BODY_PCT = 0.10
ABSORB_SELLER_MAX_CLV = 0.35
MILESTONE_STEP_PCT = 1.5
BREAKOUT_FLOW_PCT = 55.0  # 1m aggressive share that confirms a fresh day high / low

# Feed supervision
FEED_STALE_SEC_DEFAULT = 30.0
FEED_NO_DATA_SEC = 180.0
WS_BATCH_DEFAULT = 50
LEGACY_OPEN_TIMEOUT_SEC = 25.0

# Telegram / dispatcher
TG_MESSAGE_LIMIT = 3800  # Telegram hard limit is 4096 characters
TG_MAX_ATTEMPTS = 5
MAX_PENDING_ALERTS = 500
SHUTDOWN_FLUSH_SEC = 10.0
STATS_EVERY_SEC = 60.0

# 1-second bucket layout: (sec_timestamp, open, high, low, close, agg_buy_vol, agg_sell_vol,
#                          total_vol, atp)
B_TS, B_O, B_H, B_L, B_C, B_BUY, B_SELL, B_TOT, B_ATP = range(9)

LEGACY_FIELDS = ("ltp", "v", "ap", "bp", "sp", "op", "h", "lo", "c")

TITLE_FAST_UP = "🚀 FAST ROLLING UP (अचानक तेज़ खरीदारी)"
TITLE_FAST_DOWN = "🩸 FAST ROLLING DOWN (अचानक तेज़ बिकवाली)"
TITLE_SLOW_ACCUM = "📈 SLOW STEADY ACCUMULATION (धीरे-धीरे ऊपर का ट्रेंड)"
TITLE_SLOW_DIST = "📉 SLOW STEADY DISTRIBUTION (धीरे-धीरे नीचे का ट्रेंड)"
TITLE_HIDDEN_BUYER = "🛡️ HIDDEN BUYER ABSORPTION / ICEBERG (नीचे के लेवल पर छुपे खरीदार)"
TITLE_HIDDEN_SELLER = "🧱 HIDDEN SELLER ABSORPTION / ICEBERG (ऊपर के लेवल पर छुपे बिकवाल)"
TITLE_DAY_HIGH = "⚡ FRESH DAY HIGH BREAKOUT (दिन का नया हाई)"
TITLE_DAY_LOW = "⚡ FRESH DAY LOW BREAKDOWN (दिन का नया लो)"
TITLE_PULSE = "📊 NIFTY 200 MARKET PULSE (बाज़ार की नब्ज़)"

# Nifty 200 constituents snapshot (NSE ind_nifty200list.csv, downloaded 2026-11-02), used only
# when the live NSE download fails.
NIFTY200_FALLBACK: Tuple[str, ...] = (
    "360ONE", "ABB", "APLAPOLLO", "AUBANK", "ADANIENSOL", "ADANIENT", "ADANIGREEN", "ADANIPORTS",
    "ADANIPOWER", "ATGL", "ABCAPITAL", "AMBUJACEM", "APARINDS", "APOLLOHOSP", "ASHOKLEY", "ASIANPAINT",
    "ASTRAL", "AUROPHARMA", "DMART", "AXISBANK", "BSE", "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV",
    "BAJAJHLDNG", "BANKBARODA", "BANKINDIA", "MAHABANK", "BDL", "BEL", "BHARATFORG", "BHEL",
    "BPCL", "BHARTIARTL", "GROWW", "BIOCON", "BLUESTARCO", "BOSCHLTD", "BRITANNIA", "CGPOWER",
    "CANBK", "CHOLAFIN", "CIPLA", "COALINDIA", "COCHINSHIP", "COFORGE", "COLPAL", "CONCOR",
    "CUMMINSIND", "DLF", "DABUR", "DIVISLAB", "DIXON", "DRREDDY", "EICHERMOT", "ETERNAL",
    "EXIDEIND", "NYKAA", "FEDERALBNK", "FORTIS", "GAIL", "GVT&D", "GMRAIRPORT", "GLENMARK",
    "GODFRYPHLP", "GODREJCP", "GODREJPROP", "GRASIM", "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE",
    "HAVELLS", "HEROMOTOCO", "HINDALCO", "HAL", "HINDCOPPER", "HINDPETRO", "HINDUNILVR", "HINDZINC",
    "POWERINDIA", "HYUNDAI", "ICICIBANK", "ICICIGI", "ICICIAMC", "IDFCFIRSTB", "ITC", "INDIANB",
    "INDHOTEL", "IOC", "IRCTC", "IRFC", "IREDA", "INDUSTOWER", "INDUSINDBK", "NAUKRI",
    "INFY", "INDIGO", "JSWENERGY", "JSWSTEEL", "JINDALSTEL", "JIOFIN", "JUBLFOOD", "KEI",
    "KALYANKJIL", "KOTAKBANK", "LTF", "LGEINDIA", "LICHSGFIN", "LTM", "LT", "LAURUSLABS",
    "LENSKART", "LICI", "LODHA", "LUPIN", "MRF", "M&MFIN", "M&M", "MANKIND",
    "MARICO", "MARUTI", "MFSL", "MAXHEALTH", "MAZDOCK", "MEESHO", "MOTILALOFS", "MPHASIS",
    "MCX", "MUTHOOTFIN", "NHPC", "NLCINDIA", "NMDC", "NTPC", "NATIONALUM", "NESTLEIND",
    "OBEROIRLTY", "ONGC", "OIL", "PAYTM", "OFSS", "POLICYBZR", "PIIND", "PAGEIND",
    "PATANJALI", "PERSISTENT", "PHOENIXLTD", "PIDILITIND", "POLYCAB", "PFC", "POWERGRID", "PREMIERENE",
    "PRESTIGE", "PNB", "RECLTD", "RADICO", "RVNL", "RELIANCE", "SBICARD", "SBILIFE",
    "SRF", "MOTHERSON", "SHRIRAMFIN", "ENRIN", "SIEMENS", "SOLARINDS", "SBIN", "SAIL",
    "SUNPHARMA", "SUPREMEIND", "SUZLON", "SWIGGY", "TVSMOTOR", "TATACAP", "TATACOMM", "TCS",
    "TATACONSUM", "TMCV", "TMPV", "TATAPOWER", "TATASTEEL", "TECHM", "TITAN", "TORNTPHARM",
    "TRENT", "TIINDIA", "UPL", "ULTRACEMCO", "UNIONBANK", "UNITDSPR", "VBL", "VAML",
    "VEDL", "VMM", "IDEA", "VOLTAS", "WAAREEENER", "WIPRO", "YESBANK", "ZYDUSLIFE",
)


# ════════════════════════════════════════════════════════════════════════════════════════════
# Small helpers: IST clock math, formatting, .env loading
# ════════════════════════════════════════════════════════════════════════════════════════════
class ConfigError(Exception):
    """Invalid or missing configuration."""


class LoginError(Exception):
    """TOTP login or MPIN validation failed."""


def ist_day(epoch: float) -> int:
    return int(epoch + IST_OFFSET_SEC) // 86_400


def ist_sod(epoch: float) -> int:
    """IST seconds-of-day for a Unix timestamp."""
    return int(epoch + IST_OFFSET_SEC) % 86_400


def ist_weekday(epoch: float) -> int:
    """Monday=0 ... Sunday=6 (1970-01-01 was a Thursday)."""
    return (ist_day(epoch) + 3) % 7


def ist_hms(epoch: float) -> str:
    return time.strftime("%H:%M:%S", time.gmtime(int(epoch) + IST_OFFSET_SEC))


def ist_date(epoch: float) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(int(epoch) + IST_OFFSET_SEC))


def sod_to_hhmm(sod: int) -> str:
    return f"{sod // 3600:02d}:{(sod % 3600) // 60:02d}"


def parse_hhmm(text: str, default: str) -> int:
    value = (text or "").strip() or default
    match = re.fullmatch(r"(\d{1,2}):(\d{2})", value)
    if not match:
        raise ConfigError(f"Invalid time '{value}' (expected HH:MM)")
    hours, minutes = int(match.group(1)), int(match.group(2))
    if hours > 23 or minutes > 59:
        raise ConfigError(f"Invalid time '{value}' (expected HH:MM)")
    return hours * 3600 + minutes * 60


def env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def env_float(name: str, default: float, low: float, high: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got '{raw}'") from exc
    return min(high, max(low, value))


def env_int(name: str, default: int, low: int, high: int) -> int:
    return int(env_float(name, float(default), float(low), float(high)))


def normalize_mobile(raw: str) -> str:
    """Kotak expects the registered mobile with country code, e.g. +919876543210."""
    text = raw.strip()
    digits = re.sub(r"\D", "", text)
    if len(digits) == 10:
        return "+91" + digits
    if len(digits) == 11 and digits.startswith("0"):
        return "+91" + digits[1:]
    if len(digits) == 12 and digits.startswith("91"):
        return "+" + digits
    if text.startswith("+") and digits:
        return "+" + digits
    return text


def mask(text: str, keep: int = 4) -> str:
    if len(text) <= keep:
        return "*" * len(text)
    return "*" * (len(text) - keep) + text[-keep:]


def fmt_price(price: float) -> str:
    return f"₹{price:,.2f}"


def fmt_pct(value: float) -> str:
    return f"{value:+.2f}%"


def fmt_qty(qty: float) -> str:
    size = abs(qty)
    if size >= 1e7:
        text = f"{size / 1e7:.2f}Cr"
    elif size >= 1e5:
        text = f"{size / 1e5:.2f}L"
    elif size >= 1e3:
        text = f"{size / 1e3:.1f}K"
    else:
        text = f"{int(size)}"
    return ("-" if qty < 0 else "") + text


def fmt_signed_qty(qty: float) -> str:
    return ("+" if qty > 0 else "") + fmt_qty(qty)


def fmt_inr_cr(crores: float) -> str:
    if abs(crores) >= 1.0:
        return f"₹{crores:,.2f} Cr"
    return f"₹{crores * 100.0:,.1f} L"


def fmt_signed_inr_cr(crores: float) -> str:
    sign = "+" if crores > 0 else "-" if crores < 0 else ""
    return sign + fmt_inr_cr(abs(crores))


_TAG_RE = re.compile(r"<[^>]+>")


def html_to_text(text: str) -> str:
    return html.unescape(_TAG_RE.sub("", text))


def pack_messages(parts: Sequence[str], limit: int) -> List[str]:
    """Join alert texts into as few Telegram messages as possible, each <= limit chars."""
    messages: List[str] = []
    buffer = ""
    for part in parts:
        if len(part) > limit:
            part = part[: limit - 1] + "…"
        if not buffer:
            buffer = part
        elif len(buffer) + 2 + len(part) <= limit:
            buffer = f"{buffer}\n\n{part}"
        else:
            messages.append(buffer)
            buffer = part
    if buffer:
        messages.append(buffer)
    return messages


def load_env_file(path: Path) -> bool:
    """Load KEY=VALUE pairs (python-dotenv when installed, a strict built-in parser otherwise).

    Existing environment variables always win over the file.
    """
    if not path.is_file():
        return False
    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None
    if load_dotenv is not None:
        load_dotenv(dotenv_path=path, override=False)
        return True
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, sep, value = line.partition("=")
        if not sep:
            continue
        key, value = key.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        else:
            value = value.split(" #", 1)[0].rstrip()
        os.environ.setdefault(key, value)
    return True


class IstFormatter(logging.Formatter):
    """Log timestamps in IST regardless of the host time zone."""

    def converter(self, timestamp):  # type: ignore[override]
        return time.gmtime(timestamp + IST_OFFSET_SEC)


def setup_logging(level_name: str, log_dir: Path) -> Tuple[logging.Logger, logging.Logger]:
    with contextlib.suppress(Exception):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    level = getattr(logging, level_name.upper(), logging.INFO)
    formatter = IstFormatter("%(asctime)s IST %(levelname)-7s [%(threadName)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    handlers: List[logging.Handler] = []
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    handlers.append(console)
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_dir / "nifty200_scanner.log", maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    except OSError as exc:
        print(f"WARNING: file logging disabled ({exc})", file=sys.stderr)
    log = logging.getLogger("scanner")
    log.setLevel(level)
    log.propagate = False
    for handler in list(log.handlers):
        log.removeHandler(handler)
    for handler in handlers:
        log.addHandler(handler)
    alert_log = logging.getLogger("scanner.alerts")
    alert_log.setLevel(logging.INFO)
    return log, alert_log


# ════════════════════════════════════════════════════════════════════════════════════════════
# Configuration
# ════════════════════════════════════════════════════════════════════════════════════════════
class Config:
    REQUIRED = ("KOTAK_CONSUMER_KEY", "KOTAK_MOBILE_NUMBER", "KOTAK_UCC", "KOTAK_MPIN", "KOTAK_TOTP_SECRET")

    def __init__(self) -> None:
        missing = [key for key in self.REQUIRED if not env_str(key)]
        if missing:
            raise ConfigError("Missing required .env keys: " + ", ".join(missing))
        self.consumer_key = env_str("KOTAK_CONSUMER_KEY")
        self.consumer_secret = env_str("KOTAK_CONSUMER_SECRET")
        self.mobile = normalize_mobile(env_str("KOTAK_MOBILE_NUMBER"))
        self.ucc = env_str("KOTAK_UCC")
        self.mpin = env_str("KOTAK_MPIN")
        self.totp_secret = re.sub(r"[\s-]", "", env_str("KOTAK_TOTP_SECRET")).upper()
        try:
            pyotp.TOTP(self.totp_secret).now()
        except Exception as exc:
            raise ConfigError("KOTAK_TOTP_SECRET is not a valid base32 TOTP secret") from exc
        self.environment = env_str("KOTAK_ENVIRONMENT", "prod").lower() or "prod"
        self.neo_fin_key = env_str("KOTAK_NEO_FIN_KEY") or None
        self.tg_token = env_str("TELEGRAM_BOT_TOKEN")
        self.tg_chat_id = env_str("TELEGRAM_CHAT_ID")
        default_interval = 3.1 if self.tg_chat_id.startswith("-") else 1.05  # group: 20 msg/min
        self.tg_min_interval = env_float("TELEGRAM_MIN_INTERVAL_SEC", default_interval, 0.2, 60.0)
        self.market_open_sod = parse_hhmm(env_str("MARKET_OPEN"), "09:15")
        self.market_close_sod = parse_hhmm(env_str("MARKET_CLOSE"), "15:30")
        if self.market_close_sod <= self.market_open_sod + WARMUP_SEC:
            raise ConfigError("MARKET_CLOSE must be later than MARKET_OPEN + 2 minutes")
        self.exit_after_close = env_bool("EXIT_AFTER_CLOSE", True)
        self.ws_batch_size = env_int("WS_SUBSCRIBE_BATCH_SIZE", WS_BATCH_DEFAULT, 1, 500)
        self.stale_feed_sec = env_float("FEED_STALE_SEC", FEED_STALE_SEC_DEFAULT, 5.0, 600.0)


# ════════════════════════════════════════════════════════════════════════════════════════════
# Kotak Neo session: automated TOTP login + MPIN validation
# ════════════════════════════════════════════════════════════════════════════════════════════
def _response_ok(resp: Any) -> bool:
    if not isinstance(resp, dict):
        return False
    data = resp.get("data")
    if not isinstance(data, dict) or not data.get("token"):
        return False
    return str(data.get("status", "success")).lower() not in ("failure", "failed", "error")


def _response_error(resp: Any) -> str:
    """Human-readable error text that never echoes tokens or session ids."""
    if isinstance(resp, dict):
        for key in ("errMsg", "message", "emsg", "error", "Error", "Error Message", "description"):
            value = resp.get(key)
            if value:
                return str(value)[:300]
        data = resp.get("data")
        if isinstance(data, dict):
            for key in ("message", "errMsg", "emsg", "status"):
                if data.get(key):
                    return str(data[key])[:300]
        return "unexpected response (keys: " + ", ".join(sorted(map(str, resp.keys()))) + ")"
    return f"unexpected response type {type(resp).__name__}"


class KotakSession:
    """Owns the authenticated NeoAPI client; re-logins once per IST day or after invalidation."""

    def __init__(self, cfg: Config, log: logging.Logger) -> None:
        self.cfg = cfg
        self.log = log
        self.client: Any = None
        self.login_day = -1
        self._valid = False

    def _make_client(self) -> Any:
        wanted: Dict[str, Any] = {
            "consumer_key": self.cfg.consumer_key,
            "environment": self.cfg.environment,
            "access_token": None,
            "neo_fin_key": self.cfg.neo_fin_key,
        }
        if self.cfg.consumer_secret:
            wanted["consumer_secret"] = self.cfg.consumer_secret
        try:
            params = inspect.signature(NeoAPI.__init__).parameters
            accepts_any = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())
            kwargs = {k: v for k, v in wanted.items() if accepts_any or k in params}
        except (TypeError, ValueError):
            kwargs = {"consumer_key": self.cfg.consumer_key, "environment": self.cfg.environment}
        try:
            return NeoAPI(**kwargs)
        except TypeError:
            return NeoAPI(consumer_key=self.cfg.consumer_key, environment=self.cfg.environment)

    def _fresh_totp(self) -> str:
        totp = pyotp.TOTP(self.cfg.totp_secret)
        remaining = totp.interval - (time.time() % totp.interval)
        if remaining < 5.0:  # avoid submitting a code that expires in flight
            time.sleep(remaining + 0.5)
        return totp.now()

    def login(self, attempts: int = 3) -> Any:
        last_error = "unknown error"
        for attempt in range(1, attempts + 1):
            try:
                client = self._make_client()
                code = self._fresh_totp()
                step1 = client.totp_login(mobile_number=self.cfg.mobile, ucc=self.cfg.ucc, totp=code)
                if not _response_ok(step1):
                    raise LoginError(f"totp_login rejected: {_response_error(step1)}")
                step2 = client.totp_validate(mpin=self.cfg.mpin)
                edit_token = getattr(getattr(client, "configuration", None), "edit_token", None)
                if not _response_ok(step2) or not edit_token:
                    raise LoginError(f"totp_validate rejected: {_response_error(step2)}")
                self.client = client
                self.login_day = ist_day(time.time())
                self._valid = True
                self.log.info("Kotak Neo login OK (UCC %s, mobile %s)", self.cfg.ucc, mask(self.cfg.mobile))
                return client
            except LoginError as exc:
                last_error = str(exc)
                self.log.error("Login attempt %d/%d failed: %s", attempt, attempts, last_error)
                if attempt < attempts:  # a TOTP code is single-use: wait for the next 30 s window
                    time.sleep(31.0 - (time.time() % 30.0))
            except Exception as exc:  # network errors surface as ApiException / httpx errors
                last_error = f"{type(exc).__name__}: {exc}"
                self.log.error("Login attempt %d/%d error: %s", attempt, attempts, last_error)
                if attempt < attempts:
                    time.sleep(5.0 * attempt)
        raise LoginError(last_error)

    def ensure_fresh(self) -> Any:
        if self.client is None or not self._valid or self.login_day != ist_day(time.time()):
            self.log.info("Refreshing Kotak Neo session (TOTP login)")
            return self.login()
        return self.client

    def invalidate(self) -> None:
        self._valid = False


# ════════════════════════════════════════════════════════════════════════════════════════════
# Nifty 200 universe: NSE constituents -> Kotak scrip master pSymbol (instrument_token)
# ════════════════════════════════════════════════════════════════════════════════════════════
class Constituent:
    __slots__ = ("symbol", "series", "isin", "company", "industry")

    def __init__(self, symbol: str, series: str = "EQ", isin: str = "", company: str = "", industry: str = "") -> None:
        self.symbol = symbol
        self.series = series or "EQ"
        self.isin = isin
        self.company = company
        self.industry = industry


class Instrument:
    __slots__ = ("symbol", "token", "trading_symbol", "isin", "company", "industry")

    def __init__(self, symbol: str, token: str, trading_symbol: str, isin: str = "", company: str = "", industry: str = "") -> None:
        self.symbol = symbol
        self.token = token
        self.trading_symbol = trading_symbol
        self.isin = isin
        self.company = company
        self.industry = industry

    def to_dict(self) -> Dict[str, str]:
        return {name: getattr(self, name) for name in self.__slots__}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Instrument":
        return cls(
            str(data["symbol"]),
            str(data["token"]),
            str(data.get("trading_symbol") or f"{data['symbol']}-EQ"),
            str(data.get("isin") or ""),
            str(data.get("company") or ""),
            str(data.get("industry") or ""),
        )


def parse_nse_constituents(content: bytes) -> List[Constituent]:
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig", errors="replace")))
    if not reader.fieldnames:
        return []
    reader.fieldnames = [(name or "").strip() for name in reader.fieldnames]
    out: List[Constituent] = []
    seen = set()
    for row in reader:
        symbol = (row.get("Symbol") or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(
            Constituent(
                symbol,
                (row.get("Series") or "EQ").strip().upper() or "EQ",
                (row.get("ISIN Code") or "").strip().upper(),
                (row.get("Company Name") or "").strip(),
                (row.get("Industry") or "").strip(),
            )
        )
    return out


def fetch_nifty200_constituents(log: logging.Logger) -> Tuple[List[Constituent], str]:
    http = requests.Session()
    http.headers.update(BROWSER_HEADERS)
    for attempt in range(1, 4):
        try:
            resp = http.get(NSE_NIFTY200_URL, timeout=(10, 30))
            if resp.status_code == 200 and b"Symbol" in resp.content[:300]:
                constituents = parse_nse_constituents(resp.content)
                if len(constituents) >= MIN_MAPPED_INSTRUMENTS:
                    log.info("NSE Nifty 200 list downloaded: %d constituents", len(constituents))
                    return constituents, "NSE live list"
                log.warning("NSE list looked truncated (%d rows)", len(constituents))
            else:
                log.warning("NSE list download returned HTTP %s (attempt %d)", resp.status_code, attempt)
                if resp.status_code in (401, 403):
                    with contextlib.suppress(requests.RequestException):
                        http.get(NSE_HOME_URL, timeout=(10, 30))  # primes NSE cookies
        except requests.RequestException as exc:
            log.warning("NSE list download failed (attempt %d): %s", attempt, type(exc).__name__)
        time.sleep(1.5 * attempt)
    log.warning("Using the built-in Nifty 200 fallback list (%d symbols)", len(NIFTY200_FALLBACK))
    return [Constituent(symbol) for symbol in NIFTY200_FALLBACK], "built-in fallback list"


def resolve_scrip_master_url(client: Any) -> str:
    """client.scrip_master(exchange_segment="nse_cm") returns the CSV URL (both SDK generations)."""
    result = client.scrip_master(exchange_segment=SEGMENT)
    if isinstance(result, str) and result.startswith("http"):
        return result
    listing = client.scrip_master()
    if isinstance(listing, dict):
        paths = listing.get("filesPaths") or (listing.get("data") or {}).get("filesPaths") or []
        matches = [p for p in paths if isinstance(p, str) and "nse_cm" in p.lower()]
        if matches:
            matches.sort(key=lambda p: ("-v1" not in p.lower(), p))
            return matches[0]
    raise RuntimeError(f"scrip_master() did not return an nse_cm CSV URL: {str(result)[:200]}")


def download_scrip_master(url: str) -> List[Tuple[str, str, str, str, str]]:
    """Return (token, group, symbol_name, trading_symbol, isin) rows for the nse_cm segment."""
    resp = requests.get(url, timeout=(10, 120), headers={"User-Agent": BROWSER_HEADERS["User-Agent"]})
    resp.raise_for_status()
    reader = csv.reader(io.StringIO(resp.content.decode("utf-8-sig", errors="replace")))
    header = [name.strip() for name in next(reader)]  # raw headers carry trailing spaces
    index = {name: i for i, name in enumerate(header)}
    needed = ("pSymbol", "pGroup", "pSymbolName", "pTrdSymbol")
    absent = [name for name in needed if name not in index]
    if absent:
        raise RuntimeError(f"Scrip master is missing columns {absent}")
    i_tok, i_grp, i_name, i_trd = (index[name] for name in needed)
    i_isin = index.get("pISIN")
    i_seg = index.get("pExchSeg")
    width = max([i_tok, i_grp, i_name, i_trd] + [i for i in (i_isin, i_seg) if i is not None])
    rows: List[Tuple[str, str, str, str, str]] = []
    for rec in reader:
        if len(rec) <= width:
            continue
        token = rec[i_tok].strip()
        if token.endswith(".0"):
            token = token[:-2]
        if not token.isdigit():
            continue
        if i_seg is not None and rec[i_seg].strip().lower() not in ("", SEGMENT):
            continue
        rows.append(
            (
                token,
                rec[i_grp].strip().upper(),
                rec[i_name].strip().upper(),
                rec[i_trd].strip().upper(),
                rec[i_isin].strip().upper() if i_isin is not None else "",
            )
        )
    if not rows:
        raise RuntimeError("Scrip master CSV contained no nse_cm rows")
    return rows


def map_constituents(
    constituents: Sequence[Constituent], rows: Sequence[Tuple[str, str, str, str, str]]
) -> Tuple[List[Instrument], List[str]]:
    by_trd: Dict[str, Tuple[str, str]] = {}
    by_name: Dict[Tuple[str, str], Tuple[str, str]] = {}
    by_isin: Dict[Tuple[str, str], Tuple[str, str]] = {}
    for token, group, name, trd, isin in rows:
        by_trd.setdefault(trd, (token, trd))
        if group:
            by_name.setdefault((name, group), (token, trd))
            if isin:
                by_isin.setdefault((isin, group), (token, trd))
    instruments: List[Instrument] = []
    missing: List[str] = []
    seen_tokens = set()
    for c in constituents:
        hit = (
            by_trd.get(f"{c.symbol}-{c.series}")
            or by_name.get((c.symbol, c.series))
            or (by_isin.get((c.isin, c.series)) if c.isin else None)
        )
        if hit is None and c.series != "EQ":
            hit = by_trd.get(f"{c.symbol}-EQ") or by_name.get((c.symbol, "EQ"))
        if hit is None or hit[0] in seen_tokens:
            missing.append(c.symbol)
            continue
        seen_tokens.add(hit[0])
        instruments.append(Instrument(c.symbol, hit[0], hit[1], c.isin, c.company, c.industry))
    return instruments, missing


def save_universe_cache(path: Path, instruments: Sequence[Instrument], source: str) -> None:
    payload = {
        "saved_at_ist": f"{ist_date(time.time())} {ist_hms(time.time())}",
        "source": source,
        "instruments": [inst.to_dict() for inst in instruments],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=1), encoding="utf-8")
    os.replace(tmp, path)


def load_universe_cache(path: Path) -> Tuple[List[Instrument], str]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return [Instrument.from_dict(item) for item in data["instruments"]], str(data.get("saved_at_ist", "?"))


def build_universe(client: Any, cache_path: Path, log: logging.Logger) -> Tuple[List[Instrument], str]:
    constituents, source = fetch_nifty200_constituents(log)
    try:
        url = resolve_scrip_master_url(client)
        rows = download_scrip_master(url)
        instruments, missing = map_constituents(constituents, rows)
        if missing:
            log.warning("No nse_cm token found for: %s", ", ".join(missing))
        if len(instruments) < MIN_MAPPED_INSTRUMENTS:
            raise RuntimeError(f"only {len(instruments)} of {len(constituents)} symbols mapped")
        try:
            save_universe_cache(cache_path, instruments, source)
        except OSError as exc:
            log.warning("Could not write token cache %s: %s", cache_path, exc)
        info = f"{len(instruments)}/{len(constituents)} mapped ({source}; {url.rsplit('/', 1)[-1]})"
        log.info("Token map ready: %s", info)
        return instruments, info
    except Exception as exc:
        log.error("Live token mapping failed: %s: %s", type(exc).__name__, exc)
        if cache_path.is_file():
            instruments, saved_at = load_universe_cache(cache_path)
            log.warning("Using cached token map from %s (%d instruments)", saved_at, len(instruments))
            return instruments, f"{len(instruments)} mapped (cached {saved_at})"
        raise


# ════════════════════════════════════════════════════════════════════════════════════════════
# Quant core: rolling candles over 1-second buckets
# ════════════════════════════════════════════════════════════════════════════════════════════
class Alert:
    """One message travelling from the QuantEngine (T2) or main thread to the dispatcher (T3)."""

    __slots__ = ("ts", "kind", "symbol", "title", "lines")

    def __init__(self, ts: float, kind: str, symbol: str, title: str, lines: Sequence[str]) -> None:
        self.ts = ts
        self.kind = kind
        self.symbol = symbol
        self.title = title
        self.lines = list(lines)


class Candle:
    """Rolling candle metrics: Body_Pct, Range_Pct, CLV, Buy/Sell volume, Turnover_Cr, Buy_Pct."""

    __slots__ = (
        "open", "high", "low", "close", "buy", "sell", "total",
        "body_pct", "range_pct", "clv", "buy_pct", "sell_pct", "turnover_cr",
    )

    def __init__(self, o: float, h: float, low: float, c: float, buy: int, sell: int, total: int) -> None:
        self.open = o
        self.high = h
        self.low = low
        self.close = c
        self.buy = buy
        self.sell = sell
        self.total = total
        self.body_pct = (c - o) / o * 100.0 if o > 0.0 else 0.0
        self.range_pct = (h - low) / o * 100.0 if o > 0.0 else 0.0
        spread = h - low
        # CLV = (Close - Low) / max(0.01, High - Low). A flat window (High == Low) has no close
        # location, so it reads as neutral 0.5 instead of 0 ("closed at the low").
        self.clv = (c - low) / max(0.01, spread) if spread > 0.0 else 0.5
        denom = max(1, total)
        self.buy_pct = buy / denom * 100.0
        self.sell_pct = sell / denom * 100.0
        self.turnover_cr = total * c / 1e7


def combine_candles(parts: Sequence[Candle]) -> Candle:
    return Candle(
        parts[0].open,
        max(p.high for p in parts),
        min(p.low for p in parts),
        parts[-1].close,
        sum(p.buy for p in parts),
        sum(p.sell for p in parts),
        sum(p.total for p in parts),
    )


class RollingWindow:
    """Sliding aggregate over finalized 1-second buckets with O(1) amortised push/evict.

    Volume sums are running totals; window high/low come from monotonic deques. Buckets evicted
    from one window can be handed to an older window (5m slice pipeline W3 -> W2 -> W1).
    """

    __slots__ = ("items", "hi_q", "lo_q", "buy", "sell", "tot")

    def __init__(self) -> None:
        self.items: deque = deque()
        self.hi_q: deque = deque()
        self.lo_q: deque = deque()
        self.buy = 0
        self.sell = 0
        self.tot = 0

    def clear(self) -> None:
        self.items.clear()
        self.hi_q.clear()
        self.lo_q.clear()
        self.buy = 0
        self.sell = 0
        self.tot = 0

    def push(self, bucket: tuple) -> None:
        self.items.append(bucket)
        self.buy += bucket[B_BUY]
        self.sell += bucket[B_SELL]
        self.tot += bucket[B_TOT]
        high = bucket[B_H]
        hi_q = self.hi_q
        while hi_q and hi_q[-1][B_H] <= high:
            hi_q.pop()
        hi_q.append(bucket)
        low = bucket[B_L]
        lo_q = self.lo_q
        while lo_q and lo_q[-1][B_L] >= low:
            lo_q.pop()
        lo_q.append(bucket)

    def evict_before(self, min_ts: int, sink: Optional["RollingWindow"] = None) -> None:
        items = self.items
        if not items or items[0][B_TS] >= min_ts:
            return
        while items and items[0][B_TS] < min_ts:
            bucket = items.popleft()
            self.buy -= bucket[B_BUY]
            self.sell -= bucket[B_SELL]
            self.tot -= bucket[B_TOT]
            if sink is not None:
                sink.push(bucket)
        hi_q = self.hi_q
        while hi_q and hi_q[0][B_TS] < min_ts:
            hi_q.popleft()
        lo_q = self.lo_q
        while lo_q and lo_q[0][B_TS] < min_ts:
            lo_q.popleft()

    def candle(self) -> Optional[Candle]:
        items = self.items
        if not items:
            return None
        return Candle(items[0][B_O], self.hi_q[0][B_H], self.lo_q[0][B_L], items[-1][B_C], self.buy, self.sell, self.tot)


class StockState:
    """Per-stock live state. Mutated only by the QuantEngine thread."""

    __slots__ = (
        "symbol", "token", "company", "industry",
        "ltp", "atp", "bid", "ask", "cum_vol", "need_baseline", "last_dir",
        "prev_close", "day_open", "day_high", "day_low",
        "cur", "history", "w1m", "w3", "w2", "w1", "first_ts", "last_tick_ts",
        "sess_buy", "sess_sell", "ms_init", "ms_up", "ms_down", "hi_ref", "lo_ref", "raw",
    )

    def __init__(self, inst: Instrument) -> None:
        self.symbol = inst.symbol
        self.token = inst.token
        self.company = inst.company
        self.industry = inst.industry
        self.history: deque = deque(maxlen=HISTORY_SECONDS)
        self.w1m = RollingWindow()  # [t-59s, t]
        self.w3 = RollingWindow()  # 5m slice W3: [t-299s, t]      (also the rolling 5m window)
        self.w2 = RollingWindow()  # 5m slice W2: [t-599s, t-300s]
        self.w1 = RollingWindow()  # 5m slice W1: [t-899s, t-600s]
        self.raw: Dict[str, float] = {}  # legacy feed: merged partial updates
        self.reset_day()

    def reset_day(self) -> None:
        self.ltp = 0.0
        self.atp = 0.0
        self.bid = 0.0
        self.ask = 0.0
        self.cum_vol = 0
        self.need_baseline = True
        self.last_dir = 0
        self.prev_close = 0.0
        self.day_open = 0.0
        self.day_high = 0.0
        self.day_low = 0.0
        self.cur: Optional[list] = None
        self.history.clear()
        for window in (self.w1m, self.w3, self.w2, self.w1):
            window.clear()
        self.first_ts = 0
        self.last_tick_ts = 0
        self.sess_buy = 0
        self.sess_sell = 0
        self.ms_init = False
        self.ms_up = 0
        self.ms_down = 0
        self.hi_ref = 0.0
        self.lo_ref = 0.0
        self.raw.clear()


class _Control:
    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f"<{self.name}>"


RESYNC = _Control("RESYNC")  # pushed on every (re)connect: re-baseline cumulative volume


class QuantEngine(threading.Thread):
    """T2: tick classification, 1-second buckets, rolling candles, triggers, market pulse."""

    def __init__(
        self,
        instruments: Sequence[Instrument],
        tick_q: "queue.Queue",
        alert_q: "queue.Queue",
        stop: threading.Event,
        clock: "MarketClock",
        log: logging.Logger,
    ) -> None:
        super().__init__(name="QuantEngine", daemon=True)
        self.stocks = [StockState(inst) for inst in instruments]
        self.by_token = {st.token: st for st in self.stocks}
        self.tick_q = tick_q
        self.alert_q = alert_q
        self.stop = stop
        self.log = log
        self.open_sod = clock.open_sod
        self.close_sod = clock.close_sod
        self.warmup_end_sod = clock.warmup_end_sod
        self.breakout_sod = clock.breakout_sod
        self.dirty: set = set()
        self.cooldown: Dict[Tuple[str, str], int] = {}
        now = time.time()
        self.clock_sec = int(now)
        self.day = ist_day(now)
        self.session_open_epoch = self._session_open_epoch(self.day)
        self.next_pulse_sod = self._next_pulse_boundary(ist_sod(now))
        self.processed = 0
        self.errors = 0
        self.lag_ms = 0.0
        self.resyncs = 0
        self.alerts = 0
        self.suppressed = 0
        self.alerts_dropped = 0
        self._last_error_log = 0.0

    # ── thread loop ─────────────────────────────────────────────────────────────────────────
    def run(self) -> None:
        get = self.tick_q.get
        empty = queue.Empty
        stop = self.stop
        self.log.info("QuantEngine started for %d instruments", len(self.stocks))
        while not stop.is_set():
            try:
                recv_ts, payload = get(timeout=0.2)
            except empty:
                now_sec = int(time.time())
                if now_sec > self.clock_sec:
                    self.advance(now_sec)
                continue
            sec = int(recv_ts)
            if sec > self.clock_sec:
                self.advance(sec)  # every tick of earlier seconds is already applied
            elif sec < self.clock_sec:
                sec = self.clock_sec  # late or clock-skewed tick: fold into the open second
            try:
                self.ingest(sec, payload)
            except Exception:
                self._record_error("tick processing")
            self.processed += 1
            if not self.processed & 0xFF:
                self.lag_ms = (time.time() - recv_ts) * 1000.0
        self.log.info("QuantEngine stopped after %d ticks", self.processed)

    def advance(self, sec: int) -> None:
        try:
            self.on_new_second(sec)
        except Exception:
            self._record_error("per-second evaluation")
        finally:
            self.clock_sec = sec

    def _record_error(self, where: str) -> None:
        self.errors += 1
        now = time.time()
        if now - self._last_error_log >= 10.0:
            self._last_error_log = now
            self.log.exception("QuantEngine error during %s (total errors: %d)", where, self.errors)

    # ── ingestion ───────────────────────────────────────────────────────────────────────────
    def ingest(self, sec: int, payload: Any) -> None:
        if payload is RESYNC:
            for st in self.stocks:
                st.need_baseline = True
            self.resyncs += 1
            return
        if isinstance(payload, dict):
            self._ingest_legacy(sec, payload)
            return
        if isinstance(payload, (str, bytes)):
            return
        mtype = getattr(payload, "type", None)
        if mtype == "scrip":
            self._ingest_sfeed(sec, payload)
        elif mtype == "market_status":
            self.log.info(
                "Exchange status: %s (%s)", getattr(payload, "status", "?"), getattr(payload, "exchange_segment", "?")
            )

    def _ingest_sfeed(self, sec: int, msg: Any) -> None:
        st = self.by_token.get(msg.instrument_token)
        if st is None:
            return
        buy = msg.buy
        sell = msg.sell
        self._apply(
            st,
            sec,
            msg.last_traded_price,
            msg.volume_traded_today,
            msg.average_trade_price,
            buy[0].price if buy else 0.0,
            sell[0].price if sell else 0.0,
            msg.open_price,
            msg.high_price,
            msg.low_price,
            msg.close_price,
        )

    def _ingest_legacy(self, sec: int, payload: Dict[str, Any]) -> None:
        if payload.get("type") != "stock_feed":
            return
        data = payload.get("data")
        if not data:
            return
        by_token = self.by_token
        for item in data:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            if name is not None and name != "sf":
                continue
            st = by_token.get(item.get("tk"))
            if st is None:
                continue
            raw = st.raw  # legacy ticks carry only the fields that changed: merge them
            for key in LEGACY_FIELDS:
                value = item.get(key)
                if value is not None:
                    with contextlib.suppress(TypeError, ValueError):
                        raw[key] = float(value)
            ltp = raw.get("ltp")
            vol = raw.get("v")
            if ltp is None or vol is None:
                continue
            self._apply(
                st, sec, ltp, int(vol), raw.get("ap", 0.0), raw.get("bp", 0.0), raw.get("sp", 0.0),
                raw.get("op", 0.0), raw.get("h", 0.0), raw.get("lo", 0.0), raw.get("c", 0.0),
            )

    def _apply(
        self, st: StockState, sec: int, ltp: float, cum_vol: int, atp: float, bid: float, ask: float,
        day_open: float, day_high: float, day_low: float, prev_close: float,
    ) -> None:
        if ltp <= 0.0:
            return
        if prev_close > 0.0:
            st.prev_close = prev_close
        if day_open > 0.0:
            st.day_open = day_open
        if day_high > 0.0:
            st.day_high = day_high
        if day_low > 0.0:
            st.day_low = day_low
        if atp > 0.0:
            st.atp = atp

        # Executed-volume delta + Lee-Ready / EMO classification.
        prev_ltp = st.ltp
        delta = 0
        side = 0
        if st.need_baseline:
            st.cum_vol = cum_vol
            st.need_baseline = False
        elif cum_vol > st.cum_vol:
            delta = cum_vol - st.cum_vol
            st.cum_vol = cum_vol
            ref_bid = st.bid if st.bid > 0.0 else bid  # quote prevailing before this print
            ref_ask = st.ask if st.ask > 0.0 else ask
            if not (ref_bid > 0.0 and ref_ask > 0.0 and ref_bid >= ref_ask):  # skip crossed books
                if ref_ask > 0.0 and ltp >= ref_ask:
                    side = 1  # lifted the offer: aggressive buyer
                elif ref_bid > 0.0 and ltp <= ref_bid:
                    side = -1  # hit the bid: aggressive seller
            if side == 0 and prev_ltp > 0.0:
                if ltp > prev_ltp:
                    side = 1
                elif ltp < prev_ltp:
                    side = -1
                else:
                    side = st.last_dir  # zero tick inherits the last non-zero move
        elif cum_vol < st.cum_vol:
            st.cum_vol = cum_vol  # exchange correction / feed reset: re-baseline silently

        if prev_ltp > 0.0 and ltp != prev_ltp:
            st.last_dir = 1 if ltp > prev_ltp else -1
        st.ltp = ltp
        st.bid = bid
        st.ask = ask
        st.last_tick_ts = sec

        sod = (sec + IST_OFFSET_SEC) % 86_400
        if sod < self.open_sod or sod >= self.close_sod:
            return  # pre-open / post-close prints only move the baseline
        cur = st.cur
        if cur is None or cur[B_TS] != sec:
            if cur is not None:
                self._finalize(st, cur)
            cur = [sec, ltp, ltp, ltp, ltp, 0, 0, 0, st.atp]
            st.cur = cur
            if not st.first_ts:
                st.first_ts = sec
            self.dirty.add(st)
        else:
            if ltp > cur[B_H]:
                cur[B_H] = ltp
            elif ltp < cur[B_L]:
                cur[B_L] = ltp
            cur[B_C] = ltp
            cur[B_ATP] = st.atp
        if delta:
            cur[B_TOT] += delta
            if side > 0:
                cur[B_BUY] += delta
                st.sess_buy += delta
            elif side < 0:
                cur[B_SELL] += delta
                st.sess_sell += delta

    @staticmethod
    def _finalize(st: StockState, cur: list) -> None:
        bucket = tuple(cur)
        st.history.append(bucket)
        st.w1m.push(bucket)
        st.w3.push(bucket)

    @staticmethod
    def _evict(st: StockState, end: int) -> None:
        st.w1m.evict_before(end - (WIN_1M - 1))
        st.w3.evict_before(end - (WIN_5M - 1), st.w2)
        st.w2.evict_before(end - (2 * WIN_5M - 1), st.w1)
        st.w1.evict_before(end - (WIN_15M - 1))

    # ── per-second evaluation ───────────────────────────────────────────────────────────────
    def _session_open_epoch(self, day: int) -> int:
        return day * 86_400 - IST_OFFSET_SEC + self.open_sod

    def _next_pulse_boundary(self, sod: int) -> int:
        return self.open_sod + ((sod - self.open_sod) // PULSE_EVERY_SEC + 1) * PULSE_EVERY_SEC

    def _new_day(self, day: int, sod: int) -> None:
        for st in self.stocks:
            st.reset_day()
        self.dirty.clear()
        self.cooldown.clear()
        self.day = day
        self.session_open_epoch = self._session_open_epoch(day)
        self.next_pulse_sod = self._next_pulse_boundary(sod)
        self.log.info("New IST trading day %s: per-stock state reset", ist_date(self.session_open_epoch))

    def on_new_second(self, sec: int) -> None:
        """Close second ``sec - 1``: finalize buckets, slide windows, evaluate dirty stocks."""
        day = ist_day(sec)
        if day != self.day:
            self._new_day(day, ist_sod(sec))
        end = sec - 1
        sod_end = ist_sod(end)
        alerts_on = self.warmup_end_sod <= sod_end < self.close_sod
        breakouts_on = sod_end >= self.breakout_sod
        if self.dirty:
            dirty = self.dirty
            self.dirty = set()
            for st in dirty:
                try:
                    cur = st.cur
                    if cur is not None and cur[B_TS] < sec:
                        self._finalize(st, cur)
                        st.cur = None
                    self._evict(st, end)
                    self._evaluate(st, end, alerts_on, breakouts_on)
                except Exception:
                    self._record_error(f"evaluation of {st.symbol}")
        sod = ist_sod(sec)
        if sod >= self.next_pulse_sod:
            boundary = self.next_pulse_sod
            self.next_pulse_sod = self._next_pulse_boundary(sod)
            if self.warmup_end_sod <= boundary <= self.close_sod:
                self._market_pulse(end)

    def _evaluate(self, st: StockState, end: int, alerts_on: bool, breakouts_on: bool) -> None:
        ltp = st.ltp
        if ltp <= 0.0:
            return
        c1 = st.w1m.candle()
        c5 = st.w3.candle()
        can_alert = alerts_on and c5 is not None and c5.turnover_cr >= MIN_TURNOVER_5M_CR
        coverage = end - st.first_ts + 1 if st.first_ts else 0

        # ⚡ Day milestones: every ±1.5% step of the day change. Levels already reached at
        # start-up or during the warm-up are absorbed silently; only new steps alert.
        prev_close = st.prev_close
        if prev_close > 0.0:
            change = (ltp - prev_close) / prev_close * 100.0
            up = int(change / MILESTONE_STEP_PCT + 1e-9) if change > 0.0 else 0
            down = int(-change / MILESTONE_STEP_PCT + 1e-9) if change < 0.0 else 0
            if not st.ms_init:
                st.ms_init = True
                st.ms_up = up
                st.ms_down = down
            else:
                if up > st.ms_up:
                    if can_alert:
                        self._alert_milestone(st, end, 1, up, change, c5)
                    st.ms_up = up
                if down > st.ms_down:
                    if can_alert:
                        self._alert_milestone(st, end, -1, down, change, c5)
                    st.ms_down = down

        # ⚡ Fresh day high / low after 09:25, confirmed by 1m executed volume delta.
        if st.day_high > 0.0:
            if st.hi_ref <= 0.0:
                st.hi_ref = st.day_high
            elif st.day_high > st.hi_ref:
                if (can_alert and breakouts_on and c1 is not None and c1.buy > c1.sell
                        and c1.buy_pct >= BREAKOUT_FLOW_PCT):
                    self._alert_breakout(st, end, 1, st.hi_ref, c1, c5)
                st.hi_ref = st.day_high
        if st.day_low > 0.0:
            if st.lo_ref <= 0.0:
                st.lo_ref = st.day_low
            elif st.day_low < st.lo_ref:
                if (can_alert and breakouts_on and c1 is not None and c1.sell > c1.buy
                        and c1.sell_pct >= BREAKOUT_FLOW_PCT):
                    self._alert_breakout(st, end, -1, st.lo_ref, c1, c5)
                st.lo_ref = st.day_low

        if not can_alert:
            return
        c5_full = c5 if coverage >= WIN_5M else None
        c1_full = c1 if coverage >= WIN_1M else None
        vwap = st.atp

        # 🚀 / 🩸 Fast rolling moves (the window that shows the move must show the flow too).
        if vwap > 0.0 and ltp > vwap:
            hit = None
            if (c5_full is not None and c5_full.body_pct >= FAST_BODY_5M_PCT
                    and c5_full.buy_pct >= FAST_FLOW_PCT and c5_full.clv >= FAST_CLV_UP):
                hit = ("5m", c5_full)
            elif (c1_full is not None and c1_full.body_pct >= FAST_BODY_1M_PCT
                    and c1_full.buy_pct >= FAST_FLOW_PCT and c1_full.clv >= FAST_CLV_UP):
                hit = ("1m", c1_full)
            if hit is not None:
                self._alert_fast(st, end, 1, hit[0], hit[1], c1, c5)
        elif 0.0 < ltp < vwap:
            hit = None
            if (c5_full is not None and c5_full.body_pct <= -FAST_BODY_5M_PCT
                    and c5_full.sell_pct >= FAST_FLOW_PCT and c5_full.clv <= FAST_CLV_DOWN):
                hit = ("5m", c5_full)
            elif (c1_full is not None and c1_full.body_pct <= -FAST_BODY_1M_PCT
                    and c1_full.sell_pct >= FAST_FLOW_PCT and c1_full.clv <= FAST_CLV_DOWN):
                hit = ("1m", c1_full)
            if hit is not None:
                self._alert_fast(st, end, -1, hit[0], hit[1], c1, c5)

        # 📈 / 📉 Slow steady trend: three consecutive 5m slices of the trailing 15m.
        if coverage >= WIN_15M and st.w1.items and st.w2.items and c5_full is not None:
            a = st.w1.candle()
            b = st.w2.candle()
            c = c5_full
            if a is not None and b is not None:
                if c.high > b.high > a.high and c.low > b.low > a.low and c.close > b.close > a.close:
                    c15 = combine_candles((a, b, c))
                    if c15.body_pct >= SLOW_MIN_MOVE_PCT and c15.buy_pct >= SLOW_FLOW_PCT:
                        self._alert_slow(st, end, 1, a, b, c, c15)
                elif c.high < b.high < a.high and c.low < b.low < a.low and c.close < b.close < a.close:
                    c15 = combine_candles((a, b, c))
                    if c15.body_pct <= -SLOW_MIN_MOVE_PCT and c15.sell_pct >= SLOW_FLOW_PCT:
                        self._alert_slow(st, end, -1, a, b, c, c15)

        # 🛡️ / 🧱 Absorption: heavy one-sided aggression that fails to move price.
        # Average 5m volume = session volume before this window (exchange cumulative volume)
        # scaled to 300 s; needs at least one full 5m period of history before the window.
        if c5_full is not None:
            pre_window_sec = (end - (WIN_5M - 1)) - self.session_open_epoch
            prior_vol = st.cum_vol - c5_full.total
            if pre_window_sec >= WIN_5M and prior_vol > 0:
                avg_5m = prior_vol * WIN_5M / pre_window_sec
                multiple = c5_full.total / avg_5m
                if multiple >= ABSORB_VOL_MULT:
                    if c5_full.sell_pct >= ABSORB_FLOW_PCT and (
                        c5_full.body_pct > ABSORB_BUYER_MIN_BODY_PCT or c5_full.clv >= ABSORB_BUYER_MIN_CLV
                    ):
                        self._alert_absorption(st, end, 1, c5_full, multiple, avg_5m)
                    elif c5_full.buy_pct >= ABSORB_FLOW_PCT and (
                        c5_full.body_pct < ABSORB_SELLER_MAX_BODY_PCT or c5_full.clv <= ABSORB_SELLER_MAX_CLV
                    ):
                        self._alert_absorption(st, end, -1, c5_full, multiple, avg_5m)

    # ── alert construction ──────────────────────────────────────────────────────────────────
    def _emit(self, st_symbol: str, kind: str, end: int, title: str, lines: Sequence[str]) -> bool:
        key = (st_symbol, kind)
        last = self.cooldown.get(key)
        if last is not None and end - last < COOLDOWN_SEC:
            self.suppressed += 1
            return False
        self.cooldown[key] = end
        try:
            self.alert_q.put_nowait(Alert(float(end), kind, st_symbol, title, lines))
            self.alerts += 1
        except queue.Full:
            self.alerts_dropped += 1
        return True

    @staticmethod
    def _price_line(st: StockState) -> str:
        parts = [f"LTP {fmt_price(st.ltp)}"]
        if st.prev_close > 0.0:
            parts.append(f"Day {fmt_pct((st.ltp - st.prev_close) / st.prev_close * 100.0)}")
        if st.atp > 0.0:
            where = "above" if st.ltp > st.atp else "below" if st.ltp < st.atp else "at"
            parts.append(f"VWAP {fmt_price(st.atp)} (LTP {where})")
        return " | ".join(parts)

    @staticmethod
    def _flow_line(label: str, c: Candle) -> str:
        return f"Flow {label}: Buy {c.buy_pct:.1f}% | Sell {c.sell_pct:.1f}% | Net {fmt_signed_qty(c.buy - c.sell)} sh"

    @staticmethod
    def _candle_line(label: str, c: Candle) -> str:
        return f"{label} candle: Body {fmt_pct(c.body_pct)} | Range {c.range_pct:.2f}% | CLV {c.clv:.2f}"

    def _alert_fast(
        self, st: StockState, end: int, direction: int, label: str, c: Candle,
        c1: Optional[Candle], c5: Optional[Candle],
    ) -> None:
        other = c1 if label == "5m" else c5
        other_label = "1m" if label == "5m" else "5m"
        lines = [
            self._price_line(st),
            self._candle_line(label, c),
            self._flow_line(label, c),
            f"Turnover {label}: {fmt_inr_cr(c.turnover_cr)}",
        ]
        if other is not None:
            lines.append(f"{other_label}: Body {fmt_pct(other.body_pct)} | Buy {other.buy_pct:.1f}% | Sell {other.sell_pct:.1f}%")
        if direction > 0:
            self._emit(st.symbol, "FAST_UP", end, TITLE_FAST_UP, lines)
        else:
            self._emit(st.symbol, "FAST_DOWN", end, TITLE_FAST_DOWN, lines)

    def _alert_slow(
        self, st: StockState, end: int, direction: int, a: Candle, b: Candle, c: Candle, c15: Candle
    ) -> None:
        lines = [
            self._price_line(st),
            f"15m move {fmt_pct(c15.body_pct)} | Buy {c15.buy_pct:.1f}% | Sell {c15.sell_pct:.1f}%",
            f"5m highs: {a.high:,.2f} → {b.high:,.2f} → {c.high:,.2f}",
            f"5m lows: {a.low:,.2f} → {b.low:,.2f} → {c.low:,.2f}",
            f"5m closes: {a.close:,.2f} → {b.close:,.2f} → {c.close:,.2f}",
            f"Turnover 15m: {fmt_inr_cr(c15.turnover_cr)}",
        ]
        if direction > 0:
            self._emit(st.symbol, "SLOW_ACCUM", end, TITLE_SLOW_ACCUM, lines)
        else:
            self._emit(st.symbol, "SLOW_DIST", end, TITLE_SLOW_DIST, lines)

    def _alert_absorption(
        self, st: StockState, end: int, direction: int, c5: Candle, multiple: float, avg_5m: float
    ) -> None:
        if direction > 0:
            verdict = f"Sellers hit {c5.sell_pct:.1f}% of volume yet 5m body is {fmt_pct(c5.body_pct)}"
        else:
            verdict = f"Buyers lifted {c5.buy_pct:.1f}% of volume yet 5m body is {fmt_pct(c5.body_pct)}"
        lines = [
            self._price_line(st),
            f"5m volume {multiple:.1f}× avg ({fmt_qty(c5.total)} vs {fmt_qty(avg_5m)} sh)",
            verdict,
            self._candle_line("5m", c5),
            self._flow_line("5m", c5),
            f"Turnover 5m: {fmt_inr_cr(c5.turnover_cr)}",
        ]
        if direction > 0:
            self._emit(st.symbol, "HIDDEN_BUYER", end, TITLE_HIDDEN_BUYER, lines)
        else:
            self._emit(st.symbol, "HIDDEN_SELLER", end, TITLE_HIDDEN_SELLER, lines)

    def _alert_milestone(
        self, st: StockState, end: int, direction: int, level: int, change: float, c5: Optional[Candle]
    ) -> None:
        step = level * MILESTONE_STEP_PCT
        if direction > 0:
            title = f"⚡ DAY MILESTONE +{step:.1f}% (दिन में +{step:.1f}% की बढ़त)"
            kind = f"MILESTONE_UP_{level}"
        else:
            title = f"⚡ DAY MILESTONE -{step:.1f}% (दिन में -{step:.1f}% की गिरावट)"
            kind = f"MILESTONE_DOWN_{level}"
        lines = [
            self._price_line(st),
            f"Prev close {fmt_price(st.prev_close)} | Day change {fmt_pct(change)}",
            f"Day O/H/L: {st.day_open:,.2f} / {st.day_high:,.2f} / {st.day_low:,.2f}",
        ]
        if c5 is not None:
            lines.append(self._flow_line("5m", c5))
            lines.append(f"Turnover 5m: {fmt_inr_cr(c5.turnover_cr)}")
        self._emit(st.symbol, kind, end, title, lines)

    def _alert_breakout(
        self, st: StockState, end: int, direction: int, previous: float, c1: Candle, c5: Optional[Candle]
    ) -> None:
        if direction > 0:
            level_line = f"New day high {fmt_price(st.day_high)} (previous {fmt_price(previous)})"
        else:
            level_line = f"New day low {fmt_price(st.day_low)} (previous {fmt_price(previous)})"
        lines = [level_line, self._price_line(st), self._flow_line("1m", c1)]
        if c5 is not None:
            lines.append(f"Turnover 5m: {fmt_inr_cr(c5.turnover_cr)} | 5m Body {fmt_pct(c5.body_pct)}")
        if direction > 0:
            self._emit(st.symbol, "DAY_HIGH", end, TITLE_DAY_HIGH, lines)
        else:
            self._emit(st.symbol, "DAY_LOW", end, TITLE_DAY_LOW, lines)

    def _market_pulse(self, end: int) -> None:
        advancing = declining = unchanged = above = below = net_buy = net_sell = live = 0
        net_flow_cr = 0.0
        for st in self.stocks:
            if st.ltp <= 0.0:
                continue
            live += 1
            if st.prev_close > 0.0:
                if st.ltp > st.prev_close:
                    advancing += 1
                elif st.ltp < st.prev_close:
                    declining += 1
                else:
                    unchanged += 1
            if st.atp > 0.0:
                if st.ltp > st.atp:
                    above += 1
                elif st.ltp < st.atp:
                    below += 1
            if st.cur is not None and st.cur[B_TS] <= end:
                self._finalize(st, st.cur)
                st.cur = None
            self._evict(st, end)
            window = st.w3
            if window.tot > 0:
                if window.buy > window.sell:
                    net_buy += 1
                elif window.sell > window.buy:
                    net_sell += 1
                net_flow_cr += (window.buy - window.sell) * st.ltp / 1e7
        ratio = f"{advancing / declining:.2f}" if declining else "∞" if advancing else "-"
        lines = [
            f"Advancing {advancing} | Declining {declining} | Unchanged {unchanged} | A/D {ratio}",
            f"Above VWAP {above} | Below VWAP {below}",
            f"Net aggressive buying (5m) {net_buy} | Net aggressive selling (5m) {net_sell}",
            f"Net 5m executed flow: {fmt_signed_inr_cr(net_flow_cr)}",
            f"Live stocks {live}/{len(self.stocks)}",
        ]
        try:
            self.alert_q.put_nowait(Alert(float(end + 1), "MARKET_PULSE", "", TITLE_PULSE, lines))
            self.alerts += 1
        except queue.Full:
            self.alerts_dropped += 1

    def live_count(self) -> int:
        return sum(1 for st in self.stocks if st.ltp > 0.0)


# ════════════════════════════════════════════════════════════════════════════════════════════
# Market clock (IST session windows)
# ════════════════════════════════════════════════════════════════════════════════════════════
class MarketClock:
    def __init__(self, open_sod: int, close_sod: int) -> None:
        self.open_sod = open_sod
        self.close_sod = close_sod
        self.warmup_end_sod = open_sod + WARMUP_SEC
        self.breakout_sod = open_sod + BREAKOUT_DELAY_SEC
        self.feed_start_sod = max(0, open_sod - PRE_OPEN_CONNECT_SEC)
        self.watchdog_start_sod = open_sod + WATCHDOG_GRACE_SEC
        self.rollover_sod = min(86_399, close_sod + SESSION_ROLLOVER_SEC)

    def in_feed_window(self, now: float) -> bool:
        """(Re)connect window: 09:00-15:30 IST by default."""
        return self.feed_start_sod <= ist_sod(now) < self.close_sod

    def in_watchdog_window(self, now: float) -> bool:
        """Stale-feed detection: weekdays 09:15:30-15:30 IST."""
        return ist_weekday(now) < 5 and self.watchdog_start_sod <= ist_sod(now) < self.close_sod

    def session_over(self, connected_at: float, now: float) -> bool:
        """True once a socket outlives its trading day (forces a fresh login + snapshot)."""
        if connected_at <= 0.0:
            return False
        if ist_day(now) > ist_day(connected_at):
            return True
        return ist_sod(connected_at) < self.rollover_sod <= ist_sod(now)

    def exit_epoch(self, now: float) -> float:
        return float(ist_day(now) * 86_400 - IST_OFFSET_SEC + self.close_sod + 60)


# ════════════════════════════════════════════════════════════════════════════════════════════
# T1: WebSocket producer
# ════════════════════════════════════════════════════════════════════════════════════════════
class TickProducer:
    """on_message() only stamps the receive time and enqueues the raw payload (put_nowait)."""

    __slots__ = ("q", "received", "dropped", "last_msg_ts")

    def __init__(self, tick_q: "queue.Queue") -> None:
        self.q = tick_q
        self.received = 0
        self.dropped = 0
        self.last_msg_ts = 0.0

    def on_message(self, message: Any) -> None:
        now = time.time()
        self.last_msg_ts = now
        try:
            self.q.put_nowait((now, message))
            self.received += 1
        except queue.Full:
            self.dropped += 1  # cumulative volume means the next update still carries this volume

    def push_control(self, control: _Control) -> None:
        with contextlib.suppress(queue.Full):
            self.q.put_nowait((time.time(), control))


class FeedBase(threading.Thread):
    """Shared supervision for both SDK generations: health checks, backoff, session refresh."""

    kind = "base"

    def __init__(
        self,
        session: KotakSession,
        instruments: Sequence[Instrument],
        producer: TickProducer,
        stop: threading.Event,
        clock: MarketClock,
        cfg: Config,
        log: logging.Logger,
    ) -> None:
        super().__init__(name="FeedProducer", daemon=True)
        self.session = session
        self.instruments = list(instruments)
        self.producer = producer
        self.stop = stop
        self.clock = clock
        self.log = log
        self.batch_size = cfg.ws_batch_size
        self.stale_sec = cfg.stale_feed_sec
        self.connected = False
        self.connected_at = 0.0
        self.rx_at_connect = 0
        self.connections = 0
        self.failures = 0
        self._paused_logged = False
        self._last_error_log = 0.0

    def _mark_connected(self) -> None:
        self.connected = True
        self.connected_at = time.time()
        self.rx_at_connect = self.producer.received
        self.connections += 1
        self.log.info(
            "%s feed live: %d instruments subscribed (connection #%d)", self.kind, len(self.instruments), self.connections
        )

    def _health_check(self, now: float) -> Optional[str]:
        if self.clock.session_over(self.connected_at, now):
            return "Trading session rolled over"
        if not self.clock.in_watchdog_window(now):
            return None
        silent = now - max(self.producer.last_msg_ts, self.connected_at)
        got_data = self.producer.received > self.rx_at_connect
        limit = self.stale_sec if got_data else FEED_NO_DATA_SEC
        if silent > limit:
            return f"No market data for {silent:.0f}s"
        return None

    def _next_delay(self, got_data: bool) -> float:
        if got_data:
            self.failures = 0
            return 2.0
        self.failures += 1
        if self.failures % 3 == 0:
            self.session.invalidate()  # repeated dead connections: force a fresh login
        return min(60.0, 2.0 ** min(self.failures, 6)) + random.uniform(0.0, 1.0)

    def _log_paused_once(self) -> None:
        if not self._paused_logged:
            self._paused_logged = True
            self.log.info(
                "Outside the market window; feed reconnects at %s IST", sod_to_hhmm(self.clock.feed_start_sod)
            )

    def _log_feed_error(self, error: Any) -> None:
        now = time.time()
        if now - self._last_error_log >= 10.0:
            self._last_error_log = now
            self.log.warning("%s WebSocket error: %s", self.kind, error)


class SFeedFeed(FeedBase):
    """Current SDK (kotakneoapi >= 2.2): async SFeed client driven by an asyncio loop in T1.

    ws.on_message is the producer callback (runs inline in the SDK receive loop). The SDK also
    copies every message into its own unbounded asyncio.Queue, so this thread drains it with
    ``async for`` to keep memory flat; that loop also ends when the SDK gives up reconnecting.
    """

    kind = "SFeed"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.ws_tokens = [SFeedWsToken(SEGMENT, inst.token) for inst in self.instruments]
        self.drained = 0

    def run(self) -> None:
        try:
            asyncio.run(self._main())
        except Exception:
            self.log.exception("SFeed producer crashed")
        finally:
            self.connected = False

    async def _sleep(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while not self.stop.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0.0:
                return
            await asyncio.sleep(min(0.5, remaining))

    async def _main(self) -> None:
        attempt_now = True
        while not self.stop.is_set():
            if not attempt_now and not self.clock.in_feed_window(time.time()):
                self._log_paused_once()
                await self._sleep(5.0)
                continue
            self._paused_logged = False
            attempt_now = False
            got_data = await self._run_connection()
            if self.stop.is_set():
                break
            delay = self._next_delay(got_data)
            self.log.warning("SFeed connection ended; reconnecting in %.1fs", delay)
            await self._sleep(delay)

    async def _run_connection(self) -> bool:
        rx_before = self.producer.received
        ws = None
        watchdog = None
        try:
            client = self.session.ensure_fresh()
            ws = client.create_websocket()
            ws.on_message = self.producer.on_message
            ws.on_connect = self._on_connect
            ws.on_disconnect = self._on_disconnect
            ws.on_error = self._log_feed_error
            await ws.connect()
            total = len(self.ws_tokens)
            for start in range(0, total, self.batch_size):
                batch = self.ws_tokens[start:start + self.batch_size]
                await ws.subscribe_scrips(batch)
                self.log.info("SFeed subscribed %d/%d", start + len(batch), total)
                if self.stop.is_set():
                    break
            self._mark_connected()
            watchdog = asyncio.create_task(self._watchdog(ws))
            try:
                async for _message in ws:
                    self.drained += 1
            except SFeedNotConnectedError:
                self.log.info("SFeed iterator closed")
        except SFeedAuthError as exc:
            self.log.error("SFeed authentication failed (%s); the session will be refreshed", exc)
            self.session.invalidate()
        except LoginError as exc:
            self.log.error("Kotak login failed: %s", exc)
        except Exception as exc:
            self.log.error("SFeed error: %s: %s", type(exc).__name__, exc)
        finally:
            self.connected = False
            if watchdog is not None:
                watchdog.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await watchdog
            if ws is not None:
                with contextlib.suppress(Exception):
                    await ws.close()
        return self.producer.received > rx_before

    async def _watchdog(self, ws: Any) -> None:
        while True:
            await asyncio.sleep(1.0)
            if self.stop.is_set():
                self.log.info("Stop requested: closing SFeed WebSocket")
                self.connected = False
                await ws.close()
                return
            reason = self._health_check(time.time())
            if reason:
                self.log.warning("%s; recycling SFeed WebSocket", reason)
                self.connected = False
                await ws.close()
                return

    def _on_connect(self) -> None:
        self.producer.push_control(RESYNC)  # also fires after the SDK's internal auto-reconnect
        self.log.info("SFeed socket connected and authenticated")

    def _on_disconnect(self) -> None:
        if self.connected:
            self.log.warning("SFeed socket disconnected (SDK auto-reconnect in progress)")


class LegacyFeed(FeedBase):
    """Legacy SDK (Kotak-neo-api-v2): client.subscribe() + client.on_message callbacks.

    The SDK runs websocket-client on its own (non-daemon) thread and calls on_message there.
    It does not reconnect after an error (its error handler closes the socket), so this thread
    re-opens it; subscribe() with already-known tokens re-opens the socket and the SDK's
    connection handler re-subscribes every token.
    """

    kind = "Legacy"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._client: Any = None
        self._subscribed = False

    @staticmethod
    def _token_dicts(instruments: Sequence[Instrument]) -> List[Dict[str, str]]:
        # Fresh dicts per call: the SDK mutates them (adds 'subscription_type').
        return [{"instrument_token": inst.token, "exchange_segment": SEGMENT} for inst in instruments]

    def run(self) -> None:
        attempt_now = True
        try:
            while not self.stop.is_set():
                if not attempt_now and not self.clock.in_feed_window(time.time()):
                    self._log_paused_once()
                    self.stop.wait(5.0)
                    continue
                self._paused_logged = False
                attempt_now = False
                if not self._connect():
                    delay = self._next_delay(False)
                    self.log.warning("Legacy WebSocket connect failed; retrying in %.1fs", delay)
                    self.stop.wait(delay)
                    continue
                while not self.stop.is_set() and self.connected and self._socket_open():
                    reason = self._health_check(time.time())
                    if reason:
                        self.log.warning("%s; recycling legacy WebSocket", reason)
                        break
                    self.stop.wait(1.0)
                got_data = self.producer.received > self.rx_at_connect
                self._close_socket()
                if self.stop.is_set():
                    break
                delay = self._next_delay(got_data)
                self.log.warning("Legacy WebSocket closed; reconnecting in %.1fs", delay)
                self.stop.wait(delay)
        except Exception:
            self.log.exception("Legacy feed supervisor crashed")
        finally:
            self._close_socket()

    def _socket_open(self) -> bool:
        nws = getattr(self._client, "NeoWebSocket", None)
        return nws is not None and getattr(nws, "is_hsw_open", 0) == 1

    def _wait_open(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and not self.stop.is_set():
            if self._socket_open():
                return True
            time.sleep(0.1)
        return False

    def _connect(self) -> bool:
        try:
            client = self.session.ensure_fresh()
        except Exception as exc:
            self.log.error("Session refresh failed: %s", exc)
            return False
        try:
            if client is not self._client:
                if self._client is not None:
                    self._close_socket()
                client.on_message = self.producer.on_message
                client.on_error = self._on_error
                client.on_close = self._on_close
                client.on_open = self._on_open
                self._client = client
                self._subscribed = False
            if not self._subscribed:
                tokens = self._token_dicts(self.instruments)
                first, rest = tokens[: self.batch_size], tokens[self.batch_size:]
                client.subscribe(instrument_tokens=first, isIndex=False, isDepth=False)
                if not self._wait_open(LEGACY_OPEN_TIMEOUT_SEC):
                    self.log.error("Legacy WebSocket did not authenticate within %.0fs", LEGACY_OPEN_TIMEOUT_SEC)
                    self._close_socket()
                    return False
                done = len(first)
                for start in range(0, len(rest), self.batch_size):
                    batch = rest[start:start + self.batch_size]
                    client.subscribe(instrument_tokens=batch, isIndex=False, isDepth=False)
                    done += len(batch)
                    self.log.info("Legacy subscribed %d/%d", done, len(tokens))
                    time.sleep(0.25)
                self._subscribed = True
            else:
                nws = getattr(client, "NeoWebSocket", None)
                if nws is not None:
                    nws.is_hsw_open = 0  # the SDK only opens a new socket when it believes it is closed
                client.subscribe(instrument_tokens=self._token_dicts(self.instruments[:1]), isIndex=False, isDepth=False)
                if not self._wait_open(LEGACY_OPEN_TIMEOUT_SEC):
                    self.log.error("Legacy WebSocket did not re-open within %.0fs", LEGACY_OPEN_TIMEOUT_SEC)
                    self._close_socket()
                    return False
            self._mark_connected()
            return True
        except Exception as exc:
            self.log.error("Legacy subscribe failed: %s: %s", type(exc).__name__, exc)
            self._close_socket()
            return False

    def _close_socket(self) -> None:
        self.connected = False
        nws = getattr(self._client, "NeoWebSocket", None)
        if nws is None:
            return
        hs = getattr(nws, "hsWebsocket", None)
        if hs is not None:
            with contextlib.suppress(Exception):
                hs.close()
        with contextlib.suppress(Exception):
            nws.is_hsw_open = 0
        worker = getattr(nws, "hsw_thread", None)
        if worker is not None and worker.is_alive() and worker is not threading.current_thread():
            worker.join(timeout=5.0)

    def _on_open(self, *_args: Any) -> None:
        self.producer.push_control(RESYNC)
        self.log.info("Legacy WebSocket opened")

    def _on_close(self, *_args: Any) -> None:
        if self.connected:
            self.log.warning("Legacy WebSocket closed by server/network")
        self.connected = False

    def _on_error(self, *args: Any) -> None:
        self._log_feed_error(args[0] if args else "unknown error")
        self.connected = False


def make_feed(
    session: KotakSession, instruments: Sequence[Instrument], producer: TickProducer,
    stop: threading.Event, clock: MarketClock, cfg: Config, log: logging.Logger,
) -> FeedBase:
    client = session.client
    if callable(getattr(client, "create_websocket", None)) and SFeedWsToken is not None:
        return SFeedFeed(session, instruments, producer, stop, clock, cfg, log)
    if callable(getattr(client, "subscribe", None)):
        return LegacyFeed(session, instruments, producer, stop, clock, cfg, log)
    raise RuntimeError("Unsupported neo_api_client: neither create_websocket() nor subscribe() is available")


# ════════════════════════════════════════════════════════════════════════════════════════════
# T3: console + Telegram dispatcher
# ════════════════════════════════════════════════════════════════════════════════════════════
def format_console(alert: Alert) -> str:
    head = f"{alert.title} | {alert.symbol}" if alert.symbol else alert.title
    return f"{head} | " + " | ".join(alert.lines) if alert.lines else head


def format_html(alert: Alert) -> str:
    parts = [f"<b>{html.escape(alert.title, quote=False)}</b>"]
    if alert.symbol:
        parts.append(f"<b>{html.escape(alert.symbol, quote=False)}</b>")
    parts.extend(html.escape(line, quote=False) for line in alert.lines)
    parts.append(f"<i>🕒 {ist_hms(alert.ts)} IST</i>")
    return "\n".join(parts)


class TelegramSender:
    """Single-attempt sends with a non-blocking rate gate; the dispatcher owns retries."""

    def __init__(self, token: str, chat_id: str, min_interval: float, log: logging.Logger) -> None:
        self.token = token
        self.chat_id = chat_id
        self.min_interval = min_interval
        self.log = log
        self.enabled = bool(token and chat_id)
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.http = requests.Session()
        self.http.headers.update({"User-Agent": "kotak-nifty200-scanner/1.0"})
        self._next_at = 0.0
        self.sent = 0
        self.failed = 0

    def ready_in(self) -> float:
        return self._next_at - time.monotonic()

    def _scrub(self, text: str) -> str:
        return text.replace(self.token, "<bot-token>") if self.token else text

    def _disable(self, reason: str) -> None:
        self.enabled = False
        self.log.error("Telegram disabled: %s (alerts continue on console/log)", reason)

    def send_once(self, text: str, plain: bool = False) -> str:
        """Return 'ok', 'retry', 'plain' (resend without HTML) or 'drop'."""
        payload: Dict[str, Any] = {"chat_id": self.chat_id, "disable_web_page_preview": True}
        if plain:
            payload["text"] = html_to_text(text)
        else:
            payload["text"] = text
            payload["parse_mode"] = "HTML"
        try:
            resp = self.http.post(self.url, json=payload, timeout=(5, 15))
        except requests.RequestException as exc:
            self._next_at = time.monotonic() + 5.0
            self.log.warning("Telegram network error: %s", self._scrub(f"{type(exc).__name__}: {exc}")[:300])
            return "retry"
        self._next_at = time.monotonic() + self.min_interval
        if resp.status_code == 200:
            self.sent += 1
            return "ok"
        try:
            body = resp.json()
        except ValueError:
            body = {}
        description = str(body.get("description", resp.text[:200]))
        if resp.status_code == 429:
            retry_after = float((body.get("parameters") or {}).get("retry_after", 5))
            self._next_at = time.monotonic() + retry_after + 0.25
            self.log.warning("Telegram rate limit: retry after %.0fs", retry_after)
            return "retry"
        if resp.status_code == 400:
            lowered = description.lower()
            if not plain and ("parse" in lowered or "entit" in lowered or "tag" in lowered):
                return "plain"
            if "chat not found" in lowered:
                self._disable(f"chat {self.chat_id} not found")
            else:
                self.log.error("Telegram rejected a message: %s", description)
            return "drop"
        if resp.status_code in (401, 403, 404):
            self._disable(f"HTTP {resp.status_code}: {description}")
            return "drop"
        self._next_at = time.monotonic() + 10.0
        self.log.warning("Telegram HTTP %s: %s", resp.status_code, description)
        return "retry"

    def send_blocking(self, text: str, attempts: int = 3) -> bool:
        plain = False
        for _attempt in range(attempts * 2):
            wait = self.ready_in()
            if wait > 0.0:
                time.sleep(wait)
            if not self.enabled:
                return False
            result = self.send_once(text, plain=plain)
            if result == "ok":
                return True
            if result == "drop":
                return False
            if result == "plain":
                plain = True
        return False


class AlertDispatcher(threading.Thread):
    """T3: prints every alert immediately; batches Telegram messages under the rate limit."""

    def __init__(
        self, alert_q: "queue.Queue", stop: threading.Event, sender: TelegramSender,
        log: logging.Logger, alert_log: logging.Logger,
    ) -> None:
        super().__init__(name="AlertDispatcher", daemon=True)
        self.alert_q = alert_q
        self.stop = stop
        self.sender = sender
        self.log = log
        self.alert_log = alert_log
        self.printed = 0
        self.dropped = 0
        self._pending: List[str] = []  # HTML texts not yet packed into a Telegram message
        self._outbox: deque = deque()  # [text, attempts, plain]

    def _has_telegram_work(self) -> bool:
        return self.sender.enabled and bool(self._pending or self._outbox)

    def run(self) -> None:
        q = self.alert_q
        flush_deadline: Optional[float] = None
        while True:
            if self.stop.is_set():
                if flush_deadline is None:
                    flush_deadline = time.monotonic() + SHUTDOWN_FLUSH_SEC
                if (q.empty() and not self._has_telegram_work()) or time.monotonic() > flush_deadline:
                    break
            timeout = 0.5
            if self._has_telegram_work():
                timeout = min(timeout, max(0.0, self.sender.ready_in()))
            try:
                alert = q.get(timeout=timeout) if timeout > 0.0 else q.get_nowait()
            except queue.Empty:
                alert = None
            if alert is not None:
                self._handle(alert)
                for _burst in range(500):
                    try:
                        self._handle(q.get_nowait())
                    except queue.Empty:
                        break
            if self._has_telegram_work() and self.sender.ready_in() <= 0.0:
                self._send_next()
        if self._pending or self._outbox:
            self.log.warning("Dispatcher exiting with %d unsent Telegram messages", len(self._pending) + len(self._outbox))
        self.log.info("AlertDispatcher stopped (printed %d, Telegram sent %d)", self.printed, self.sender.sent)

    def _handle(self, alert: Alert) -> None:
        self.alert_log.info(format_console(alert))
        self.printed += 1
        if self.sender.enabled:
            self._pending.append(format_html(alert))
            overflow = len(self._pending) - MAX_PENDING_ALERTS
            if overflow > 0:
                del self._pending[:overflow]
                self.dropped += overflow

    def _send_next(self) -> None:
        if not self._outbox:
            for text in pack_messages(self._pending, TG_MESSAGE_LIMIT):
                self._outbox.append([text, 0, False])
            self._pending.clear()
        if not self._outbox:
            return
        item = self._outbox[0]
        result = self.sender.send_once(item[0], plain=item[2])
        if result == "ok":
            self._outbox.popleft()
        elif result == "plain":
            item[2] = True
        elif result == "retry":
            item[1] += 1
            if item[1] >= TG_MAX_ATTEMPTS:
                self._outbox.popleft()
                self.sender.failed += 1
                self.log.error("Telegram message dropped after %d attempts", TG_MAX_ATTEMPTS)
        else:
            self._outbox.popleft()
            self.sender.failed += 1
        if not self.sender.enabled:
            self._outbox.clear()
            self._pending.clear()


# ════════════════════════════════════════════════════════════════════════════════════════════
# Supervisor + entry point
# ════════════════════════════════════════════════════════════════════════════════════════════
def install_signal_handlers(stop: threading.Event, log: logging.Logger) -> None:
    def handler(signum: int, _frame: Any) -> None:
        if stop.is_set():
            log.warning("Second signal received: exiting immediately")
            logging.shutdown()
            os._exit(130)
        log.info("Signal %s received: shutting down gracefully (Ctrl+C again to force)", signum)
        stop.set()

    signal.signal(signal.SIGINT, handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handler)


def supervise(
    stop: threading.Event, clock: MarketClock, cfg: Config, feed: FeedBase, producer: TickProducer,
    engine: QuantEngine, dispatcher: AlertDispatcher, sender: TelegramSender,
    tick_q: "queue.Queue", log: logging.Logger,
) -> int:
    started = time.time()
    exit_at: Optional[float] = None
    if cfg.exit_after_close:
        candidate = clock.exit_epoch(started)
        if started < candidate:
            exit_at = candidate
            log.info("Auto-exit scheduled at %s IST (EXIT_AFTER_CLOSE=1)", ist_hms(candidate))
        else:
            log.info("Started after today's close: auto-exit disabled for this run (Ctrl+C to stop)")
    last_stats = started
    last_rx = producer.received
    while not stop.is_set():
        stop.wait(1.0)
        now = time.time()
        if exit_at is not None and now >= exit_at:
            log.info("Market closed: stopping the scanner")
            return 0
        for thread, name in ((engine, "QuantEngine"), (dispatcher, "AlertDispatcher"), (feed, "FeedProducer")):
            if not thread.is_alive():
                log.error("%s thread stopped unexpectedly", name)
                return 1
        if now - last_stats >= STATS_EVERY_SEC:
            rate = (producer.received - last_rx) / max(1e-6, now - last_stats)
            log.info(
                "STATS feed=%s %s | rx=%d (%.0f/s) dropped=%d queue=%d | lag=%.1fms err=%d resync=%d | "
                "alerts=%d cooldown=%d dropped=%d | telegram sent=%d failed=%d | live=%d/%d",
                feed.kind, "UP" if feed.connected else "DOWN", producer.received, rate, producer.dropped,
                tick_q.qsize(), engine.lag_ms, engine.errors, engine.resyncs, engine.alerts, engine.suppressed,
                engine.alerts_dropped, sender.sent, sender.failed, engine.live_count(), len(engine.stocks),
            )
            last_stats = now
            last_rx = producer.received
    return 0


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kotak Neo real-time Nifty 200 order-flow scanner")
    parser.add_argument("--env-file", default="", help="path to the .env file (default: ./.env, then next to the script)")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    parser.add_argument("--check", action="store_true", help="login, build the token map, send a Telegram test message, exit")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    script_dir = Path(__file__).resolve().parent
    if args.env_file:
        env_path = Path(args.env_file)
    elif (Path.cwd() / ".env").is_file():
        env_path = Path.cwd() / ".env"
    else:
        env_path = script_dir / ".env"
    env_loaded = load_env_file(env_path)
    log, alert_log = setup_logging(args.log_level, script_dir / "logs")
    if MISSING_DEPENDENCIES:
        log.error("Missing packages: %s. Install with: pip install kotakneoapi requests pyotp python-dotenv",
                  ", ".join(MISSING_DEPENDENCIES))
        return 2
    if not env_loaded:
        log.warning("No .env file at %s; using process environment variables only", env_path)
    try:
        cfg = Config()
    except ConfigError as exc:
        log.error("Configuration error: %s", exc)
        return 2

    clock = MarketClock(cfg.market_open_sod, cfg.market_close_sod)
    sdk_version = getattr(neo_api_client, "__version__", "unknown")
    log.info(
        "Kotak Nifty 200 scanner | neo_api_client %s | session %s-%s IST | alerts from %s | breakouts from %s",
        sdk_version, sod_to_hhmm(clock.open_sod), sod_to_hhmm(clock.close_sod),
        sod_to_hhmm(clock.warmup_end_sod), sod_to_hhmm(clock.breakout_sod),
    )
    if cfg.consumer_secret and NeoAPI is not None:
        with contextlib.suppress(TypeError, ValueError):
            if "consumer_secret" not in inspect.signature(NeoAPI.__init__).parameters:
                log.info("KOTAK_CONSUMER_SECRET is not used by this SDK version (consumer key + TOTP only)")

    session = KotakSession(cfg, log)
    try:
        session.login()
    except LoginError as exc:
        log.error("Kotak Neo login failed: %s", exc)
        return 3

    cache_path = script_dir / "cache" / "nifty200_token_map.json"
    try:
        instruments, universe_info = build_universe(session.client, cache_path, log)
    except Exception as exc:
        log.error("Could not build the Nifty 200 token map: %s: %s", type(exc).__name__, exc)
        return 4

    sender = TelegramSender(cfg.tg_token, cfg.tg_chat_id, cfg.tg_min_interval, log)
    if not sender.enabled:
        log.warning("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set: alerts go to console and log only")
    backend = "SFeed (async)" if callable(getattr(session.client, "create_websocket", None)) else "Legacy (callbacks)"

    if args.check:
        sample = ", ".join(f"{inst.symbol}={inst.token}" for inst in instruments[:8])
        log.info("CHECK OK | %s | feed backend: %s | sample: %s", universe_info, backend, sample)
        if sender.enabled:
            ok = sender.send_blocking(
                format_html(Alert(time.time(), "SYSTEM", "", "✅ KOTAK NIFTY 200 SCANNER CHECK OK",
                                  [f"Token map: {universe_info}", f"Feed backend: {backend}"]))
            )
            log.info("Telegram test message %s", "sent" if ok else "FAILED")
            return 0 if ok else 5
        return 0

    sys.setswitchinterval(0.002)  # hand the GIL to the socket thread quickly under engine load
    tick_q: "queue.Queue" = queue.Queue(maxsize=TICK_QUEUE_MAXSIZE)
    alert_q: "queue.Queue" = queue.Queue(maxsize=ALERT_QUEUE_MAXSIZE)
    stop = threading.Event()
    install_signal_handlers(stop, log)

    producer = TickProducer(tick_q)
    engine = QuantEngine(instruments, tick_q, alert_q, stop, clock, log)
    dispatcher = AlertDispatcher(alert_q, stop, sender, log, alert_log)
    feed = make_feed(session, instruments, producer, stop, clock, cfg, log)
    dispatcher.start()
    engine.start()
    feed.start()
    with contextlib.suppress(queue.Full):
        alert_q.put_nowait(Alert(time.time(), "SYSTEM", "", "✅ NIFTY 200 SCANNER ONLINE", [
            f"Token map: {universe_info}",
            f"Feed: {backend} | neo_api_client {sdk_version}",
            f"Alerts from {sod_to_hhmm(clock.warmup_end_sod)} IST | breakouts from {sod_to_hhmm(clock.breakout_sod)} | "
            f"pulse every 5 min | cooldown {COOLDOWN_SEC}s",
        ]))

    exit_code = supervise(stop, clock, cfg, feed, producer, engine, dispatcher, sender, tick_q, log)

    with contextlib.suppress(queue.Full):
        alert_q.put_nowait(Alert(time.time(), "SYSTEM", "", "🛑 NIFTY 200 SCANNER STOPPED", [
            f"Ticks {producer.received} | alerts {engine.alerts} | Telegram sent {sender.sent}",
        ]))
    stop.set()
    feed.join(timeout=15.0)
    engine.join(timeout=5.0)
    dispatcher.join(timeout=SHUTDOWN_FLUSH_SEC + 5.0)
    log.info("Scanner stopped (exit code %d)", exit_code)
    return exit_code


if __name__ == "__main__":
    try:
        code = main()
    except KeyboardInterrupt:
        code = 130
    logging.shutdown()
    with contextlib.suppress(Exception):
        sys.stdout.flush()
    if any(t.is_alive() and not t.daemon and t is not threading.main_thread() for t in threading.enumerate()):
        os._exit(code)  # the legacy SDK's socket thread is non-daemon
    sys.exit(code)
