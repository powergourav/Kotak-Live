"""
Real-time Nifty 200 institutional order-flow scanner for Kotak Neo.

Runtime requirements:
    Python 3.10+
    pip install "kotakneoapi>=3.0.13,<4" pyotp python-dotenv requests

The scanner uses Kotak Neo's current async SFeed WebSocket API on one dedicated
producer thread. The producer hands decoded feed messages to a non-blocking
100,000-item queue; one quant thread owns all mutable market state; one dispatcher
thread owns non-blocking console output.

This program generates market-data alerts only. It never places an order.
"""

from __future__ import annotations

import asyncio
import csv
import inspect
import io
import logging
import math
import os
import queue
import re
import signal
import sys
import threading
import time
from collections import deque
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from datetime import time as dt_time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, NamedTuple
from zoneinfo import ZoneInfo

try:
    import pyotp
    import requests
    from dotenv import load_dotenv
    from neo_api_client import NeoAPI
    from neo_api_client.websocket.feed import SFeedScrip, WsToken
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError as import_error:
    raise SystemExit(
        "Missing runtime dependency. Install with: "
        'pip install "kotakneoapi>=3.0.13,<4" pyotp python-dotenv requests\n'
        f"Import error: {import_error}"
    ) from import_error

try:
    from neo_api_client.logger import setup_logging as setup_neo_logging
except ImportError:
    setup_neo_logging = None


IST = ZoneInfo("Asia/Kolkata")
NSE_NIFTY_200_URL = (
    "https://nsearchives.nseindia.com/content/indices/ind_nifty200list.csv"
)
MARKET_OPEN = dt_time(9, 15)
WARMUP_END = dt_time(9, 17)
BREAKOUT_START = dt_time(9, 25)
MARKET_CLOSE = dt_time(15, 30)
TICK_QUEUE_CAPACITY = 100_000
ALERT_QUEUE_CAPACITY = 10_000
SUBSCRIPTION_BATCH_SIZE = 50
ALERT_COOLDOWN_SECONDS = 180
MIN_FIVE_MIN_TURNOVER_RUPEES = 2_000_000.0
MAX_BUCKETS = 900

# Snapshot of the official NSE constituent file on 2026-11-02. The live NSE
# download is always preferred; this exact 200-symbol set is used only if that
# download is unavailable or malformed.
FALLBACK_NIFTY_200 = tuple(
    """
    360ONE ABB APLAPOLLO AUBANK ADANIENSOL ADANIENT ADANIGREEN ADANIPORTS
    ADANIPOWER ATGL ABCAPITAL AMBUJACEM APARINDS APOLLOHOSP ASHOKLEY
    ASIANPAINT ASTRAL AUROPHARMA DMART AXISBANK BSE BAJAJ-AUTO BAJFINANCE
    BAJAJFINSV BAJAJHLDNG BANKBARODA BANKINDIA MAHABANK BDL BEL BHARATFORG
    BHEL BPCL BHARTIARTL GROWW BIOCON BLUESTARCO BOSCHLTD BRITANNIA CGPOWER
    CANBK CHOLAFIN CIPLA COALINDIA COCHINSHIP COFORGE COLPAL CONCOR
    CUMMINSIND DLF DABUR DIVISLAB DIXON DRREDDY EICHERMOT ETERNAL EXIDEIND
    NYKAA FEDERALBNK FORTIS GAIL GVT&D GMRAIRPORT GLENMARK GODFRYPHLP
    GODREJCP GODREJPROP GRASIM HCLTECH HDFCAMC HDFCBANK HDFCLIFE HAVELLS
    HEROMOTOCO HINDALCO HAL HINDCOPPER HINDPETRO HINDUNILVR HINDZINC
    POWERINDIA HYUNDAI ICICIBANK ICICIGI ICICIAMC IDFCFIRSTB ITC INDIANB
    INDHOTEL IOC IRCTC IRFC IREDA INDUSTOWER INDUSINDBK NAUKRI INFY INDIGO
    JSWENERGY JSWSTEEL JINDALSTEL JIOFIN JUBLFOOD KEI KALYANKJIL KOTAKBANK
    LTF LGEINDIA LICHSGFIN LTM LT LAURUSLABS LENSKART LICI LODHA LUPIN MRF
    M&MFIN M&M MANKIND MARICO MARUTI MFSL MAXHEALTH MAZDOCK MEESHO
    MOTILALOFS MPHASIS MCX MUTHOOTFIN NHPC NLCINDIA NMDC NTPC NATIONALUM
    NESTLEIND OBEROIRLTY ONGC OIL PAYTM OFSS POLICYBZR PIIND PAGEIND
    PATANJALI PERSISTENT PHOENIXLTD PIDILITIND POLYCAB PFC POWERGRID
    PREMIERENE PRESTIGE PNB RECLTD RADICO RVNL RELIANCE SBICARD SBILIFE SRF
    MOTHERSON SHRIRAMFIN ENRIN SIEMENS SOLARINDS SBIN SAIL SUNPHARMA
    SUPREMEIND SUZLON SWIGGY TVSMOTOR TATACAP TATACOMM TCS TATACONSUM TMCV
    TMPV TATAPOWER TATASTEEL TECHM TITAN TORNTPHARM TRENT TIINDIA UPL
    ULTRACEMCO UNIONBANK UNITDSPR VBL VAML VEDL VMM IDEA VOLTAS WAAREEENER
    WIPRO YESBANK ZYDUSLIFE
    """.split()  # noqa: SIM905 - compact, auditable embedded symbol snapshot
)

if len(FALLBACK_NIFTY_200) != 200 or len(set(FALLBACK_NIFTY_200)) != 200:
    raise RuntimeError("Embedded Nifty 200 fallback must contain 200 unique symbols")


LOGGER = logging.getLogger("kotak_nifty200_scanner")


@dataclass(frozen=True, slots=True)
class ScannerConfig:
    consumer_key: str
    consumer_secret: str
    mobile_number: str
    ucc: str
    mpin: str
    totp_secret: str

    @classmethod
    def from_env(cls) -> ScannerConfig:
        env_path = Path(__file__).resolve().with_name(".env")
        load_dotenv(dotenv_path=env_path, override=False)
        names = (
            "KOTAK_CONSUMER_KEY",
            "KOTAK_CONSUMER_SECRET",
            "KOTAK_MOBILE_NUMBER",
            "KOTAK_UCC",
            "KOTAK_MPIN",
            "KOTAK_TOTP_SECRET",
        )
        required_names = tuple(
            name for name in names if name != "KOTAK_CONSUMER_SECRET"
        )
        values = {name: os.getenv(name, "").strip() for name in names}
        missing = [name for name in required_names if not values[name]]
        if missing:
            raise RuntimeError(
                f"Missing required .env variable(s): {', '.join(sorted(missing))}. "
                f"Expected .env at {env_path}"
            )

        normalized_totp_secret = values["KOTAK_TOTP_SECRET"].replace(" ", "").upper()
        try:
            pyotp.TOTP(normalized_totp_secret).at(int(time.time()))
        except Exception as exc:
            raise RuntimeError(
                "KOTAK_TOTP_SECRET is not a valid TOTP Base32 secret"
            ) from exc

        return cls(
            consumer_key=values["KOTAK_CONSUMER_KEY"],
            consumer_secret=values["KOTAK_CONSUMER_SECRET"],
            mobile_number=values["KOTAK_MOBILE_NUMBER"],
            ucc=values["KOTAK_UCC"],
            mpin=values["KOTAK_MPIN"],
            totp_secret=normalized_totp_secret,
        )


@dataclass(frozen=True, slots=True)
class Constituent:
    symbol: str
    isin: str = ""


@dataclass(frozen=True, slots=True)
class Instrument:
    symbol: str
    instrument_token: str
    trading_symbol: str
    isin: str = ""


@dataclass(frozen=True, slots=True)
class NormalizedTick:
    instrument: Instrument
    receipt_sec: int
    ltp: float
    cumulative_volume: int | None
    best_bid: float
    best_ask: float
    atp: float
    day_open: float
    day_high: float
    day_low: float
    previous_close: float


class SecondBucket(NamedTuple):
    sec_timestamp: int
    open: float
    high: float
    low: float
    close: float
    agg_buy_vol: int
    agg_sell_vol: int
    total_vol: int
    atp: float


@dataclass(slots=True)
class MutableSecond:
    sec_timestamp: int
    open: float
    high: float
    low: float
    close: float
    agg_buy_vol: int = 0
    agg_sell_vol: int = 0
    total_vol: int = 0
    atp: float = 0.0

    @classmethod
    def from_price(cls, sec_timestamp: int, price: float, atp: float) -> MutableSecond:
        return cls(
            sec_timestamp=sec_timestamp,
            open=price,
            high=price,
            low=price,
            close=price,
            atp=atp,
        )

    def update_price(self, price: float, atp: float) -> None:
        if self.open <= 0.0:
            self.open = price
            self.high = price
            self.low = price
        else:
            self.high = max(self.high, price)
            self.low = min(self.low, price)
        self.close = price
        if atp > 0.0:
            self.atp = atp

    def freeze(self) -> SecondBucket:
        return SecondBucket(
            self.sec_timestamp,
            self.open,
            self.high,
            self.low,
            self.close,
            self.agg_buy_vol,
            self.agg_sell_vol,
            self.total_vol,
            self.atp,
        )


@dataclass(frozen=True, slots=True)
class RollingCandle:
    open: float
    high: float
    low: float
    close: float
    body_pct: float
    range_pct: float
    clv: float
    buy_vol: int
    sell_vol: int
    total_vol: int
    turnover_cr: float
    buy_pct: float
    sell_pct: float
    atp: float
    bucket_count: int
    coverage_seconds: int


@dataclass(slots=True)
class SymbolState:
    instrument: Instrument
    session_date: date | None = None
    buckets: deque[SecondBucket] = field(
        default_factory=lambda: deque(maxlen=MAX_BUCKETS)
    )
    current: MutableSecond | None = None
    current_fresh_high: bool = False
    current_fresh_low: bool = False
    previous_ltp: float | None = None
    last_tick_direction: int = 0
    previous_cumulative_volume: int | None = None
    ltp: float = 0.0
    atp: float = 0.0
    best_bid: float = 0.0
    best_ask: float = 0.0
    previous_close: float = 0.0
    day_open: float = 0.0
    observed_day_high: float | None = None
    observed_day_low: float | None = None
    daily_buy_vol: int = 0
    daily_sell_vol: int = 0
    daily_total_vol: int = 0
    last_tick_sec: int = 0
    milestones_hit: set[int] = field(default_factory=set)

    def reset_for_session(self, session_date: date) -> None:
        self.session_date = session_date
        self.buckets.clear()
        self.current = None
        self.current_fresh_high = False
        self.current_fresh_low = False
        self.previous_ltp = None
        self.last_tick_direction = 0
        self.previous_cumulative_volume = None
        self.ltp = 0.0
        self.atp = 0.0
        self.best_bid = 0.0
        self.best_ask = 0.0
        self.previous_close = 0.0
        self.day_open = 0.0
        self.observed_day_high = None
        self.observed_day_low = None
        self.daily_buy_vol = 0
        self.daily_sell_vol = 0
        self.daily_total_vol = 0
        self.last_tick_sec = 0
        self.milestones_hit.clear()


@dataclass(frozen=True, slots=True)
class Alert:
    created_at: datetime
    text: str


def configure_logging() -> None:
    LOGGER.setLevel(logging.INFO)
    LOGGER.propagate = False
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(threadName)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    LOGGER.handlers.clear()
    LOGGER.addHandler(console)

    log_path = Path(__file__).resolve().with_name("kotak_nifty200_scanner.log")
    try:
        file_handler = RotatingFileHandler(
            log_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        LOGGER.addHandler(file_handler)
    except OSError as exc:
        LOGGER.warning("File logging disabled: %s", exc)

    if setup_neo_logging is not None:
        try:
            setup_neo_logging(level="WARNING", file_level="WARNING")
        except Exception as exc:  # noqa: BLE001 - isolate external API/library failures
            LOGGER.warning("Could not configure Kotak SDK logging: %s", exc)


def build_download_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=8)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "Chrome/129.0 Safari/537.36"
            ),
            "Accept": "text/csv,application/octet-stream,*/*;q=0.8",
            "Referer": "https://www.niftyindices.com/",
        }
    )
    return session


def decode_csv_content(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def canonical_column(value: str) -> str:
    return "".join(character for character in value.lower() if character.isalnum())


def canonical_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    return symbol.removesuffix(".NS").removesuffix("-EQ").strip()


def normalize_token(value: Any) -> str:
    token = str(value or "").strip()
    if "|" in token:
        token = token.rsplit("|", 1)[-1].strip()
    if re.fullmatch(r"\d+\.0+", token):
        token = token.split(".", 1)[0]
    return token


def safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        result = float(value)
        return result if math.isfinite(result) else default
    text = str(value).strip().replace(",", "")
    if not text or text.lower() in {"none", "null", "nan", "--"}:
        return default
    try:
        result = float(text)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def safe_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return max(0, value)
    parsed = safe_float(value, default=-1.0)
    if parsed < 0.0:
        return None
    return int(parsed)


def first_present(mapping: dict[str, Any], names: Sequence[str]) -> Any:
    for name in names:
        if name in mapping and mapping[name] not in (None, ""):
            return mapping[name]
    return None


def candle_from_buckets(buckets: Sequence[SecondBucket]) -> RollingCandle | None:
    if not buckets:
        return None
    priced = [bucket for bucket in buckets if bucket.close > 0.0]
    if not priced:
        return None
    rolling_open = priced[0].open
    rolling_close = priced[-1].close
    if rolling_open <= 0.0 or rolling_close <= 0.0:
        return None
    rolling_high = max(bucket.high for bucket in priced)
    rolling_low = min(bucket.low for bucket in priced)
    buy_vol = sum(bucket.agg_buy_vol for bucket in buckets)
    sell_vol = sum(bucket.agg_sell_vol for bucket in buckets)
    total_vol = sum(bucket.total_vol for bucket in buckets)
    body_pct = ((rolling_close - rolling_open) / rolling_open) * 100.0
    range_pct = ((rolling_high - rolling_low) / rolling_open) * 100.0
    clv = (rolling_close - rolling_low) / max(0.01, rolling_high - rolling_low)
    denominator = max(1, total_vol)
    buy_pct = (buy_vol / denominator) * 100.0
    sell_pct = (sell_vol / denominator) * 100.0
    atp = next((bucket.atp for bucket in reversed(priced) if bucket.atp > 0.0), 0.0)
    return RollingCandle(
        open=rolling_open,
        high=rolling_high,
        low=rolling_low,
        close=rolling_close,
        body_pct=body_pct,
        range_pct=range_pct,
        clv=max(0.0, min(1.0, clv)),
        buy_vol=buy_vol,
        sell_vol=sell_vol,
        total_vol=total_vol,
        turnover_cr=(total_vol * rolling_close) / 10_000_000.0,
        buy_pct=buy_pct,
        sell_pct=sell_pct,
        atp=atp,
        bucket_count=len(buckets),
        coverage_seconds=buckets[-1].sec_timestamp - buckets[0].sec_timestamp + 1,
    )


def market_bounds(session_date: date) -> tuple[int, int]:
    opening = datetime.combine(session_date, MARKET_OPEN, tzinfo=IST)
    closing = datetime.combine(session_date, MARKET_CLOSE, tzinfo=IST)
    return int(opening.timestamp()), int(closing.timestamp())


def is_market_hours(epoch_seconds: float | None = None) -> bool:
    current = datetime.fromtimestamp(epoch_seconds or time.time(), IST)
    return current.weekday() < 5 and MARKET_OPEN <= current.time() < MARKET_CLOSE


def seconds_until_next_market_open(epoch_seconds: float | None = None) -> float:
    current = datetime.fromtimestamp(epoch_seconds or time.time(), IST)
    candidate_date = current.date()
    candidate = datetime.combine(candidate_date, MARKET_OPEN, tzinfo=IST)
    if current >= candidate:
        candidate_date += timedelta(days=1)
        candidate = datetime.combine(candidate_date, MARKET_OPEN, tzinfo=IST)
    while candidate.weekday() >= 5:
        candidate_date += timedelta(days=1)
        candidate = datetime.combine(candidate_date, MARKET_OPEN, tzinfo=IST)
    return max(0.0, (candidate - current).total_seconds())


def format_volume(value: int) -> str:
    return f"{value:,}"


def api_error_text(response: Any) -> str | None:
    if not isinstance(response, dict):
        return "empty or non-dictionary API response" if response is None else None
    for key in ("Error", "error", "errors", "Error Message"):
        if key in response and response[key] not in (None, "", [], {}):
            value = response[key]
            if isinstance(value, list):
                parts: list[str] = []
                for item in value:
                    if isinstance(item, dict):
                        message = (
                            item.get("message") or item.get("msg") or item.get("code")
                        )
                        parts.append(str(message or "API error"))
                    else:
                        parts.append(str(item))
                return "; ".join(parts)
            if isinstance(value, dict):
                return str(value.get("message") or value.get("msg") or "API error")
            return str(value)
    status = str(response.get("status", "")).strip().lower()
    if status in {"error", "failed", "failure"}:
        return str(response.get("message") or "API reported failure")
    data = response.get("data")
    if isinstance(data, dict):
        nested_status = str(data.get("status", "")).strip().lower()
        if nested_status in {"error", "failed", "failure"}:
            return str(data.get("message") or "API reported failure")
    return None


class KotakNifty200Scanner:
    def __init__(self, config: ScannerConfig) -> None:
        self.config = config
        self.stop_event = threading.Event()
        self.feed_connected = threading.Event()
        self.producer_done = threading.Event()
        self.quant_done = threading.Event()
        self.tick_queue: queue.Queue[tuple[int, Any]] = queue.Queue(
            maxsize=TICK_QUEUE_CAPACITY
        )
        self.alert_queue: queue.Queue[Alert] = queue.Queue(maxsize=ALERT_QUEUE_CAPACITY)
        self.client: NeoAPI | None = None
        self.client_lock = threading.Lock()
        self.mapping_lock = threading.Lock()
        self.authenticated_date: date | None = None
        self.authenticated_monotonic = 0.0
        self.mapped_date: date | None = None
        self.instruments_by_symbol: dict[str, Instrument] = {}
        self.instruments_by_token: dict[str, Instrument] = {}
        self.instruments_by_trading_symbol: dict[str, Instrument] = {}
        self.states: dict[str, SymbolState] = {}
        self.alert_cooldowns: dict[tuple[str, str], int] = {}
        self.system_cooldowns: dict[str, float] = {}
        self.system_cooldown_lock = threading.Lock()
        self.last_pulse_slot = int(time.time()) // 300
        self.last_pulse_date: date | None = None
        self.threads: list[threading.Thread] = []
        self.received_payloads = 0
        self.normalized_ticks = 0
        self.dropped_ticks = 0
        self.malformed_payloads = 0
        self.dropped_alerts = 0
        self.websocket_errors = 0
        self.websocket_reconnects = 0

    def _new_client(self) -> NeoAPI:
        constructor_parameters = inspect.signature(NeoAPI).parameters
        kwargs: dict[str, Any] = {
            "consumer_key": self.config.consumer_key,
            "environment": "prod",
        }
        if "consumer_secret" in constructor_parameters:
            kwargs["consumer_secret"] = self.config.consumer_secret
        if "access_token" in constructor_parameters:
            kwargs["access_token"] = None
        if "neo_fin_key" in constructor_parameters:
            kwargs["neo_fin_key"] = None
        return NeoAPI(**kwargs)

    def authenticate(self) -> None:
        LOGGER.info("Authenticating with Kotak Neo TOTP flow")
        client = self._new_client()
        totp_generator = pyotp.TOTP(self.config.totp_secret)
        remaining = totp_generator.interval - (time.time() % totp_generator.interval)
        if remaining < 3.0 and self.stop_event.wait(remaining + 0.25):
            raise RuntimeError("Shutdown requested during TOTP authentication")
        current_totp = totp_generator.now()
        login_response = client.totp_login(
            mobile_number=self.config.mobile_number,
            ucc=self.config.ucc,
            totp=current_totp,
        )
        login_error = api_error_text(login_response)
        if login_error:
            raise RuntimeError(f"Kotak TOTP login failed: {login_error}")

        validate_response = client.totp_validate(mpin=self.config.mpin)
        validation_error = api_error_text(validate_response)
        if validation_error:
            raise RuntimeError(f"Kotak MPIN validation failed: {validation_error}")

        configuration = getattr(client, "configuration", None)
        edit_token = getattr(configuration, "edit_token", None)
        edit_sid = getattr(configuration, "edit_sid", None)
        if not edit_token or not edit_sid:
            raise RuntimeError(
                "Kotak authentication returned no trade token/session ID; "
                "verify the consumer key, mobile number, UCC, MPIN, and system clock"
            )

        with self.client_lock:
            self.client = client
            self.authenticated_date = datetime.now(IST).date()
            self.authenticated_monotonic = time.monotonic()
        LOGGER.info("Kotak Neo authentication succeeded")

    def _current_client(self) -> NeoAPI:
        with self.client_lock:
            if self.client is None:
                raise RuntimeError("Kotak client is not authenticated")
            return self.client

    def _fetch_nifty_200(self) -> list[Constituent]:
        try:
            with build_download_session() as session:
                response = session.get(
                    NSE_NIFTY_200_URL,
                    timeout=(7.0, 35.0),
                )
                response.raise_for_status()
                text = decode_csv_content(response.content)
            reader = csv.DictReader(io.StringIO(text))
            if reader.fieldnames is None:
                raise ValueError("NSE CSV has no header")
            column_map = {canonical_column(name): name for name in reader.fieldnames}
            symbol_column = column_map.get("symbol")
            series_column = column_map.get("series")
            isin_column = column_map.get("isincode") or column_map.get("isin")
            if symbol_column is None:
                raise ValueError("NSE CSV has no Symbol column")
            constituents: list[Constituent] = []
            for row in reader:
                symbol = canonical_symbol(row.get(symbol_column))
                series = (
                    str(row.get(series_column, "EQ") if series_column else "EQ")
                    .strip()
                    .upper()
                )
                isin = (
                    str(row.get(isin_column, "") if isin_column else "").strip().upper()
                )
                if symbol and series == "EQ":
                    constituents.append(Constituent(symbol=symbol, isin=isin))
            if (
                len(constituents) != 200
                or len({item.symbol for item in constituents}) != 200
            ):
                raise ValueError(
                    f"NSE constituent CSV returned {len(constituents)} unique EQ rows, expected 200"
                )
            LOGGER.info("Loaded 200 live constituents from the official NSE CSV")
            return constituents
        except Exception as exc:  # noqa: BLE001 - isolate external API/library failures
            LOGGER.warning(
                "Official NSE Nifty 200 download failed; using embedded 200-symbol fallback: %s",
                exc,
            )
            return [Constituent(symbol=symbol) for symbol in FALLBACK_NIFTY_200]

    @staticmethod
    def _extract_urls(value: Any) -> list[str]:
        urls: list[str] = []
        if isinstance(value, str):
            stripped = value.strip().strip('"')
            if stripped.lower().startswith(("https://", "http://")):
                urls.append(stripped)
        elif isinstance(value, dict):
            for nested in value.values():
                urls.extend(KotakNifty200Scanner._extract_urls(nested))
        elif isinstance(value, (list, tuple, set)):
            for nested in value:
                urls.extend(KotakNifty200Scanner._extract_urls(nested))
        return urls

    def _download_scrip_master(self) -> str:
        client = self._current_client()
        response = client.scrip_master(exchange_segment="nse_cm")
        response_error = api_error_text(response)
        if response_error:
            raise RuntimeError(f"Kotak scrip-master request failed: {response_error}")

        if isinstance(response, bytes):
            return decode_csv_content(response)
        if isinstance(response, str) and "\n" in response and "," in response:
            return response

        urls = self._extract_urls(response)
        nse_urls = [
            url
            for url in urls
            if "nse_cm" in url.lower() and url.lower().split("?", 1)[0].endswith(".csv")
        ]
        if not nse_urls:
            raise RuntimeError(
                "Kotak scrip-master response contained no nse_cm CSV URL"
            )
        nse_urls.sort(
            key=lambda url: (
                "transformed-v1" in url.lower(),
                "nse_cm-v1.csv" in url.lower(),
            ),
            reverse=True,
        )
        selected_url = nse_urls[0]
        with build_download_session() as session:
            download = session.get(selected_url, timeout=(7.0, 60.0))
            download.raise_for_status()
            content = download.content
        if len(content) < 100:
            raise RuntimeError(
                "Downloaded Kotak nse_cm scrip master is unexpectedly small"
            )
        return decode_csv_content(content)

    @staticmethod
    def _map_instruments(
        constituents: Sequence[Constituent], scrip_master_text: str
    ) -> list[Instrument]:
        reader = csv.DictReader(io.StringIO(scrip_master_text))
        if reader.fieldnames is None:
            raise RuntimeError("Kotak scrip-master CSV has no header")
        fields = {canonical_column(name.strip()): name for name in reader.fieldnames}

        def resolve_field(*aliases: str) -> str | None:
            for alias in aliases:
                field_name = fields.get(canonical_column(alias))
                if field_name is not None:
                    return field_name
            return None

        token_field = resolve_field(
            "pSymbol", "instrument_token", "instrumentToken", "scrip_token", "token"
        )
        symbol_name_field = resolve_field(
            "pSymbolName", "symbol_name", "symbolName", "underlying"
        )
        trading_symbol_field = resolve_field(
            "pTrdSymbol", "trading_symbol", "tradingSymbol", "tradingsymbol"
        )
        group_field = resolve_field("pGroup", "pSeries", "series", "group")
        instrument_type_field = resolve_field("pInstType", "instrument_type")
        exchange_field = resolve_field(
            "pExchSeg", "exchange_segment", "exchangeSegment"
        )
        isin_field = resolve_field("pISIN", "isin", "isinCode")
        if token_field is None or (
            symbol_name_field is None and trading_symbol_field is None
        ):
            raise RuntimeError(
                "Kotak scrip-master CSV lacks pSymbol and symbol/trading-symbol columns"
            )

        rows_by_symbol: dict[str, list[dict[str, str]]] = {}
        rows_by_isin: dict[str, list[dict[str, str]]] = {}
        for raw_row in reader:
            row = {
                str(key).strip(): str(value or "").strip()
                for key, value in raw_row.items()
            }
            token = normalize_token(row.get(token_field))
            if not token:
                continue
            exchange = (
                row.get(exchange_field, "nse_cm").strip().lower()
                if exchange_field
                else "nse_cm"
            )
            if exchange and exchange not in {"nse_cm", "nse", "1"}:
                continue
            trading_symbol = (
                row.get(trading_symbol_field, "") if trading_symbol_field else ""
            )
            symbol_name = row.get(symbol_name_field, "") if symbol_name_field else ""
            group = row.get(group_field, "").upper() if group_field else ""
            instrument_type = (
                row.get(instrument_type_field, "").upper()
                if instrument_type_field
                else ""
            )
            is_equity = (
                group == "EQ"
                or instrument_type == "EQ"
                or trading_symbol.upper().endswith("-EQ")
            )
            if not is_equity:
                continue
            candidates = {
                canonical_symbol(symbol_name),
                canonical_symbol(trading_symbol),
            }
            for candidate in candidates:
                if candidate:
                    rows_by_symbol.setdefault(candidate, []).append(row)
            isin = row.get(isin_field, "").upper() if isin_field else ""
            if isin:
                rows_by_isin.setdefault(isin, []).append(row)

        mapped: list[Instrument] = []
        missing: list[str] = []
        for constituent in constituents:
            target_symbol = constituent.symbol
            target_isin = constituent.isin
            candidates = list(rows_by_symbol.get(target_symbol, []))
            if target_isin:
                for row in rows_by_isin.get(target_isin, []):
                    if row not in candidates:
                        candidates.append(row)
            if not candidates:
                missing.append(target_symbol)
                continue

            def candidate_score(
                row: dict[str, str],
                bound_symbol: str = target_symbol,
                bound_isin: str = target_isin,
            ) -> tuple[int, int, str]:
                symbol_name = canonical_symbol(
                    row.get(symbol_name_field, "") if symbol_name_field else ""
                )
                trading_symbol = (
                    str(
                        row.get(trading_symbol_field, "")
                        if trading_symbol_field
                        else ""
                    )
                    .strip()
                    .upper()
                )
                isin = row.get(isin_field, "").upper() if isin_field else ""
                score = 0
                if symbol_name == bound_symbol:
                    score += 10
                if trading_symbol == f"{bound_symbol}-EQ":
                    score += 10
                if bound_isin and isin == bound_isin:
                    score += 8
                group = row.get(group_field, "").upper() if group_field else ""
                if group == "EQ":
                    score += 4
                token = normalize_token(row.get(token_field))
                return score, 1 if token.isdigit() else 0, token

            candidates.sort(key=candidate_score, reverse=True)
            selected = candidates[0]
            selected_score = candidate_score(selected)
            equally_ranked_tokens = {
                normalize_token(row.get(token_field))
                for row in candidates
                if candidate_score(row)[:2] == selected_score[:2]
            }
            if len(equally_ranked_tokens) > 1:
                raise RuntimeError(
                    f"Ambiguous Kotak EQ token mapping for {target_symbol}: "
                    f"{sorted(equally_ranked_tokens)}"
                )
            token = normalize_token(selected.get(token_field))
            trading_symbol = (
                str(
                    selected.get(trading_symbol_field, "")
                    if trading_symbol_field
                    else ""
                )
                .strip()
                .upper()
            )
            if not trading_symbol:
                trading_symbol = f"{target_symbol}-EQ"
            mapped.append(
                Instrument(
                    symbol=target_symbol,
                    instrument_token=token,
                    trading_symbol=trading_symbol,
                    isin=target_isin,
                )
            )

        if missing:
            raise RuntimeError(
                "Kotak nse_cm scrip master did not contain EQ tokens for: "
                + ", ".join(missing)
            )
        if len(mapped) != 200:
            raise RuntimeError(
                f"Mapped {len(mapped)} Nifty 200 instruments, expected 200"
            )
        tokens = [instrument.instrument_token for instrument in mapped]
        if len(set(tokens)) != 200:
            duplicates = sorted({token for token in tokens if tokens.count(token) > 1})
            raise RuntimeError(
                f"Duplicate Kotak instrument tokens detected: {duplicates}"
            )
        return mapped

    def refresh_instrument_mapping(self) -> None:
        LOGGER.info("Refreshing NSE constituents and Kotak nse_cm token mapping")
        constituents = self._fetch_nifty_200()
        scrip_master = self._download_scrip_master()
        mapped = self._map_instruments(constituents, scrip_master)
        by_symbol = {instrument.symbol: instrument for instrument in mapped}
        by_token = {instrument.instrument_token: instrument for instrument in mapped}
        by_trading: dict[str, Instrument] = {}
        for instrument in mapped:
            by_trading[instrument.trading_symbol.upper()] = instrument
            by_trading[canonical_symbol(instrument.trading_symbol)] = instrument
            by_trading[instrument.symbol] = instrument
        with self.mapping_lock:
            self.instruments_by_symbol = by_symbol
            self.instruments_by_token = by_token
            self.instruments_by_trading_symbol = by_trading
            self.mapped_date = datetime.now(IST).date()
        LOGGER.info(
            "Mapped all 200 Nifty constituents to unique Kotak instrument tokens"
        )

    def _instrument_snapshot(self) -> list[Instrument]:
        with self.mapping_lock:
            return list(self.instruments_by_symbol.values())

    def on_message(self, message: Any) -> None:
        self.received_payloads += 1
        try:
            self.tick_queue.put_nowait((time.time_ns(), message))
        except queue.Full:
            self.dropped_ticks += 1

    def _on_websocket_error(self, error: Exception) -> None:
        self.websocket_errors += 1

    def _put_alert(self, alert: Alert) -> bool:
        try:
            self.alert_queue.put_nowait(alert)
            return True
        except queue.Full:
            self.dropped_alerts += 1
            return False

    def _put_system_alert(
        self, key: str, text: str, cooldown_seconds: float = 60.0
    ) -> bool:
        now = time.monotonic()
        with self.system_cooldown_lock:
            last = self.system_cooldowns.get(key, 0.0)
            if now - last < cooldown_seconds:
                return False
            queued = self._put_alert(Alert(created_at=datetime.now(IST), text=text))
            if queued:
                self.system_cooldowns[key] = now
            return queued

    async def _websocket_session(self) -> None:
        client = self._current_client()
        instruments = self._instrument_snapshot()
        if len(instruments) != 200:
            raise RuntimeError(
                f"WebSocket requires 200 mapped instruments, got {len(instruments)}"
            )
        tokens = [WsToken("nse_cm", item.instrument_token) for item in instruments]
        websocket = client.create_websocket(
            reconnect_delay=2,
            max_reconnect_attempts=12,
            max_connect_retries=5,
            ping_interval=20,
            max_subscriptions=1_000,
            ack_wait_timeout=5.0,
        )
        websocket.on_message = self.on_message
        websocket.on_error = self._on_websocket_error
        websocket.on_connect = self.feed_connected.set
        websocket.on_disconnect = self.feed_connected.clear
        pump_task: asyncio.Task[None] | None = None
        async with websocket:

            async def pump_messages() -> None:
                async for _message in websocket:
                    if self.stop_event.is_set():
                        return

            pump_task = asyncio.create_task(pump_messages(), name="kotak-sfeed-pump")
            try:
                for start in range(0, len(tokens), SUBSCRIPTION_BATCH_SIZE):
                    await websocket.subscribe_depth(
                        tokens[start : start + SUBSCRIPTION_BATCH_SIZE]
                    )
                await websocket.subscribe_exchange()
                self.feed_connected.set()
                self._put_system_alert(
                    "feed-connected",
                    "✅ KOTAK NEO FEED CONNECTED\n"
                    f"Subscribed to depth/touch-line updates for {len(tokens)} Nifty 200 stocks "
                    f"in batches of {SUBSCRIPTION_BATCH_SIZE}.",
                    cooldown_seconds=120.0,
                )

                while not self.stop_event.is_set() and is_market_hours():
                    done, _ = await asyncio.wait({pump_task}, timeout=0.5)
                    if done:
                        exception = pump_task.exception()
                        if exception is not None:
                            raise exception
                        raise RuntimeError("Kotak WebSocket message stream ended")
            finally:
                if pump_task is not None and not pump_task.done():
                    pump_task.cancel()
                    try:
                        await pump_task
                    except asyncio.CancelledError:
                        LOGGER.debug("WebSocket pump cancelled during orderly shutdown")
                self.feed_connected.clear()

    def _websocket_producer(self) -> None:
        reconnect_delay = 1.0
        while not self.stop_event.is_set():
            if not is_market_hours():
                wait_seconds = min(60.0, max(1.0, seconds_until_next_market_open()))
                self.stop_event.wait(wait_seconds)
                continue

            current_date = datetime.now(IST).date()
            auth_age = time.monotonic() - self.authenticated_monotonic
            try:
                if self.authenticated_date != current_date or auth_age > 14_400.0:
                    self.authenticate()
                if self.mapped_date != current_date:
                    self.refresh_instrument_mapping()
                asyncio.run(self._websocket_session())
                reconnect_delay = 1.0
                if is_market_hours() and not self.stop_event.is_set():
                    raise RuntimeError(
                        "Kotak WebSocket disconnected during market hours"
                    )
            except Exception as exc:  # noqa: BLE001 - isolate external API/library failures
                self.feed_connected.clear()
                if self.stop_event.is_set():
                    break
                self.websocket_reconnects += 1
                safe_error = self._redact(str(exc))[:500]
                LOGGER.error("WebSocket cycle failed: %s", safe_error)
                self._put_system_alert(
                    "feed-disconnected",
                    "⚠️ KOTAK NEO FEED DISCONNECTED\n"
                    f"Reason: {safe_error}\n"
                    f"Automatic reconnect in {reconnect_delay:.0f}s while the market is open.",
                    cooldown_seconds=60.0,
                )
                lowered = safe_error.lower()
                if "auth" in lowered or "token" in lowered or "session" in lowered:
                    self.authenticated_monotonic = 0.0
                self.stop_event.wait(reconnect_delay)
                reconnect_delay = min(60.0, reconnect_delay * 2.0)

    def _redact(self, text: str) -> str:
        redacted = text
        for secret in (
            self.config.consumer_key,
            self.config.consumer_secret,
            self.config.mpin,
            self.config.totp_secret,
        ):
            if secret:
                redacted = redacted.replace(secret, "***")
        return redacted

    @staticmethod
    def _iter_tick_records(payload: Any) -> Iterable[Any]:
        if isinstance(payload, SFeedScrip):
            yield payload
            return
        if isinstance(payload, (list, tuple)):
            for item in payload:
                yield from KotakNifty200Scanner._iter_tick_records(item)
            return
        if isinstance(payload, dict):
            has_price = any(
                key in payload
                for key in ("last_traded_price", "ltp", "iv", "lastTradedPrice")
            )
            has_token = any(
                key in payload
                for key in ("instrument_token", "tk", "token", "instrumentToken")
            )
            if has_price and has_token:
                yield payload
                return
            for key in ("data", "result", "message", "ticks"):
                nested = payload.get(key)
                if isinstance(nested, (dict, list, tuple)):
                    yield from KotakNifty200Scanner._iter_tick_records(nested)

    @staticmethod
    def _depth_price(depth: Any) -> float:
        if not depth:
            return 0.0
        first = depth[0] if isinstance(depth, (list, tuple)) else depth
        if isinstance(first, dict):
            return safe_float(
                first_present(first, ("price", "p", "bid_price", "ask_price"))
            )
        return safe_float(getattr(first, "price", 0.0))

    def _normalize_tick(self, payload: Any, receipt_sec: int) -> NormalizedTick | None:
        if isinstance(payload, SFeedScrip):
            token = normalize_token(payload.instrument_token)
            trading_symbol = str(payload.trading_symbol or "").upper()
            ltp = safe_float(payload.last_traded_price)
            cumulative_volume = safe_int(payload.volume_traded_today)
            best_bid = self._depth_price(payload.buy)
            best_ask = self._depth_price(payload.sell)
            atp = safe_float(payload.average_trade_price)
            day_open = safe_float(payload.open_price)
            day_high = safe_float(payload.high_price)
            day_low = safe_float(payload.low_price)
            previous_close = safe_float(payload.close_price)
        elif isinstance(payload, dict):
            token = normalize_token(
                first_present(
                    payload, ("instrument_token", "instrumentToken", "tk", "token")
                )
            )
            trading_symbol = (
                str(
                    first_present(payload, ("trading_symbol", "tradingSymbol", "ts"))
                    or ""
                )
                .strip()
                .upper()
            )
            ltp = safe_float(
                first_present(
                    payload, ("last_traded_price", "lastTradedPrice", "ltp", "iv")
                )
            )
            cumulative_volume = safe_int(
                first_present(
                    payload,
                    ("volume_traded_today", "volumeTradedToday", "v", "volume"),
                )
            )
            best_bid = self._depth_price(payload.get("buy")) or safe_float(
                first_present(payload, ("bp", "bp1", "best_bid", "bestBidPrice"))
            )
            best_ask = self._depth_price(payload.get("sell")) or safe_float(
                first_present(payload, ("sp", "sp1", "best_ask", "bestAskPrice"))
            )
            atp = safe_float(
                first_present(
                    payload, ("average_trade_price", "averageTradePrice", "ap", "atp")
                )
            )
            day_open = safe_float(
                first_present(payload, ("open_price", "openPrice", "op", "o"))
            )
            day_high = safe_float(
                first_present(payload, ("high_price", "highPrice", "h", "high"))
            )
            day_low = safe_float(
                first_present(payload, ("low_price", "lowPrice", "lo", "low"))
            )
            previous_close = safe_float(
                first_present(payload, ("close_price", "closePrice", "c", "close"))
            )
        else:
            return None

        if ltp <= 0.0:
            return None
        with self.mapping_lock:
            instrument = self.instruments_by_token.get(token)
            if instrument is None and trading_symbol:
                instrument = self.instruments_by_trading_symbol.get(trading_symbol)
                if instrument is None:
                    instrument = self.instruments_by_trading_symbol.get(
                        canonical_symbol(trading_symbol)
                    )
        if instrument is None:
            return None
        return NormalizedTick(
            instrument=instrument,
            receipt_sec=receipt_sec,
            ltp=ltp,
            cumulative_volume=cumulative_volume,
            best_bid=best_bid,
            best_ask=best_ask,
            atp=atp,
            day_open=day_open,
            day_high=day_high,
            day_low=day_low,
            previous_close=previous_close,
        )

    def _classify_delta(self, state: SymbolState, tick: NormalizedTick) -> int:
        ltp = tick.ltp
        ask = tick.best_ask if tick.best_ask > 0.0 else state.best_ask
        bid = tick.best_bid if tick.best_bid > 0.0 else state.best_bid
        previous_ltp = state.previous_ltp
        if ask > 0.0 and ltp >= ask:
            return 1
        if bid > 0.0 and ltp <= bid:
            return -1
        if previous_ltp is not None and ltp > previous_ltp:
            return 1
        if previous_ltp is not None and ltp < previous_ltp:
            return -1
        if previous_ltp is not None and ltp == previous_ltp:
            return state.last_tick_direction
        return 0

    def _process_tick(self, tick: NormalizedTick) -> None:
        local_time = datetime.fromtimestamp(tick.receipt_sec, IST)
        if (
            local_time.weekday() >= 5
            or local_time.time() < MARKET_OPEN
            or local_time.time() >= MARKET_CLOSE
        ):
            return

        state = self.states.get(tick.instrument.symbol)
        if state is None:
            state = SymbolState(instrument=tick.instrument)
            self.states[tick.instrument.symbol] = state
        elif state.instrument != tick.instrument:
            state.instrument = tick.instrument

        if state.session_date != local_time.date():
            state.reset_for_session(local_time.date())

        if state.current is not None and tick.receipt_sec > state.current.sec_timestamp:
            self._advance_state_to(state, tick.receipt_sec)
        if state.current is None:
            state.current = MutableSecond.from_price(
                tick.receipt_sec, tick.ltp, tick.atp
            )
        elif tick.receipt_sec < state.current.sec_timestamp:
            tick = NormalizedTick(
                instrument=tick.instrument,
                receipt_sec=state.current.sec_timestamp,
                ltp=tick.ltp,
                cumulative_volume=tick.cumulative_volume,
                best_bid=tick.best_bid,
                best_ask=tick.best_ask,
                atp=tick.atp,
                day_open=tick.day_open,
                day_high=tick.day_high,
                day_low=tick.day_low,
                previous_close=tick.previous_close,
            )

        if tick.atp > 0.0:
            state.atp = tick.atp
        if tick.best_bid > 0.0:
            state.best_bid = tick.best_bid
        if tick.best_ask > 0.0:
            state.best_ask = tick.best_ask
        if state.previous_close <= 0.0 and tick.previous_close > 0.0:
            state.previous_close = tick.previous_close
        if tick.day_open > 0.0:
            state.day_open = tick.day_open

        candidate_high = (
            max(tick.ltp, tick.day_high) if tick.day_high > 0.0 else tick.ltp
        )
        candidate_low = min(tick.ltp, tick.day_low) if tick.day_low > 0.0 else tick.ltp
        price_epsilon = max(0.0001, tick.ltp * 1e-9)
        if state.observed_day_high is None:
            state.observed_day_high = candidate_high
        elif candidate_high > state.observed_day_high + price_epsilon:
            state.observed_day_high = candidate_high
            state.current_fresh_high = True
        if state.observed_day_low is None:
            state.observed_day_low = candidate_low
        elif candidate_low < state.observed_day_low - price_epsilon:
            state.observed_day_low = candidate_low
            state.current_fresh_low = True

        delta_volume = 0
        if tick.cumulative_volume is not None:
            if state.previous_cumulative_volume is None:
                state.previous_cumulative_volume = tick.cumulative_volume
            elif tick.cumulative_volume >= state.previous_cumulative_volume:
                delta_volume = tick.cumulative_volume - state.previous_cumulative_volume
                state.previous_cumulative_volume = tick.cumulative_volume
            else:
                state.previous_cumulative_volume = tick.cumulative_volume

        direction = self._classify_delta(state, tick) if delta_volume > 0 else 0
        state.current.update_price(tick.ltp, state.atp)
        if delta_volume > 0:
            state.current.total_vol += delta_volume
            state.daily_total_vol += delta_volume
            if direction > 0:
                state.current.agg_buy_vol += delta_volume
                state.daily_buy_vol += delta_volume
            elif direction < 0:
                state.current.agg_sell_vol += delta_volume
                state.daily_sell_vol += delta_volume

        if state.previous_ltp is not None:
            if tick.ltp > state.previous_ltp:
                state.last_tick_direction = 1
            elif tick.ltp < state.previous_ltp:
                state.last_tick_direction = -1
        state.previous_ltp = tick.ltp
        state.ltp = tick.ltp
        state.last_tick_sec = max(state.last_tick_sec, tick.receipt_sec)
        self.normalized_ticks += 1

    def _advance_state_to(self, state: SymbolState, target_sec: int) -> None:
        if state.current is None or state.session_date is None:
            return
        _, close_epoch = market_bounds(state.session_date)
        effective_target = min(target_sec, close_epoch)
        while state.current.sec_timestamp < effective_target:
            bucket = state.current.freeze()
            fresh_high = state.current_fresh_high
            fresh_low = state.current_fresh_low
            state.buckets.append(bucket)
            self._evaluate_alerts(state, bucket, fresh_high, fresh_low)
            next_sec = state.current.sec_timestamp + 1
            carry_price = (
                state.current.close if state.current.close > 0.0 else state.ltp
            )
            state.current = MutableSecond.from_price(next_sec, carry_price, state.atp)
            state.current_fresh_high = False
            state.current_fresh_low = False

    @staticmethod
    def _window(
        buckets: Sequence[SecondBucket], start_sec: int, end_sec: int
    ) -> list[SecondBucket]:
        return [
            bucket for bucket in buckets if start_sec <= bucket.sec_timestamp <= end_sec
        ]

    @staticmethod
    def _full_window(candle: RollingCandle | None, seconds: int) -> bool:
        return bool(
            candle is not None
            and candle.bucket_count >= seconds
            and candle.coverage_seconds >= seconds
        )

    def _rolling_metrics(
        self, state: SymbolState, end_sec: int
    ) -> tuple[
        RollingCandle | None,
        RollingCandle | None,
        RollingCandle | None,
        RollingCandle | None,
        RollingCandle | None,
        RollingCandle | None,
    ]:
        buckets = tuple(state.buckets)
        one = candle_from_buckets(self._window(buckets, end_sec - 59, end_sec))
        five = candle_from_buckets(self._window(buckets, end_sec - 299, end_sec))
        fifteen = candle_from_buckets(self._window(buckets, end_sec - 899, end_sec))
        w1 = candle_from_buckets(self._window(buckets, end_sec - 899, end_sec - 600))
        w2 = candle_from_buckets(self._window(buckets, end_sec - 599, end_sec - 300))
        w3 = candle_from_buckets(self._window(buckets, end_sec - 299, end_sec))
        return one, five, fifteen, w1, w2, w3

    @staticmethod
    def _metric_line(label: str, candle: RollingCandle | None) -> str:
        if candle is None:
            return f"{label}: unavailable"
        return (
            f"{label}: Body {candle.body_pct:+.2f}% | Range {candle.range_pct:.2f}% | "
            f"Buy {candle.buy_pct:.1f}% | Sell {candle.sell_pct:.1f}% | "
            f"CLV {candle.clv:.2f} | Turnover ₹{candle.turnover_cr:.2f} Cr"
        )

    def _signal_text(
        self,
        title: str,
        state: SymbolState,
        end_sec: int,
        lines: Sequence[str],
    ) -> str:
        event_time = datetime.fromtimestamp(end_sec, IST).strftime(
            "%d-%b-%Y %H:%M:%S IST"
        )
        day_change = (
            ((state.ltp - state.previous_close) / state.previous_close) * 100.0
            if state.previous_close > 0.0
            else 0.0
        )
        header = [
            title,
            f"{state.instrument.symbol} ({state.instrument.trading_symbol})",
            f"LTP ₹{state.ltp:,.2f} | ATP/VWAP ₹{state.atp:,.2f} | Day {day_change:+.2f}%",
        ]
        footer = [
            (
                f"Session Delta: Buy {format_volume(state.daily_buy_vol)} | "
                f"Sell {format_volume(state.daily_sell_vol)} | "
                f"Total {format_volume(state.daily_total_vol)}"
            ),
            event_time,
        ]
        return "\n".join([*header, *lines, *footer])

    def _emit_signal(
        self,
        state: SymbolState,
        alert_type: str,
        title: str,
        end_sec: int,
        lines: Sequence[str],
    ) -> bool:
        key = (state.instrument.symbol, alert_type)
        last = self.alert_cooldowns.get(key, 0)
        if end_sec - last < ALERT_COOLDOWN_SECONDS:
            return False
        text = self._signal_text(title, state, end_sec, lines)
        queued = self._put_alert(
            Alert(created_at=datetime.fromtimestamp(end_sec, IST), text=text)
        )
        if queued:
            self.alert_cooldowns[key] = end_sec
        return queued

    def _evaluate_alerts(
        self,
        state: SymbolState,
        latest_bucket: SecondBucket,
        fresh_high: bool,
        fresh_low: bool,
    ) -> None:
        end_sec = latest_bucket.sec_timestamp
        local = datetime.fromtimestamp(end_sec, IST)
        if (
            local.weekday() >= 5
            or local.time() < WARMUP_END
            or local.time() >= MARKET_CLOSE
            or end_sec - state.last_tick_sec > 5
        ):
            return

        one, five, fifteen, w1, w2, w3 = self._rolling_metrics(state, end_sec)
        if (
            five is None
            or five.turnover_cr * 10_000_000.0 < MIN_FIVE_MIN_TURNOVER_RUPEES
        ):
            return

        up_five = bool(
            five.coverage_seconds >= 240
            and five.body_pct >= 0.5
            and five.buy_pct >= 62.0
            and five.clv >= 0.72
        )
        up_one = bool(
            one is not None
            and one.coverage_seconds >= 50
            and one.body_pct >= 0.3
            and one.buy_pct >= 62.0
            and one.clv >= 0.72
        )
        if (up_five or up_one) and state.atp > 0.0 and state.ltp > state.atp:
            self._emit_signal(
                state,
                "FAST_ROLLING_UP",
                "🚀 FAST ROLLING UP (अचानक तेज़ खरीदारी)",
                end_sec,
                (self._metric_line("1m", one), self._metric_line("5m", five)),
            )

        down_five = bool(
            five.coverage_seconds >= 240
            and five.body_pct <= -0.5
            and five.sell_pct >= 62.0
            and five.buy_pct <= 38.0
            and five.clv <= 0.28
        )
        down_one = bool(
            one is not None
            and one.coverage_seconds >= 50
            and one.body_pct <= -0.3
            and one.sell_pct >= 62.0
            and one.buy_pct <= 38.0
            and one.clv <= 0.28
        )
        if (down_five or down_one) and state.atp > 0.0 and state.ltp < state.atp:
            self._emit_signal(
                state,
                "FAST_ROLLING_DOWN",
                "🩸 FAST ROLLING DOWN (अचानक तेज़ बिकवाली)",
                end_sec,
                (self._metric_line("1m", one), self._metric_line("5m", five)),
            )

        full_slow_windows = all(
            self._full_window(window, 300) for window in (w1, w2, w3)
        ) and self._full_window(fifteen, 900)
        if (
            full_slow_windows
            and w1 is not None
            and w2 is not None
            and w3 is not None
            and fifteen is not None
        ):
            cumulative_gain = ((w3.close - w1.open) / w1.open) * 100.0
            accumulating = (
                w3.high > w2.high > w1.high
                and w3.low > w2.low > w1.low
                and w3.close > w2.close > w1.close
                and cumulative_gain >= 0.75
                and fifteen.buy_pct >= 55.0
            )
            if accumulating:
                self._emit_signal(
                    state,
                    "SLOW_STEADY_ACCUMULATION",
                    "📈 SLOW STEADY ACCUMULATION (धीरे-धीरे ऊपर का ट्रेंड)",
                    end_sec,
                    (
                        f"15m gain {cumulative_gain:+.2f}% | Buy {fifteen.buy_pct:.1f}%",
                        f"W1/W2/W3 highs: {w1.high:.2f} < {w2.high:.2f} < {w3.high:.2f}",
                        f"W1/W2/W3 lows: {w1.low:.2f} < {w2.low:.2f} < {w3.low:.2f}",
                        f"W1/W2/W3 closes: {w1.close:.2f} < {w2.close:.2f} < {w3.close:.2f}",
                        self._metric_line("15m", fifteen),
                    ),
                )

            distributing = (
                w3.high < w2.high < w1.high
                and w3.low < w2.low < w1.low
                and w3.close < w2.close < w1.close
                and cumulative_gain <= -0.75
                and fifteen.sell_pct >= 55.0
            )
            if distributing:
                self._emit_signal(
                    state,
                    "SLOW_STEADY_DISTRIBUTION",
                    "📉 SLOW STEADY DISTRIBUTION (धीरे-धीरे नीचे का ट्रेंड)",
                    end_sec,
                    (
                        f"15m move {cumulative_gain:+.2f}% | Sell {fifteen.sell_pct:.1f}%",
                        f"W1/W2/W3 highs: {w1.high:.2f} > {w2.high:.2f} > {w3.high:.2f}",
                        f"W1/W2/W3 lows: {w1.low:.2f} > {w2.low:.2f} > {w3.low:.2f}",
                        f"W1/W2/W3 closes: {w1.close:.2f} > {w2.close:.2f} > {w3.close:.2f}",
                        self._metric_line("15m", fifteen),
                    ),
                )

            average_prior_five_min_volume = (w1.total_vol + w2.total_vol) / 2.0
            if average_prior_five_min_volume > 0.0:
                volume_multiple = five.total_vol / average_prior_five_min_volume
                candle_range = max(0.01, five.high - five.low)
                real_body = abs(five.close - five.open)
                lower_wick = max(0.0, min(five.open, five.close) - five.low)
                upper_wick = max(0.0, five.high - max(five.open, five.close))
                lower_rejection = (
                    lower_wick >= max(candle_range * 0.25, real_body * 1.5)
                    and five.clv >= 0.65
                )
                upper_rejection = (
                    upper_wick >= max(candle_range * 0.25, real_body * 1.5)
                    and five.clv <= 0.35
                )

                hidden_buyer = (
                    volume_multiple >= 2.2
                    and five.sell_pct >= 55.0
                    and (five.body_pct > -0.10 or lower_rejection)
                )
                if hidden_buyer:
                    self._emit_signal(
                        state,
                        "HIDDEN_BUYER_ABSORPTION",
                        "🛡️ HIDDEN BUYER ABSORPTION / ICEBERG (नीचे के लेवल पर छुपे खरीदार)",
                        end_sec,
                        (
                            (
                                f"5m volume {volume_multiple:.2f}x prior average | "
                                f"Sell {five.sell_pct:.1f}%"
                            ),
                            f"Lower wick ₹{lower_wick:.2f} | Rejection {'YES' if lower_rejection else 'NO'}",
                            self._metric_line("5m", five),
                        ),
                    )

                hidden_seller = (
                    volume_multiple >= 2.2
                    and five.buy_pct >= 55.0
                    and (five.body_pct < 0.10 or upper_rejection)
                )
                if hidden_seller:
                    self._emit_signal(
                        state,
                        "HIDDEN_SELLER_ABSORPTION",
                        "🧱 HIDDEN SELLER ABSORPTION / ICEBERG "
                        "(ऊपर के लेवल पर छुपे बिकवाल)",
                        end_sec,
                        (
                            (
                                f"5m volume {volume_multiple:.2f}x prior average | "
                                f"Buy {five.buy_pct:.1f}%"
                            ),
                            f"Upper wick ₹{upper_wick:.2f} | Rejection {'YES' if upper_rejection else 'NO'}",
                            self._metric_line("5m", five),
                        ),
                    )

        if state.previous_close > 0.0:
            day_change = (
                (state.ltp - state.previous_close) / state.previous_close
            ) * 100.0
            direction = 1 if day_change >= 0.0 else -1
            completed_steps = int(abs(day_change) // 1.5)
            for step in range(1, completed_steps + 1):
                signed_step = direction * step
                if signed_step in state.milestones_hit:
                    continue
                level = signed_step * 1.5
                alert_type = f"DAY_MILESTONE_{signed_step:+d}"
                queued = self._emit_signal(
                    state,
                    alert_type,
                    f"⚡ DAY MILESTONE {level:+.1f}%",
                    end_sec,
                    (
                        f"Crossed the {level:+.1f}% session milestone; current move {day_change:+.2f}%.",
                        self._metric_line("5m", five),
                    ),
                )
                if queued:
                    state.milestones_hit.add(signed_step)

        if local.time() >= BREAKOUT_START and one is not None:
            high_confirmed = (
                fresh_high
                and latest_bucket.agg_buy_vol > latest_bucket.agg_sell_vol
                and one.buy_vol > one.sell_vol
                and one.buy_pct >= 55.0
            )
            if high_confirmed:
                self._emit_signal(
                    state,
                    "FRESH_DAY_HIGH_BREAKOUT",
                    "⚡ FRESH DAY HIGH BREAKOUT",
                    end_sec,
                    (
                        (
                            f"Fresh day high ₹{state.observed_day_high or state.ltp:,.2f} "
                            "confirmed by positive executed-volume delta."
                        ),
                        self._metric_line("1m", one),
                        self._metric_line("5m", five),
                    ),
                )

            low_confirmed = (
                fresh_low
                and latest_bucket.agg_sell_vol > latest_bucket.agg_buy_vol
                and one.sell_vol > one.buy_vol
                and one.sell_pct >= 55.0
            )
            if low_confirmed:
                self._emit_signal(
                    state,
                    "FRESH_DAY_LOW_BREAKOUT",
                    "⚡ FRESH DAY LOW BREAKOUT",
                    end_sec,
                    (
                        (
                            f"Fresh day low ₹{state.observed_day_low or state.ltp:,.2f} "
                            "confirmed by negative executed-volume delta."
                        ),
                        self._metric_line("1m", one),
                        self._metric_line("5m", five),
                    ),
                )

    def _market_pulse(self, boundary_sec: int) -> None:
        current = datetime.fromtimestamp(boundary_sec, IST)
        live_states = [
            state
            for state in self.states.values()
            if state.session_date == current.date()
            and state.ltp > 0.0
            and boundary_sec - state.last_tick_sec <= 120
        ]
        advancing = 0
        declining = 0
        unchanged = 0
        above_vwap = 0
        below_vwap = 0
        at_vwap = 0
        aggressive_buying = 0
        aggressive_selling = 0
        balanced_flow = 0

        for state in live_states:
            if state.previous_close > 0.0:
                if state.ltp > state.previous_close:
                    advancing += 1
                elif state.ltp < state.previous_close:
                    declining += 1
                else:
                    unchanged += 1
            if state.atp > 0.0:
                if state.ltp > state.atp:
                    above_vwap += 1
                elif state.ltp < state.atp:
                    below_vwap += 1
                else:
                    at_vwap += 1
            if state.daily_buy_vol > state.daily_sell_vol:
                aggressive_buying += 1
            elif state.daily_sell_vol > state.daily_buy_vol:
                aggressive_selling += 1
            else:
                balanced_flow += 1

        timestamp = current.strftime("%d-%b-%Y %H:%M:%S IST")
        text = "\n".join(
            (
                "📊 NIFTY 200 MARKET PULSE (5-MINUTE)",
                f"Advancing {advancing} | Declining {declining} | Unchanged {unchanged}",
                f"Above VWAP {above_vwap} | Below VWAP {below_vwap} | At VWAP {at_vwap}",
                (
                    f"Net Aggressive Buying {aggressive_buying} | "
                    f"Net Aggressive Selling {aggressive_selling} | Balanced {balanced_flow}"
                ),
                (
                    f"Live/Tracked {len(live_states)}/200 | Feed "
                    f"{'CONNECTED' if self.feed_connected.is_set() else 'DISCONNECTED'}"
                ),
                (
                    f"Tick queue {self.tick_queue.qsize():,} | "
                    f"Dropped ticks {self.dropped_ticks:,} | "
                    f"Reconnects {self.websocket_reconnects:,}"
                ),
                timestamp,
            )
        )
        self._put_alert(Alert(created_at=current, text=text))

    def _roll_clock(self, now_sec: int) -> None:
        current = datetime.fromtimestamp(now_sec, IST)
        if current.weekday() < 5:
            open_epoch, close_epoch = market_bounds(current.date())
            if now_sec > open_epoch:
                target = min(now_sec, close_epoch)
                for state in tuple(self.states.values()):
                    if state.session_date == current.date():
                        self._advance_state_to(state, target)

            pulse_allowed = WARMUP_END <= current.time() <= MARKET_CLOSE
            pulse_slot = now_sec // 300
            if self.last_pulse_date != current.date():
                self.last_pulse_date = current.date()
                self.last_pulse_slot = pulse_slot
            elif pulse_allowed and pulse_slot != self.last_pulse_slot:
                self._market_pulse(now_sec)
                self.last_pulse_slot = pulse_slot

    def _quant_engine(self) -> None:
        last_clock_sec = int(time.time())
        while not self.stop_event.is_set() or not self.tick_queue.empty():
            try:
                item = self.tick_queue.get(timeout=0.05)
                batch = [item]
                batch_started = time.monotonic()
                while len(batch) < 4_096 and time.monotonic() - batch_started < 0.020:
                    try:
                        batch.append(self.tick_queue.get_nowait())
                    except queue.Empty:
                        break
                for receipt_ns, payload in batch:
                    receipt_sec = receipt_ns // 1_000_000_000
                    found_record = False
                    try:
                        for record in self._iter_tick_records(payload):
                            found_record = True
                            normalized = self._normalize_tick(record, receipt_sec)
                            if normalized is not None:
                                self._process_tick(normalized)
                        if not found_record:
                            self.malformed_payloads += 1
                    except Exception as exc:  # noqa: BLE001 - isolate external API/library failures
                        self.malformed_payloads += 1
                        if (
                            self.malformed_payloads <= 5
                            or self.malformed_payloads % 1_000 == 0
                        ):
                            LOGGER.warning(
                                "Malformed feed payload ignored: %s",
                                self._redact(str(exc)),
                            )
            except queue.Empty:
                batch = []

            now_sec = int(time.time())
            if now_sec > last_clock_sec:
                self._roll_clock(now_sec)
                last_clock_sec = now_sec

        self._roll_clock(int(time.time()))

    def _console_dispatcher(self) -> None:
        while (
            not self.stop_event.is_set()
            or not self.producer_done.is_set()
            or not self.quant_done.is_set()
            or not self.alert_queue.empty()
        ):
            try:
                alert = self.alert_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            timestamp = alert.created_at.astimezone(IST).strftime(
                "%d-%b-%Y %H:%M:%S IST"
            )
            print(f"\n[{timestamp}]\n{alert.text}\n", flush=True)

    def _thread_entry(self, name: str, target: Any) -> None:
        try:
            target()
        except BaseException as exc:
            safe_error = self._redact(str(exc))[:500]
            LOGGER.exception("Worker %s terminated unexpectedly: %s", name, safe_error)
            self._put_alert(
                Alert(
                    created_at=datetime.now(IST),
                    text=f"🛑 SCANNER WORKER FAILED\n{name}: {safe_error}",
                )
            )
            self.stop_event.set()
        finally:
            if name == "kotak-websocket-producer":
                self.producer_done.set()
            elif name == "quant-rolling-engine":
                self.quant_done.set()

    def _start_threads(self) -> None:
        definitions = (
            ("console-alert-dispatcher", self._console_dispatcher),
            ("quant-rolling-engine", self._quant_engine),
            ("kotak-websocket-producer", self._websocket_producer),
        )
        self.threads = [
            threading.Thread(
                name=name,
                target=self._thread_entry,
                args=(name, target),
                daemon=False,
            )
            for name, target in definitions
        ]
        for thread in self.threads:
            thread.start()

    def request_shutdown(self, reason: str) -> None:
        if not self.stop_event.is_set():
            LOGGER.info("Shutdown requested: %s", reason)
            self.stop_event.set()

    def _logout(self) -> None:
        with self.client_lock:
            client = self.client
        if client is None:
            return
        try:
            result = client.logout()
            error = api_error_text(result)
            if error:
                LOGGER.warning("Kotak logout returned an error: %s", error)
        except Exception as exc:  # noqa: BLE001 - isolate external API/library failures
            LOGGER.warning("Kotak logout failed: %s", self._redact(str(exc)))

    def run(self) -> None:
        self.authenticate()
        self.refresh_instrument_mapping()
        self._start_threads()
        self._put_system_alert(
            "scanner-started",
            "✅ NIFTY 200 REAL-TIME SCANNER STARTED\n"
            "3-thread pipeline active: Kotak WebSocket producer, 1-second quant engine, "
            "and non-blocking console dispatcher.",
            cooldown_seconds=1.0,
        )
        try:
            while not self.stop_event.wait(1.0):
                dead_threads = [
                    thread.name for thread in self.threads if not thread.is_alive()
                ]
                if dead_threads:
                    raise RuntimeError(
                        "Worker thread(s) stopped unexpectedly: "
                        + ", ".join(dead_threads)
                    )
        finally:
            self.stop_event.set()
            threads_by_name = {thread.name: thread for thread in self.threads}
            for name in (
                "kotak-websocket-producer",
                "quant-rolling-engine",
                "console-alert-dispatcher",
            ):
                threads_by_name[name].join(timeout=15.0)
            still_alive = [thread.name for thread in self.threads if thread.is_alive()]
            if still_alive:
                LOGGER.warning("Threads still active during shutdown: %s", still_alive)
            self._logout()
            LOGGER.info(
                "Scanner stopped: payloads=%s normalized_ticks=%s dropped_ticks=%s "
                "malformed=%s dropped_alerts=%s reconnects=%s",
                self.received_payloads,
                self.normalized_ticks,
                self.dropped_ticks,
                self.malformed_payloads,
                self.dropped_alerts,
                self.websocket_reconnects,
            )


def main() -> int:
    configure_logging()
    try:
        config = ScannerConfig.from_env()
        scanner = KotakNifty200Scanner(config)

        def handle_signal(signum: int, _frame: Any) -> None:
            scanner.request_shutdown(f"signal {signum}")

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
        scanner.run()
        return 0
    except KeyboardInterrupt:
        LOGGER.info("Interrupted by keyboard")
        return 130
    except Exception:
        LOGGER.exception("Fatal scanner error")
        return 1


if __name__ == "__main__":
    sys.exit(main())
