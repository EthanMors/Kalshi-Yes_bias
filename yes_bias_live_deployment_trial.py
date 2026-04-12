# LIVE TRADING — USES REAL MONEY. Set KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH
# env vars before running (RSA key-pair authentication, not a bearer token).
#
# YES Bias Fade — Live Deployment Trial
# Strategy: Buy NO contracts on KXHIGH daily-high-temperature markets for
#            5 cities (LAX, NY, CHI, MIA, DEN) when YES taker trades in the
#            0.55–0.65 band appear before 3pm local time.
#
# Trial mode: $30 account, 1 contract max per trade, hold to settlement.
# Hard stop:  no new entries at or after 17:00 America/Los_Angeles (5pm PST).
#
# Usage:
#   export KALSHI_API_KEY_ID="your-api-key-id"
#   export KALSHI_PRIVATE_KEY_PATH="/path/to/your/private-key.key"
#   python yes_bias_live_deployment_trial.py
#
# Install dependencies:
#   pip install pykalshi kalshi-python

import asyncio
import json
import math
import csv
import logging
import os
import re
import sys
import time
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import pandas as pd
from pykalshi import AsyncKalshiClient, AsyncRateLimiter
from pykalshi.enums import MarketStatus, OrderStatus, OrderType
from pykalshi.exceptions import AuthenticationError
# kalshi-python (official Kalshi OpenAPI client) — used for order placement only.
import kalshi_python
from kalshi_python.api.portfolio_api import PortfolioApi as KalshiPortfolioApi
from kalshi_python.models import CreateOrderRequest
load_dotenv()
from orderbook_collector import OrderBookCollector
# ── Account ─────────────────────────────────────────────────────────────────────
STARTING_BALANCE = 30.0          # USD — trial account
MIN_CONTRACTS    = 1
MAX_CONTRACTS    = 1             # cap at 1 for live trial

# ── Strategy parameters ──────────────────────────────────────────────────────────
YES_PRICE_LOW    = 0.55
YES_PRICE_HIGH   = 0.65
# NO price valid range (derived from YES range): 1 - YES_PRICE_HIGH to 1 - YES_PRICE_LOW
NO_PRICE_LOW     = round(1.0 - YES_PRICE_HIGH, 2)   # 0.35
NO_PRICE_HIGH    = round(1.0 - YES_PRICE_LOW,  2)   # 0.45

# ── Timing ───────────────────────────────────────────────────────────────────────
STOP_TIMEZONE        = ZoneInfo("America/Los_Angeles")
START_HOUR           = 4             # 4:00 AM PST -- earliest time to begin trading
STOP_HOUR            = 17            # no new trades at or after 5pm PST/PDT
POLL_INTERVAL_SEC    = 10            # seconds between API polling cycles per ticker
SETTLEMENT_LOG_MINUTE = 15           # snapshot filled orders at 16:45 PST (15 min before hard stop)
SHUTDOWN_MINUTE       = 30           # graceful process exit at 17:30 PST

# ── Order hold window ────────────────────────────────────────────────────────────
ORDER_HOLD_SEC         = 3600  # 1 hour — cancel unfilled limit orders after this
FILL_POLL_INTERVAL_SEC = 60    # poll every 60 seconds while waiting for a fill

# ── Rate limiting ────────────────────────────────────────────────────────────────
# Kalshi REST API limit: 10 req/s (pykalshi default). Each coroutine acquires
# a slot from the shared AsyncRateLimiter before every API call.
API_REQUESTS_PER_SECOND = 10

# ── pykalshi auth — set these env vars before running ────────────────────────────
# KALSHI_API_KEY_ID:       your API key ID (shown in the Kalshi dashboard)
# KALSHI_PRIVATE_KEY_PATH: path to your RSA private key .key file
KALSHI_API_KEY_ID        = os.environ.get("KALSHI_API_KEY_ID", "")
KALSHI_PRIVATE_KEY_PATH  = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")

# ── Logging ──────────────────────────────────────────────────────────────────────
CSV_LOG_PATH        = Path(__file__).parent / "Yes_bias_live_trade_data.csv"
SETTLEMENT_LOG_PATH = Path(__file__).parent / "settlement_log.csv"
LOG_FILE            = Path(__file__).parent / "yes_bias_bot.log"
LOG_LEVEL           = logging.INFO
JSON_LOG_DIR        = Path(__file__).parent / "JSON_logs"
ERROR_400_LOG_PATH  = Path(__file__).parent / "indepth_error_400.log"

# ── Settlement log columns ────────────────────────────────────────────────────────
SETTLEMENT_COLUMNS = [
    "date_utc", "ticker", "city", "order_id", "filled_count",
    "filled_avg_price", "order_type", "settlement_recorded",
    "settled_at_utc", "market_result", "pnl_dollars",
]

# ── Universe ─────────────────────────────────────────────────────────────────────
CITIES = {
    "LAX": {"prefix": "KXHIGHLAX"},
    "NY":  {"prefix": "KXHIGHNY"},
   # "CHI": {"prefix": "KXHIGHCHI"},
    "MIA": {"prefix": "KXHIGHMIA"},
    "DEN": {"prefix": "KXHIGHDEN"},
}

# ── CSV columns ──────────────────────────────────────────────────────────────────
CSV_COLUMNS = [
    "logged_at_utc", "city", "ticker", "signal_yes_price", "signal_no_price",
    "signal_trade_id", "signal_time_utc", "signal_time_local", "order_id",
    "order_side", "order_limit_price", "order_status", "filled_count",
    "filled_avg_price", "estimated_fee", "order_type", "balance_before",
    "balance_after", "skip_reason", "notes",
]

# ── Ticker date regex ────────────────────────────────────────────────────────────
DATE_RE = re.compile(r"-(\d{2}[A-Z]{3}\d{2})-")


# ────────────────────────────────────────────────────────────────────────────────
# Logging Setup
# ────────────────────────────────────────────────────────────────────────────────

def setup_logging() -> None:
    """Configure root logger to write to both console and a rotating log file."""
    fmt = "%(asctime)s | %(levelname)s | %(message)s"
    logging.basicConfig(
        level=LOG_LEVEL,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
        ],
    )
    JSON_LOG_DIR.mkdir(parents=True, exist_ok=True)


# ────────────────────────────────────────────────────────────────────────────────
# pykalshi async helpers
# ────────────────────────────────────────────────────────────────────────────────

async def get_balance_dollars(kalshi: AsyncKalshiClient) -> float:
    """Return the available balance in dollars.

    pykalshi's BalanceModel.balance is an integer in cents.

    Args:
        kalshi: Authenticated AsyncKalshiClient instance.

    Returns:
        Available balance in dollars.
    """
    balance_model = await kalshi.portfolio.get_balance()
    return balance_model.balance / 100.0


async def get_available_balance(kalshi: AsyncKalshiClient) -> float:
    """Return the available trading balance in dollars.

    Kalshi's GET /portfolio/balance already returns the *available* balance
    (i.e. total cash minus capital reserved by resting open orders). The
    get_resting_order_value endpoint is FCM-only and returns 403 for retail
    accounts, so we rely solely on get_balance() here.

    Args:
        kalshi: Authenticated AsyncKalshiClient instance.

    Returns:
        Available dollars (already net of resting-order reservations).
    """
    balance_model = await kalshi.portfolio.get_balance()
    return balance_model.balance / 100.0


async def get_markets_for_series(
    kalshi: AsyncKalshiClient, series_ticker: str
) -> list:
    """Return all open Market objects for a given series ticker.

    Args:
        kalshi:        Authenticated AsyncKalshiClient instance.
        series_ticker: e.g. "KXHIGHLAX"

    Returns:
        List of pykalshi AsyncMarket objects whose status is open.
    """
    return await kalshi.get_markets(
        series_ticker=series_ticker,
        status=MarketStatus.OPEN,
        limit=200,
    )


async def get_recent_trades(
    kalshi: AsyncKalshiClient,
    ticker: str,
    min_ts_iso: str | None = None,
) -> list:
    """Return recent public taker trades for a market as a list of dicts.

    pykalshi's client.get_trades() accepts min_ts as an integer Unix timestamp
    in milliseconds. This function converts the ISO 8601 string high-water mark
    used by the rest of the bot into the required integer form.

    Each returned dict contains:
      trade_id, ticker, yes_price (cents int), no_price (cents int),
      count, taker_side, created_time (ISO 8601 string).

    These dicts intentionally mirror the old raw-requests format so the
    signal detection and CSV-logging code requires zero changes.

    Args:
        kalshi:      Authenticated AsyncKalshiClient instance.
        ticker:      Market ticker, e.g. "KXHIGHLAX-26MAR16-B72.5".
        min_ts_iso:  ISO 8601 high-water mark; trades at or before this
                     timestamp are excluded (client-side filter as fallback).

    Returns:
        List of trade dicts, newest first.
    """
    # Convert ISO high-water mark to integer seconds for the API.
    # IMPORTANT: Kalshi API requires seconds, NOT milliseconds.
    min_ts_sec: int | None = None
    if min_ts_iso:
        ts = pd.Timestamp(min_ts_iso)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        min_ts_sec = int(ts.timestamp())  # seconds since epoch (NOT ms)
        # Sanity guard: a value > 1e12 means milliseconds were passed by mistake.
        assert min_ts_sec < 10_000_000_000, (
            f"min_ts_sec={min_ts_sec} looks like milliseconds — divide by 1000"
        )

    trade_models = await kalshi.get_trades(
        ticker=ticker,
        min_ts=min_ts_sec,
        limit=100,
    )

    # Convert TradeModel objects to dicts in the legacy shape.
    # TradeModel uses dollar strings; convert yes/no prices to cents ints
    # so is_qualifying_signal() and _blank_row() work unchanged.
    trades: list[dict] = []
    for t in trade_models:
        yes_cents = int(round(float(t.yes_price_dollars) * 100))
        no_cents  = int(round(float(t.no_price_dollars)  * 100))
        count_val = t.count_fp   # fixed-point string e.g. "1.00"
        trades.append({
            "trade_id":    t.trade_id,
            "ticker":      t.ticker,
            "yes_price":   yes_cents,   # cents int — matches old API shape
            "no_price":    no_cents,    # cents int
            "count":       count_val,
            "taker_side":  t.taker_side or "",
            "created_time": t.created_time or "",
        })

    # Client-side dedup: drop any trades at or before min_ts_iso in case
    # the API's min_ts filter is inclusive or partially applied.
    if min_ts_iso and trades:
        trades = [t for t in trades if t.get("created_time", "") > min_ts_iso]

    return trades


def snap_to_tick(price_dollars: float, market) -> str:
    """Snap a dollar price to the market's valid tick grid and return a formatted string.

    Reads market.tick_size_dollars first (most precise), then falls back to
    price_level_structure to infer the tick. Bracket markets often use
    tapered_deci_cent (0.001 tick in the tails) while threshold markets use
    linear_cent (0.01 tick across the full range).

    Args:
        price_dollars: Raw price in dollars (e.g. 0.437).
        market:        Market model object with tick_size_dollars /
                       price_level_structure attributes.

    Returns:
        Price string formatted to the correct decimal places (e.g. "0.437").
    """
    price = Decimal(str(price_dollars))

    # Prefer the explicit tick_size_dollars field when the server populates it.
    tick_str = getattr(market, "tick_size_dollars", None)
    if tick_str:
        tick = Decimal(str(tick_str))
    else:
        pls = getattr(market, "price_level_structure", None) or "linear_cent"
        if pls == "deci_cent":
            tick = Decimal("0.001")
        elif pls == "tapered_deci_cent":
            # Tails (< 0.10 or > 0.90) use 0.001; mid-range uses 0.01.
            if price <= Decimal("0.10") or price >= Decimal("0.90"):
                tick = Decimal("0.001")
            else:
                tick = Decimal("0.01")
        else:  # linear_cent — threshold markets and the default
            tick = Decimal("0.01")

    # Snap DOWN to the nearest valid tick (conservative — don't overpay).
    snapped = (price / tick).to_integral_value(rounding="ROUND_DOWN") * tick
    # Format with enough decimal places to represent the tick precisely.
    decimal_places = max(abs(tick.as_tuple().exponent), 2)
    return f"{snapped:.{decimal_places}f}"


async def get_best_no_ask(kalshi: AsyncKalshiClient, market) -> float | None:
    """Return the best (lowest) NO ask price snapped to the market tick, or None.

    On Kalshi, NO ask = 1.00 - best YES bid (book.best_bid).
    pykalshi's OrderbookResponse exposes best_yes_bid as the highest resting YES bid.

    Args:
        kalshi: Authenticated AsyncKalshiClient instance.
        market: Market object returned by kalshi.get_market().

    Returns:
        Best NO ask snapped to the market's tick grid (e.g. 0.40 or 0.403), or None.
    """
    try:
        book     = await market.get_orderbook()
        best_bid = book.best_yes_bid   # best YES bid, dollar string or None
        if best_bid is not None:
            raw = float(Decimal("1") - Decimal(best_bid))
            return float(snap_to_tick(raw, market))
    except Exception as exc:
        logging.warning(f"Could not fetch order book for {market.ticker}: {exc}")
    return None


async def place_no_order(
    kalshi: AsyncKalshiClient,
    market: object,
    count: int,
    no_price_dollars: float,
    client_order_id: str,
) -> object:
    """Submit a limit buy-NO order via kalshi-python (official Kalshi OpenAPI client).

    kalshi-python is synchronous; the actual HTTP call is offloaded to a thread
    executor so the async event loop is never blocked.

    Args:
        kalshi:            Authenticated AsyncKalshiClient instance (used for
                           tick-snap metadata only — not for the API call).
        market:            Market object from kalshi.get_market().
        count:             Number of whole contracts.
        no_price_dollars:  Limit price in dollars (e.g. 0.43). Snapped to valid tick.
        client_order_id:   Idempotency key.

    Returns:
        kalshi_python.models.Order object (has .order_id, .status, .no_price, etc.).
    """
    # Snap to the market's tick grid.
    no_price_str = snap_to_tick(no_price_dollars, market)
    pls = getattr(market, "price_level_structure", "linear_cent") or "linear_cent"
    logging.debug(
        f"place_no_order | {market.ticker} | pls={pls} | "
        f"raw={no_price_dollars} → snapped={no_price_str}"
    )

    _ticker_str = getattr(market, "ticker", str(market))
    _market_type = "bracket" if re.search(r"-B\d", _ticker_str) else "threshold"

    # Kalshi API expects NO price as a dollar decimal rounded to 4 decimal places
    # (e.g. 0.3600), NOT as integer cents.  model_construct bypasses the SDK's
    # ge=1 Pydantic constraint which was written for the old cents format.
    no_price_4dp = round(float(no_price_str), 4)

    # Build the request for audit logging and submission.
    order_request = CreateOrderRequest.model_construct(
        ticker          = _ticker_str,
        client_order_id = client_order_id,
        action          = "buy",
        side            = "no",
        count           = count,
        type            = "limit",
        no_price        = no_price_4dp,
    )
    _request_body = order_request.to_dict()

    try:
        # Run the synchronous kalshi-python call in a thread so we don't block
        # the event loop while waiting for the HTTP response.
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None,
            lambda: _kp_portfolio.create_order(
                create_order_request=order_request,
            ),
        )
    except Exception as exc:
        _write_jsonl_log({
            "timestamp_utc": datetime.now(tz=timezone.utc).isoformat(),
            "event":         "order_error",
            "ticker":        _ticker_str,
            "market_type":   _market_type,
            "request_body":  _request_body,
            "error_type":    type(exc).__name__,
            "error_message": str(exc),
        })
        _log_error_400(exc, _ticker_str, "place_no_order", request_body=_request_body)
        raise

    order = response.order
    _write_jsonl_log({
        "timestamp_utc":     datetime.now(tz=timezone.utc).isoformat(),
        "event":             "order_placed",
        "ticker":            _ticker_str,
        "market_type":       _market_type,
        "request_body":      _request_body,
        "response_order_id": order.order_id,
        "response_status":   order.status if isinstance(order.status, str) else str(order.status),
    })
    return order


async def get_order_status(kalshi: AsyncKalshiClient, order_id: str) -> object:
    """Return the current Order object for a given order ID.

    Args:
        kalshi:   Authenticated AsyncKalshiClient instance.
        order_id: Kalshi order ID string.

    Returns:
        pykalshi AsyncOrder object.
    """
    return await kalshi.portfolio.get_order(order_id)


async def get_position_for_ticker(
    kalshi: AsyncKalshiClient, ticker: str
) -> object | None:
    """Return the PositionModel for a specific ticker, or None.

    Used after order placement to confirm a fill occurred.

    Args:
        kalshi: Authenticated AsyncKalshiClient instance.
        ticker: Market ticker.

    Returns:
        PositionModel or None if no position exists.
    """
    try:
        positions = await kalshi.portfolio.get_positions(ticker=ticker)
        if positions:
            return positions[0]
    except Exception as exc:
        logging.warning(f"Could not fetch position for {ticker}: {exc}")
    return None


# ────────────────────────────────────────────────────────────────────────────────
# CSV Logging Helpers
# ────────────────────────────────────────────────────────────────────────────────

def log_row(writer: csv.DictWriter, row: dict) -> None:
    """Write one row to the CSV audit log and flush immediately.

    Writing on every event means no data is lost if the process is killed.
    """
    writer.writerow(row)
    # DictWriter does not expose the underlying file directly; flush via
    # the writer's internal reference stored in the calling scope.
    # Flushing is handled by the caller using csv_file.flush().


def _log_error_400(exc: Exception, ticker: str, context: str, request_body: dict | None = None) -> None:
    """Append a detailed entry to indepth_error_400.log whenever Kalshi returns a 400.

    Handles both pykalshi (exc.status_code) and kalshi-python (exc.status)
    exception shapes. Only fires on 400 responses; all errors are swallowed so
    this can never crash the bot.
    """
    # Support both pykalshi (status_code) and kalshi-python (status) attribute names.
    status_code = getattr(exc, "status_code", None) or getattr(exc, "status", None)
    if status_code != 400:
        return
    try:
        timestamp = datetime.now(tz=timezone.utc).isoformat()
        separator = "=" * 70
        lines = [
            separator,
            f"TIMESTAMP   : {timestamp}",
            f"TICKER      : {ticker}",
            f"CONTEXT     : {context}",
            f"EXCEPTION   : {type(exc).__name__}",
            f"STATUS CODE : {status_code}",
            f"ERROR CODE  : {getattr(exc, 'error_code', None)}",
            f"MESSAGE     : {getattr(exc, 'message', None) or getattr(exc, 'reason', str(exc))}",
            f"METHOD      : {getattr(exc, 'method', None)}",
            f"ENDPOINT    : {getattr(exc, 'endpoint', None)}",
            f"REQUEST BODY:",
            json.dumps(request_body or getattr(exc, "request_body", None), indent=2, default=str),
            f"RESPONSE BODY:",
            json.dumps(
                getattr(exc, "response_body", None) or getattr(exc, "body", None),
                indent=2, default=str
            ),
            separator,
            "",
        ]
        with open(ERROR_400_LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write("\n".join(lines))
    except Exception as log_exc:
        logging.warning(f"_log_error_400 failed (entry not saved): {log_exc}")


def _write_jsonl_log(record: dict) -> None:
    """Append one JSON record (one line) to the date-labelled JSONL log file.

    The file lives at JSON_logs/YYYY-MM-DD.jsonl (UTC date).  All errors are
    swallowed with a WARNING so that logging can NEVER crash the bot.

    Args:
        record: Arbitrary dict that will be serialised to a single JSON line.
    """
    try:
        utc_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        log_path = JSON_LOG_DIR / f"{utc_date}.jsonl"
        line = json.dumps(record, default=str) + "\n"
        with open(log_path, "a", encoding="utf-8") as fh:
            fh.write(line)
    except Exception as exc:
        logging.warning(f"_write_jsonl_log failed (record not saved): {exc}")


def _blank_row(ticker: str, city: str, trade: dict) -> dict:
    """Build a minimal CSV row from signal metadata (used for skip rows)."""
    yes_price = trade.get("yes_price", 0) / 100.0
    no_price  = trade.get("no_price", 0) / 100.0
    try:
        signal_local = pd.Timestamp(trade["created_time"]).tz_convert(STOP_TIMEZONE).isoformat()
    except Exception:
        signal_local = ""

    return {
        "logged_at_utc":    datetime.now(tz=timezone.utc).isoformat(),
        "city":             city,
        "ticker":           ticker,
        "signal_yes_price": yes_price,
        "signal_no_price":  no_price,
        "signal_trade_id":  trade.get("trade_id", ""),
        "signal_time_utc":  trade.get("created_time", ""),
        "signal_time_local": signal_local,
        "order_id":         "",
        "order_side":       "",
        "order_limit_price": "",
        "order_status":     "",
        "filled_count":     "",
        "filled_avg_price": "",
        "estimated_fee":    "",
        "order_type":       "",
        "balance_before":   "",
        "balance_after":    "",
        "skip_reason":      "",
        "notes":            "",
    }


def log_skip(
    writer: csv.DictWriter,
    csv_file,
    ticker: str,
    city: str,
    trade: dict,
    reason: str,
) -> None:
    """Write a skip row to the CSV. Used when a qualifying signal is ignored."""
    row = _blank_row(ticker, city, trade)
    row["skip_reason"] = reason
    logging.info(f"SKIP | {city} | {ticker} | reason={reason}")
    log_row(writer, row)
    csv_file.flush()


# ────────────────────────────────────────────────────────────────────────────────
# Signal Detection
# ────────────────────────────────────────────────────────────────────────────────

def is_qualifying_signal(trade: dict) -> bool:
    """Return True if this trade triggers the YES Bias Fade entry signal.

    Signal conditions (ALL must hold):
      1. yes_price in [YES_PRICE_LOW, YES_PRICE_HIGH] (converted from cents).
      2. taker_side == "yes" (a YES taker buy — retail over-payment).
      3. created_time converts to pst_hour < 15 (before 3pm PST).

    Args:
        trade: Trade dict (yes_price and no_price are cents ints).

    Returns:
        True if all three conditions are satisfied, False otherwise.
    """
    yes_price  = trade.get("yes_price", 0) / 100.0
    taker_side = trade.get("taker_side", "").lower()
    created_ts = pd.Timestamp(trade["created_time"])
    # created_time may already carry timezone info; tz_convert handles both.
    if created_ts.tzinfo is None:
        created_ts = created_ts.tz_localize("UTC")
    pst_hour = created_ts.tz_convert(STOP_TIMEZONE).hour

    return (
        YES_PRICE_LOW <= yes_price <= YES_PRICE_HIGH
        and taker_side == "yes"
        and pst_hour < 15
    )


# ────────────────────────────────────────────────────────────────────────────────
# Market Helpers
# ────────────────────────────────────────────────────────────────────────────────

_MONTH_ABBR = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]


def _utc_today_kalshi_str() -> str:
    """Return today's date in Kalshi ticker format, e.g. '26MAR17'. Locale-independent."""
    now = datetime.now(tz=timezone.utc)
    return f"{now.strftime('%y')}{_MONTH_ABBR[now.month - 1]}{now.strftime('%d')}"


def filter_todays_markets(markets: list) -> list:
    """Keep only markets whose ticker encodes today's date (UTC).

    Ticker format example: KXHIGHLAX-26MAR16-B72.5
    Date portion format:   YYMmmDD  (two-digit year, three-letter month, two-digit day)

    Args:
        markets: Raw market list from get_markets_for_series (Market objects).

    Returns:
        Subset of markets that expire today (UTC).
    """
    today_str = _utc_today_kalshi_str()   # e.g. "26MAR16"
    return [m for m in markets if today_str in m.ticker]


# ────────────────────────────────────────────────────────────────────────────────
# Entry Execution (async)
# ────────────────────────────────────────────────────────────────────────────────

async def _cancel_order_safe(kalshi: AsyncKalshiClient, order_id: str) -> None:
    """Cancel an open order, swallowing errors (best-effort).

    Args:
        kalshi:   Authenticated AsyncKalshiClient instance.
        order_id: Kalshi order ID to cancel.
    """
    try:
        await kalshi.portfolio.cancel_order(order_id)
        logging.debug(f"Cancelled order {order_id}.")
    except Exception as exc:
        logging.warning(f"Could not cancel order {order_id}: {exc}")


def _is_no_price_in_range(no_price: float) -> bool:
    """Return True if the NO price is still within the valid entry band.

    Valid NO price range: [NO_PRICE_LOW, NO_PRICE_HIGH] = [0.35, 0.45],
    which corresponds to YES price range [YES_PRICE_LOW, YES_PRICE_HIGH] = [0.55, 0.65].

    Args:
        no_price: NO price in dollars.

    Returns:
        True if within valid range.
    """
    return NO_PRICE_LOW <= round(no_price, 2) <= NO_PRICE_HIGH


def _is_stop_time_reached() -> bool:
    """Return True if the hard stop time (5pm PST) has been reached."""
    now_pst = datetime.now(tz=STOP_TIMEZONE)
    return now_pst.hour >= STOP_HOUR


async def _poll_for_fill(
    kalshi: AsyncKalshiClient,
    order_id: str,
    wait_seconds: int,
    stage_label: str,
    ticker: str,
    shutdown_event: asyncio.Event | None = None,
) -> tuple[bool, str, str]:
    """Poll order status until filled, time window exhausted, or stop time reached.

    Polls every FILL_POLL_INTERVAL_SEC seconds using asyncio.sleep so other
    market coroutines continue running during the wait.

    Args:
        kalshi:         Authenticated AsyncKalshiClient instance.
        order_id:       Order ID to poll.
        wait_seconds:   Maximum total seconds to wait.
        stage_label:    Human-readable label for log messages.
        ticker:         Market ticker (for log messages).
        shutdown_event: Optional asyncio.Event; if set, poll exits immediately.

    Returns:
        Tuple of (filled: bool, order_status: str, fill_count_fp: str).
        If stop time is reached mid-wait, returns (False, "stop_time", "0").
        If shutdown is signalled, returns (False, "shutdown", fill_count_fp).
        On repeated API failures, returns current known state.
    """
    deadline = time.monotonic() + wait_seconds
    order_status  = "resting"
    fill_count_fp = "0"

    while time.monotonic() < deadline:
        # Shutdown check mid-wait
        if shutdown_event is not None and shutdown_event.is_set():
            return False, "shutdown", fill_count_fp

        # Hard stop check mid-wait
        if _is_stop_time_reached():
            logging.info(
                f"{ticker} [{stage_label}]: stop time reached during fill wait — "
                "aborting escalation."
            )
            return False, "stop_time", fill_count_fp

        try:
            refreshed     = await get_order_status(kalshi, order_id)
            order_status  = refreshed.status.value
            fill_count_fp = refreshed.fill_count_fp or fill_count_fp

            if order_status == "executed":
                logging.info(
                    f"{ticker} [{stage_label}]: FILLED as {stage_label}."
                )
                return True, order_status, fill_count_fp

            if order_status == "canceled":
                logging.warning(
                    f"{ticker} [{stage_label}]: order was cancelled externally."
                )
                return False, order_status, fill_count_fp

        except Exception as exc:
            logging.warning(
                f"{ticker} [{stage_label}]: error polling order {order_id}: {exc}"
            )

        # Async sleep — yields control to the event loop so other ticker
        # coroutines can poll while this one waits for a fill.
        remaining = deadline - time.monotonic()
        sleep_secs = min(FILL_POLL_INTERVAL_SEC, max(0.0, remaining))
        if sleep_secs > 0:
            await asyncio.sleep(sleep_secs)

    return False, order_status, fill_count_fp


async def attempt_entry(
    kalshi: AsyncKalshiClient,
    writer: csv.DictWriter,
    csv_file,
    csv_lock: asyncio.Lock,
    balance_lock: asyncio.Lock,
    ticker: str,
    city: str,
    signal_trade: dict,
    shutdown_event: asyncio.Event | None = None,
) -> dict:
    """Attempt to buy 1 NO contract using a single resting limit order.

    Execution flow:
      1. Pre-flight book check: fetch best NO ask; if outside valid range, skip.
      2. Pre-flight balance check (under balance_lock): verify sufficient funds.
      3. Re-fetch best NO ask inside the lock; post ONE limit order at exactly
         the best NO ask price (no slippage buffer added).
      4. Poll for fill up to ORDER_HOLD_SEC (1 hour).
      5. If filled  → log via _log_filled with maker fee rate (1.75%) and return
         {"executed": True}.
         If stop_time/shutdown reached mid-wait → cancel order, log skip, return
         {"executed": False}.
         If still unfilled at timeout → cancel order, log unfilled row, return
         {"executed": False, "unfilled": True}.

    Balance check: before placing the order, acquires balance_lock, reads
    available_balance (total balance minus resting order value from API), and
    rejects if insufficient funds.

    Args:
        kalshi:        Authenticated AsyncKalshiClient instance.
        writer:        csv.DictWriter for the audit log.
        csv_file:      The open file handle (for flushing).
        csv_lock:      asyncio.Lock protecting all CSV writes.
        balance_lock:  asyncio.Lock protecting the available-balance check + order placement.
        ticker:        Market ticker to trade.
        city:          City key (used for timezone and logging).
        signal_trade:  The trade dict that triggered the signal.
        shutdown_event: Optional asyncio.Event; when set, aborts fill polling.

    Returns:
        {"executed": True}                    on a filled limit order.
        {"executed": False}                   on pre-order errors or stop/shutdown.
        {"executed": False, "unfilled": True} if the limit order expired unfilled.
    """
    yes_price_dollars = signal_trade.get("yes_price", 0) / 100.0
    no_price_raw      = signal_trade.get("no_price", 0) / 100.0

    # ── Build the signal_local string once (used in CSV row) ──────────────────
    try:
        signal_local = pd.Timestamp(signal_trade["created_time"]).tz_convert(STOP_TIMEZONE).isoformat()
    except Exception:
        signal_local = ""

    # ── Common CSV row scaffold ───────────────────────────────────────────────
    def _base_row() -> dict:
        return {
            "logged_at_utc":     datetime.now(tz=timezone.utc).isoformat(),
            "city":              city,
            "ticker":            ticker,
            "signal_yes_price":  yes_price_dollars,
            "signal_no_price":   no_price_raw,
            "signal_trade_id":   signal_trade.get("trade_id", ""),
            "signal_time_utc":   signal_trade.get("created_time", ""),
            "signal_time_local": signal_local,
            "order_id":          "",
            "order_side":        "no",
            "order_limit_price": "",
            "order_status":      "",
            "filled_count":      "",
            "filled_avg_price":  "",
            "estimated_fee":     "",
            "order_type":        "",
            "balance_before":    "",
            "balance_after":     "",
            "skip_reason":       "",
            "notes":             f"signal_trade_count={signal_trade.get('count', '')}",
        }

    # ── Fetch Market object (enables pykalshi tick validation on place_order) ──
    try:
        market_obj = await kalshi.get_market(ticker)
    except Exception as exc:
        logging.error(f"Could not fetch market object for {ticker}: {exc}")
        return {"executed": False}

    # ── Pre-flight: book check ────────────────────────────────────────────────
    # All markets: buy NO at the exact best NO ask price.
    initial_ask = await get_best_no_ask(kalshi, market_obj)
    if initial_ask is None:
        initial_ask = no_price_raw
        logging.debug(
            f"{ticker}: order book unavailable — using trade no_price "
            f"{no_price_raw:.2f} as fallback ask."
        )
    if not _is_no_price_in_range(initial_ask):
        async with csv_lock:
            log_skip(writer, csv_file, ticker, city, signal_trade, "price_drifted_out_of_range")
        return {"executed": False}
    # Cost estimate for the balance check — use the raw ask price directly.
    cost_check_price = initial_ask

    # ── Shared finalize-and-log helper ────────────────────────────────────────
    async def _log_filled(
        order_obj,
        limit_price: float,
        order_type_label: str,
        bal_before: float,
    ) -> None:
        order_id_str  = order_obj.order_id if order_obj else ""
        status_str    = order_obj.status.value if order_obj else "unknown"
        fill_count_fp = (order_obj.fill_count_fp or "0") if order_obj else "0"
        avg_price_str = (order_obj.no_price_dollars or "") if order_obj else ""
        est_fee       = math.ceil(0.0175 * min(yes_price_dollars, 1 - yes_price_dollars) * 100) / 100

        bal_after = ""
        try:
            bal_after = await get_balance_dollars(kalshi)
        except Exception as exc:
            logging.warning(f"Could not fetch post-order balance for {ticker}: {exc}")

        row = _base_row()
        row.update({
            "logged_at_utc":     datetime.now(tz=timezone.utc).isoformat(),
            "order_id":          order_id_str,
            "order_limit_price": limit_price,
            "order_status":      status_str,
            "filled_count":      fill_count_fp,
            "filled_avg_price":  avg_price_str,
            "estimated_fee":     est_fee,
            "order_type":        order_type_label,
            "balance_before":    bal_before,
            "balance_after":     bal_after,
        })
        async with csv_lock:
            log_row(writer, row)
            csv_file.flush()

        logging.info(
            f"ORDER FILLED | {city} | {ticker} | NO @ {limit_price:.2f} | "
            f"order_id={order_id_str} | type={order_type_label} | "
            f"filled={fill_count_fp} | est_fee=${est_fee:.2f}"
        )

    # ── Balance check + order placement inside lock ───────────────────────────
    # The lock is held only through the placement call, NOT through the fill-poll
    # wait. This prevents two coroutines both passing the balance check and then
    # double-spending the same capital.
    _order_result: object | None = None
    _place_error: str | None = None

    async with balance_lock:
        try:
            available = await get_available_balance(kalshi)
        except Exception as exc:
            logging.error(f"Could not fetch available balance for {ticker}: {exc}")
            async with csv_lock:
                log_skip(writer, csv_file, ticker, city, signal_trade, f"balance_fetch_error:{exc}")
            return {"executed": False}

        if cost_check_price * MIN_CONTRACTS > available:
            async with csv_lock:
                log_skip(writer, csv_file, ticker, city, signal_trade, "insufficient_funds")
            logging.warning(
                f"Insufficient funds for {ticker}: cost={cost_check_price:.2f}, "
                f"available={available:.2f}"
            )
            return {"executed": False}

        # Record the balance snapshot while still holding the lock.
        balance = available

        # Re-fetch best NO ask inside the lock to get the freshest price.
        best_ask = await get_best_no_ask(kalshi, market_obj) or no_price_raw
        if not _is_no_price_in_range(best_ask):
            async with csv_lock:
                log_skip(writer, csv_file, ticker, city, signal_trade, "price_drifted_out_of_range")
            return {"executed": False}

        if _is_stop_time_reached():
            async with csv_lock:
                log_skip(writer, csv_file, ticker, city, signal_trade, "stop_time_reached_during_fill_wait")
            return {"executed": False}

        # Place the limit order at exactly the best NO ask — no slippage buffer.
        limit_price = best_ask

        client_oid = f"yb_le_{ticker}_{uuid.uuid4().hex[:12]}"

        logging.info(
            f"LIMIT ENTRY | {city} | {ticker} | "
            f"posting NO @ {limit_price:.2f} (ask={best_ask:.2f})"
        )

        try:
            _order_result = await place_no_order(
                kalshi           = kalshi,
                market           = market_obj,
                count            = MIN_CONTRACTS,
                no_price_dollars = limit_price,
                client_order_id  = client_oid,
            )
        except kalshi_python.exceptions.ApiException as exc:
            # kalshi-python raises ApiException (and sub-classes like
            # BadRequestException, UnauthorizedException, etc.) for HTTP errors.
            status_code = getattr(exc, "status", None)
            body        = getattr(exc, "body", None)
            reason      = getattr(exc, "reason", None)
            logging.error(
                f"{ticker} [limit_entry]: kalshi API error "
                f"status={status_code} reason={reason} body={body}"
            )
            if status_code == 429:
                _place_error = f"rate_limit:{exc}"
            elif status_code == 400:
                # Treat 400 as an order rejection / bad request.
                _place_error = f"order_rejected_400:{exc}"
            else:
                _place_error = f"order_error:{exc}"
        except Exception as exc:
            logging.error(f"{ticker} [limit_entry]: unexpected error placing order: {exc}")
            _place_error = f"order_error:{exc}"

    # Lock released — fill polling happens outside the lock.

    if _place_error is not None:
        async with csv_lock:
            log_skip(writer, csv_file, ticker, city, signal_trade, _place_error)
        return {"executed": False}

    order_placed = _order_result
    logging.info(
        f"ORDER RESTING | {city} | {ticker} | stage=limit_entry | "
        f"NO @ {limit_price:.2f} | order_id={order_placed.order_id}"
    )

    # ── Poll for fill (yields to event loop between polls) ────────────────────
    filled_bool, poll_status, _ = await _poll_for_fill(
        kalshi         = kalshi,
        order_id       = order_placed.order_id,
        wait_seconds   = ORDER_HOLD_SEC,
        stage_label    = "limit_entry",
        ticker         = ticker,
        shutdown_event = shutdown_event,
    )
    try:
        order_final = await get_order_status(kalshi, order_placed.order_id)
    except Exception:
        order_final = order_placed

    if filled_bool:
        await _log_filled(order_final, limit_price, "limit_entry", balance)
        return {"executed": True}

    # Stop time or shutdown reached mid-wait.
    if poll_status in ("stop_time", "shutdown"):
        await _cancel_order_safe(kalshi, order_final.order_id)
        async with csv_lock:
            log_skip(writer, csv_file, ticker, city, signal_trade, "stop_time_reached_during_fill_wait")
        return {"executed": False}

    # Limit order timed out unfilled — cancel and log.
    await _cancel_order_safe(kalshi, order_final.order_id)

    order_id_str  = order_final.order_id if order_final and hasattr(order_final, "order_id") else ""
    status_str    = order_final.status.value if order_final else "unknown"

    row = _base_row()
    row.update({
        "order_id":          order_id_str,
        "order_limit_price": limit_price,
        "order_status":      status_str,
        "filled_count":      "0",
        "filled_avg_price":  "",
        "estimated_fee":     "0",
        "order_type":        "unfilled",
        "balance_before":    balance,
        "balance_after":     balance,
        "skip_reason":       "unfilled_limit_entry",
    })
    async with csv_lock:
        log_row(writer, row)
        csv_file.flush()

    logging.warning(
        f"UNFILLED | {city} | {ticker} | limit entry expired after 1 hour — order cancelled."
    )
    return {"executed": False, "unfilled": True}


# ────────────────────────────────────────────────────────────────────────────────
# Startup Validation
# ────────────────────────────────────────────────────────────────────────────────

async def validate_api_connection(kalshi: AsyncKalshiClient) -> None:
    """Test the API connection. Print balance. Exit on failure.

    Args:
        kalshi: Authenticated AsyncKalshiClient to test.

    Side effects:
        Calls sys.exit(1) on connection failure.
    """
    try:
        balance = await get_balance_dollars(kalshi)
        logging.info(f"API connection OK. Available balance: ${balance:.2f}")
        if balance < 1.0:
            logging.warning(
                f"Balance is ${balance:.2f} — below $1.00. "
                "May be insufficient for any trades."
            )
    except AuthenticationError as exc:
        logging.critical(
            f"API authentication FAILED: {exc}. "
            "Check KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH."
        )
        sys.exit(1)
    except Exception as exc:
        logging.critical(f"API connection FAILED: {exc}")
        sys.exit(1)


# ────────────────────────────────────────────────────────────────────────────────
# Startup Dedup Helper
# ────────────────────────────────────────────────────────────────────────────────

def load_traded_tickers_from_csv(csv_path: Path) -> set[str]:
    """Scan today's CSV rows and return tickers that were already traded.

    Reads today's UTC date from the logged_at_utc column and collects any
    ticker where skip_reason is blank (meaning a real order was placed, not
    a skip). Used at startup to pre-populate traded_tickers so restarts
    mid-day cannot produce duplicate trades.

    Args:
        csv_path: Path to the CSV audit log file.

    Returns:
        Set of ticker strings already traded today (UTC date).
    """
    traded = set()
    today_utc = datetime.now(tz=timezone.utc).date().isoformat()  # e.g. "2026-03-17"
    if not csv_path.exists():
        return traded
    try:
        with open(csv_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # logged_at_utc starts with today's date
                if row.get("logged_at_utc", "").startswith(today_utc):
                    # only count rows where an order was actually placed (skip_reason blank)
                    if not row.get("skip_reason", "").strip() and row.get("ticker", ""):
                        traded.add(row["ticker"])
    except Exception as e:
        logging.warning(f"Could not load prior trades from CSV: {e}")
    return traded


# ────────────────────────────────────────────────────────────────────────────────
# Settlement Logging Helpers
# ────────────────────────────────────────────────────────────────────────────────

def log_filled_orders_to_settlement_csv() -> None:
    """Snapshot today's filled orders into the settlement log at ~17:15 PST.

    Reads the main trade CSV for today's executed orders and appends any not
    already present to settlement_log.csv with settlement_recorded=False.
    All errors are caught and logged — this must never crash the bot.
    """
    try:
        today_utc = datetime.now(tz=timezone.utc).date().isoformat()

        # Collect today's filled orders from the main CSV.
        filled_rows: list[dict] = []
        if CSV_LOG_PATH.exists():
            with open(CSV_LOG_PATH, "r", newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    if (
                        row.get("logged_at_utc", "").startswith(today_utc)
                        and row.get("order_status", "").strip() == "executed"
                        and not row.get("skip_reason", "").strip()
                        and row.get("order_id", "").strip()
                    ):
                        filled_rows.append(row)

        if not filled_rows:
            logging.info("Settlement log snapshot: no executed orders today — nothing to record.")
            return

        # Find order_ids already in the settlement log to avoid duplicates.
        existing_ids: set[str] = set()
        if SETTLEMENT_LOG_PATH.exists():
            try:
                with open(SETTLEMENT_LOG_PATH, "r", newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        existing_ids.add(row.get("order_id", ""))
            except Exception as exc:
                logging.warning(f"Settlement log: could not read existing entries: {exc}")

        new_rows = [
            {
                "date_utc":            today_utc,
                "ticker":              r.get("ticker", ""),
                "city":                r.get("city", ""),
                "order_id":            r.get("order_id", ""),
                "filled_count":        r.get("filled_count", ""),
                "filled_avg_price":    r.get("filled_avg_price", ""),
                "order_type":          r.get("order_type", ""),
                "settlement_recorded": "False",
                "settled_at_utc":      "",
                "market_result":       "",
                "pnl_dollars":         "",
            }
            for r in filled_rows
            if r.get("order_id", "") not in existing_ids
        ]

        if not new_rows:
            logging.info("Settlement log snapshot: all filled orders already present — skipping.")
            return

        write_header = (
            not SETTLEMENT_LOG_PATH.exists()
            or SETTLEMENT_LOG_PATH.stat().st_size == 0
        )
        with open(SETTLEMENT_LOG_PATH, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=SETTLEMENT_COLUMNS)
            if write_header:
                w.writeheader()
            w.writerows(new_rows)

        logging.info(
            f"Settlement log snapshot: wrote {len(new_rows)} order(s) to {SETTLEMENT_LOG_PATH}"
        )

    except Exception as exc:
        logging.warning(f"Settlement log snapshot failed (non-fatal): {exc}")


async def check_and_record_settlements(kalshi: AsyncKalshiClient) -> None:
    """At bot startup, check the settlement log for unrecorded settled contracts.

    For each row where settlement_recorded==False, fetches the market from Kalshi.
    If the market has settled (has a result), records the result and PnL and
    marks settlement_recorded=True. Continues the algorithm regardless of errors.

    Args:
        kalshi: Authenticated AsyncKalshiClient instance.
    """
    if not SETTLEMENT_LOG_PATH.exists():
        return

    try:
        with open(SETTLEMENT_LOG_PATH, "r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except Exception as exc:
        logging.warning(f"Settlement check: could not read log (non-fatal): {exc}")
        return

    if not rows:
        return

    pending = [r for r in rows if r.get("settlement_recorded", "").strip().lower() != "true"]
    if not pending:
        logging.info("Settlement check: all prior orders already recorded — nothing to do.")
        return

    logging.info(f"Settlement check: {len(pending)} unrecorded order(s) to check.")
    updated = False

    for row in rows:
        if row.get("settlement_recorded", "").strip().lower() == "true":
            continue

        ticker = row.get("ticker", "").strip()
        if not ticker:
            continue

        try:
            market = await kalshi.get_market(ticker)
            status_str = str(market.status).lower() if market.status else ""
            result_str = str(market.result).lower() if getattr(market, "result", None) else ""

            # A market has settled when it has a result (yes/no) and is no longer open.
            if result_str in ("yes", "no") and "open" not in status_str:
                try:
                    filled_count = float(row.get("filled_count", "0") or "0")
                    avg_price    = float(row.get("filled_avg_price", "0") or "0")
                    # All positions are bought NO; "no" result = win, "yes" result = loss.
                    pnl = (
                        round((1.0 - avg_price) * filled_count, 4)
                        if result_str == "no"
                        else round(-avg_price * filled_count, 4)
                    )
                    pnl_str = str(pnl)
                except Exception:
                    pnl_str = ""

                row["settlement_recorded"] = "True"
                row["settled_at_utc"]      = datetime.now(tz=timezone.utc).isoformat()
                row["market_result"]       = result_str
                row["pnl_dollars"]         = pnl_str
                updated = True

                logging.info(
                    f"Settlement recorded | {row.get('city', '')} | {ticker} | "
                    f"result={result_str} | pnl={pnl_str}"
                )
            else:
                logging.info(
                    f"Settlement check | {ticker} | not yet settled "
                    f"(status={status_str}, result={result_str!r}) — will check again next startup."
                )

        except Exception as exc:
            logging.warning(f"Settlement check: could not check {ticker} (non-fatal): {exc}")
            continue

    if updated:
        try:
            with open(SETTLEMENT_LOG_PATH, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=SETTLEMENT_COLUMNS)
                w.writeheader()
                w.writerows(rows)
            logging.info("Settlement log updated with resolved contract(s).")
        except Exception as exc:
            logging.warning(f"Settlement log write failed (non-fatal): {exc}")


# ────────────────────────────────────────────────────────────────────────────────
# Per-ticker async coroutine
# ────────────────────────────────────────────────────────────────────────────────

async def poll_ticker_loop(
    kalshi: AsyncKalshiClient,
    ticker: str,
    city: str,
    writer: csv.DictWriter,
    csv_file,
    csv_lock: asyncio.Lock,
    balance_lock: asyncio.Lock,
    traded_tickers: set[str],
    traded_tickers_lock: asyncio.Lock,
    last_trade_ts: dict[str, str],
    last_trade_ts_lock: asyncio.Lock,
    unfilled_count_ref: list[int],
    unfilled_count_lock: asyncio.Lock,
    shutdown_event: asyncio.Event,
) -> None:
    """Independent polling coroutine for a single market ticker.

    Runs in parallel with all other ticker coroutines. When a maker wait is
    active (asyncio.sleep inside _poll_for_fill), this coroutine yields control
    back to the event loop so other tickers can continue polling uninterrupted.

    Args:
        kalshi:               Shared AsyncKalshiClient instance.
        ticker:               Market ticker to watch.
        city:                 City key for logging.
        writer:               csv.DictWriter protected by csv_lock.
        csv_file:             Open file handle (for flushing).
        csv_lock:             asyncio.Lock for CSV writes.
        balance_lock:         asyncio.Lock for balance check + order placement.
        traded_tickers:       Shared set of tickers already traded today.
        traded_tickers_lock:  asyncio.Lock for traded_tickers mutations.
        last_trade_ts:        Shared dict of ISO high-water marks per ticker.
        last_trade_ts_lock:   asyncio.Lock for last_trade_ts mutations.
        unfilled_count_ref:   Mutable single-element list holding the unfilled counter.
        unfilled_count_lock:  asyncio.Lock for unfilled_count_ref.
        shutdown_event:       asyncio.Event set when the bot should stop.
    """
    logging.debug(f"Ticker coroutine started: {city}/{ticker}")

    _near_settled = False  # True once yes_ask >= 0.99 for this ticker

    while not shutdown_event.is_set():
        try:
            # ── Already traded this ticker today? ─────────────────────────────
            async with traded_tickers_lock:
                already_traded = ticker in traded_tickers
            if already_traded:
                logging.debug(f"{city}/{ticker}: already traded today — coroutine exiting.")
                return

            # ── Hard stop check (no new entries) ──────────────────────────────
            trading_allowed = not _is_stop_time_reached()

            # ── Fetch incremental trades since last poll ───────────────────────
            async with last_trade_ts_lock:
                min_ts = last_trade_ts.get(ticker)

            try:
                trades = await get_recent_trades(kalshi, ticker, min_ts_iso=min_ts)
            except Exception as exc:
                logging.error(f"{city}/{ticker}: trade fetch error: {exc}")
                await asyncio.sleep(POLL_INTERVAL_SEC)
                continue

            if trades:
                # Advance high-water mark to the latest trade we saw.
                latest_ts = max(t.get("created_time", "") for t in trades)
                if latest_ts:
                    async with last_trade_ts_lock:
                        last_trade_ts[ticker] = latest_ts
                logging.debug(
                    f"{city}/{ticker}: {len(trades)} new trade(s) fetched."
                )

                # ── Near-settled throttle: check most recent trade's yes_price ──
                # yes_price is in cents (int); >= 99 means YES is trading at $0.99+
                if not _near_settled:
                    most_recent_trade = max(trades, key=lambda t: t.get("created_time", ""))
                    if most_recent_trade.get("yes_price", 0) >= 99:
                        _near_settled = True
                        logging.debug(
                            f"{city}/{ticker}: yes_price={most_recent_trade.get('yes_price')} >= 99 — "
                            "throttling to 60s poll interval."
                        )

            # Process trades chronologically (oldest first) to match
            # backtest deduplication: earliest qualifying trade wins.
            for trade in sorted(trades, key=lambda t: t.get("created_time", "")):
                if not is_qualifying_signal(trade):
                    continue

                # Signal fired — log it.
                yes_p = trade.get("yes_price", 0) / 100.0
                pst_t = pd.Timestamp(
                    trade["created_time"]
                ).tz_convert(STOP_TIMEZONE).strftime("%H:%M:%S")
                logging.info(
                    f"SIGNAL | {city} | {ticker} | "
                    f"yes={yes_p:.2f} | pst={pst_t} | "
                    f"taker={trade.get('taker_side','')}"
                )

                if not trading_allowed:
                    async with csv_lock:
                        log_skip(
                            writer, csv_file,
                            ticker, city, trade,
                            "past_stop_time",
                        )
                    break   # one signal per ticker per scan

                result = await attempt_entry(
                    kalshi, writer, csv_file,
                    csv_lock, balance_lock,
                    ticker, city, trade,
                    shutdown_event,
                )
                if result["executed"]:
                    async with traded_tickers_lock:
                        traded_tickers.add(ticker)
                    # Ticker is now done for the day — exit the coroutine.
                    logging.debug(f"{city}/{ticker}: trade executed, coroutine exiting.")
                    return
                elif result.get("unfilled"):
                    async with unfilled_count_lock:
                        unfilled_count_ref[0] += 1
                    logging.warning(
                        f"Unfilled order count today: {unfilled_count_ref[0]} "
                        f"(latest: {city}/{ticker})"
                    )

                # Regardless of success/failure, do not fire on this ticker
                # again this scan cycle (one signal per ticker per poll).
                break

        except asyncio.CancelledError:
            logging.debug(f"{city}/{ticker}: coroutine cancelled.")
            return
        except Exception as exc:
            logging.error(
                f"{city}/{ticker}: unexpected error in poll loop (will retry): {exc}",
                exc_info=True,
            )

        # Yield to the event loop before the next poll cycle.
        # Use a 60s interval once the market is near-settled (yes_ask >= 0.99).
        await asyncio.sleep(60 if _near_settled else POLL_INTERVAL_SEC)


# ────────────────────────────────────────────────────────────────────────────────
# Async Main
# ────────────────────────────────────────────────────────────────────────────────

async def async_main() -> None:
    """Async entry point. Spawns one coroutine per market ticker, all running
    in parallel. Maker waits inside each coroutine use asyncio.sleep so every
    other coroutine continues polling uninterrupted."""

    # ── Pre-flight checks ────────────────────────────────────────────────────
    if not KALSHI_API_KEY_ID:
        print(
            "ERROR: KALSHI_API_KEY_ID environment variable is not set.\n"
            "Export it before running:\n"
            "  export KALSHI_API_KEY_ID='your-api-key-id'",
            file=sys.stderr,
        )
        sys.exit(1)

    if not KALSHI_PRIVATE_KEY_PATH:
        print(
            "ERROR: KALSHI_PRIVATE_KEY_PATH environment variable is not set.\n"
            "Export it before running:\n"
            "  export KALSHI_PRIVATE_KEY_PATH='/path/to/private-key.key'",
            file=sys.stderr,
        )
        sys.exit(1)

    setup_logging()
    logging.info("=" * 70)
    logging.info("YES Bias Fade — Live Deployment Trial starting (async mode).")
    logging.info(f"  CSV log : {CSV_LOG_PATH}")
    logging.info(f"  Bot log : {LOG_FILE}")
    logging.info(f"  Poll    : every {POLL_INTERVAL_SEC}s per ticker (parallel)")
    logging.info(f"  Stop    : no new trades at/after {STOP_HOUR}:00 {STOP_TIMEZONE.key}")
    logging.info(f"  Rate    : {API_REQUESTS_PER_SECOND} req/s shared across all coroutines")
    logging.info("=" * 70)

    # ── 4am PST start guard — wait if running before market open ─────────────
    now_pst = datetime.now(tz=STOP_TIMEZONE)
    if now_pst.hour < START_HOUR:
        next_start = now_pst.replace(hour=START_HOUR, minute=0, second=0, microsecond=0)
        wait_sec = (next_start - now_pst).total_seconds()
        logging.info(
            f"Before market open. Waiting {wait_sec / 60:.1f} min until "
            f"{START_HOUR}:00 {STOP_TIMEZONE.key}..."
        )
        await asyncio.sleep(wait_sec)

    # ── Shared rate limiter (one instance, shared across all coroutines) ──────
    rate_limiter = AsyncRateLimiter(requests_per_second=API_REQUESTS_PER_SECOND)

    # ── Build async pykalshi client ───────────────────────────────────────────
    try:
        kalshi = AsyncKalshiClient(
            api_key_id       = KALSHI_API_KEY_ID,
            private_key_path = KALSHI_PRIVATE_KEY_PATH,
            rate_limiter     = rate_limiter,
        )
    except Exception as exc:
        logging.critical(f"Failed to instantiate AsyncKalshiClient: {exc}")
        sys.exit(1)

    # ── Build kalshi-python client for order placement ────────────────────────
    # kalshi-python is the official Kalshi OpenAPI client (synchronous).
    # It handles RSA-PSS auth natively via KalshiAuth inside ApiClient.
    global _kp_portfolio
    try:
        _kp_api_client = kalshi_python.ApiClient(
            configuration=kalshi_python.Configuration(
                host="https://api.elections.kalshi.com/trade-api/v2"
            )
        )
        _kp_api_client.set_kalshi_auth(
            key_id           = KALSHI_API_KEY_ID,
            private_key_path = KALSHI_PRIVATE_KEY_PATH,
        )
        _kp_portfolio = KalshiPortfolioApi(_kp_api_client)
        logging.info("kalshi-python PortfolioApi initialised for order placement.")
    except Exception as exc:
        logging.critical(f"Failed to initialise kalshi-python client: {exc}")
        sys.exit(1)

    async with kalshi:
        await validate_api_connection(kalshi)

        # ── Morning settlement check ─────────────────────────────────────────
        await check_and_record_settlements(kalshi)

        # ── Per-day state ────────────────────────────────────────────────────
        traded_tickers: set[str]       = load_traded_tickers_from_csv(CSV_LOG_PATH)
        if traded_tickers:
            logging.info(
                f"Restored {len(traded_tickers)} already-traded tickers from today's CSV: "
                f"{traded_tickers}"
            )
        last_trade_ts:  dict[str, str] = {}
        unfilled_count_ref: list[int]  = [0]  # mutable container for counter
        settlement_logged_today: bool  = False
        trading_day = datetime.now(tz=timezone.utc).date()

        # ── Async-safe locks ─────────────────────────────────────────────────
        csv_lock             = asyncio.Lock()
        balance_lock         = asyncio.Lock()
        traded_tickers_lock  = asyncio.Lock()
        last_trade_ts_lock   = asyncio.Lock()
        unfilled_count_lock  = asyncio.Lock()

        # ── Shutdown event — set to stop all ticker coroutines gracefully ─────
        shutdown_event = asyncio.Event()

        # ── Open CSV for append ──────────────────────────────────────────────
        csv_file_handle = open(CSV_LOG_PATH, "a", newline="", encoding="utf-8")
        writer = csv.DictWriter(csv_file_handle, fieldnames=CSV_COLUMNS)

        # Write header only if the file is brand new (tell() == 0 after open("a")).
        if csv_file_handle.tell() == 0:
            writer.writeheader()
            csv_file_handle.flush()

        # ── Order book collector — runs concurrently, isolated from trading ────
        _ob_output_dir = Path(__file__).parent / "orderbook_data"
        _ob_collector  = OrderBookCollector(kalshi, output_dir=_ob_output_dir)

        async def _run_collector_safe() -> None:
            """Wrap collector so any crash cannot propagate to the trading loop."""
            try:
                await _ob_collector.start()
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logging.error(
                    f"OrderBookCollector crashed (trading continues): {exc}",
                    exc_info=True,
                )

        _ob_task = asyncio.create_task(_run_collector_safe(), name="orderbook_collector")
        logging.info(f"OrderBookCollector task started. Output dir: {_ob_output_dir}")

        try:
            while not shutdown_event.is_set():
                now_utc = datetime.now(tz=timezone.utc)
                now_pst = datetime.now(tz=STOP_TIMEZONE)

                # ── Midnight UTC reset ─────────────────────────────────────────
                today = datetime.now(tz=timezone.utc).date()
                if today != trading_day:
                    logging.info(
                        f"New trading day ({today}) — resetting traded_tickers "
                        f"({len(traded_tickers)} cleared), last_trade_ts, and unfilled_count "
                        f"(was {unfilled_count_ref[0]})."
                    )
                    async with traded_tickers_lock:
                        traded_tickers.clear()
                    async with last_trade_ts_lock:
                        last_trade_ts.clear()
                    async with unfilled_count_lock:
                        unfilled_count_ref[0] = 0
                    settlement_logged_today = False
                    trading_day = today

                # ── Settlement log snapshot at 17:15 PST ──────────────────────
                if (
                    now_pst.hour == STOP_HOUR
                    and now_pst.minute >= SETTLEMENT_LOG_MINUTE
                    and not settlement_logged_today
                ):
                    log_filled_orders_to_settlement_csv()
                    settlement_logged_today = True

                # ── Graceful shutdown at 17:30 PST ────────────────────────────
                if now_pst.hour > STOP_HOUR or (
                    now_pst.hour == STOP_HOUR and now_pst.minute >= SHUTDOWN_MINUTE
                ):
                    async with traded_tickers_lock:
                        traded_set_snapshot = sorted(traded_tickers)
                    logging.info(
                        f"Shutdown time {STOP_HOUR}:{SHUTDOWN_MINUTE:02d} {STOP_TIMEZONE.key} "
                        f"reached ({now_pst.strftime('%H:%M')}) — signalling shutdown. "
                        f"Traded tickers today: {traded_set_snapshot}. "
                        f"Unfilled orders today: {unfilled_count_ref[0]}."
                    )
                    shutdown_event.set()
                    break

                # ── Hard stop check ───────────────────────────────────────────
                trading_allowed = now_pst.hour < STOP_HOUR
                if not trading_allowed:
                    async with unfilled_count_lock:
                        uc = unfilled_count_ref[0]
                    unfilled_msg = f" Unfilled orders today: {uc}." if uc > 0 else ""
                    logging.info(
                        f"Hard stop active: {now_pst.strftime('%H:%M')} "
                        f"{STOP_TIMEZONE.key} >= {STOP_HOUR}:00. "
                        f"Monitoring only — no new entries.{unfilled_msg}"
                    )

                # ── Discover today's markets and spawn per-ticker coroutines ──
                # Coroutines for tickers already in traded_tickers are skipped
                # (they would exit immediately, but we avoid the overhead).
                logging.debug(
                    f"Supervisor cycle | UTC={now_utc.strftime('%H:%M:%S')} | "
                    f"PST={now_pst.strftime('%H:%M:%S')}"
                )

                new_tasks: list[asyncio.Task] = []

                for city, cfg in CITIES.items():
                    prefix    = cfg["prefix"]

                    # Fetch today's active markets for this city.
                    try:
                        markets = await get_markets_for_series(kalshi, prefix)
                    except Exception as exc:
                        logging.error(f"{city}: failed to fetch markets: {exc}")
                        continue

                    today_markets = filter_todays_markets(markets)
                    if not today_markets:
                        logging.debug(f"{city}: no markets expiring today.")
                        continue

                    logging.debug(
                        f"{city}: {len(today_markets)} market(s) found for today."
                    )

                    for market in today_markets:
                        ticker = market.ticker
                        if not ticker:
                            continue

                        # Skip tickers already being handled by a live coroutine.
                        # We track this via a module-level set that each coroutine
                        # adds itself to on start and removes on exit.
                        async with traded_tickers_lock:
                            already_traded = ticker in traded_tickers
                        if already_traded:
                            continue

                        # Skip tickers that already have a running coroutine.
                        # active_tickers tracks tickers with live poll_ticker_loop tasks.
                        if ticker in _active_tickers:
                            continue

                        # Spawn a new per-ticker coroutine.
                        _active_tickers.add(ticker)
                        task = asyncio.create_task(
                            _ticker_task_wrapper(
                                kalshi, ticker, city,
                                writer, csv_file_handle,
                                csv_lock, balance_lock,
                                traded_tickers, traded_tickers_lock,
                                last_trade_ts, last_trade_ts_lock,
                                unfilled_count_ref, unfilled_count_lock,
                                shutdown_event,
                            ),
                            name=f"ticker_{city}_{ticker}",
                        )
                        new_tasks.append(task)

                if new_tasks:
                    logging.debug(f"Spawned {len(new_tasks)} new ticker coroutine(s).")

                # Supervisor sleep — wake up periodically to discover new markets
                # and check shutdown / settlement conditions. Individual ticker
                # coroutines poll on their own POLL_INTERVAL_SEC schedule.
                await asyncio.sleep(60)

        except KeyboardInterrupt:
            logging.info("KeyboardInterrupt received. Shutting down gracefully.")
            shutdown_event.set()
        finally:
            # ── Stop the order book collector first so it can flush its buffers ─
            await _ob_collector.stop()
            if not _ob_task.done():
                _ob_task.cancel()
                try:
                    await _ob_task
                except (asyncio.CancelledError, Exception):
                    pass

            # Cancel all running ticker tasks and wait for them to exit.
            ticker_tasks = [t for t in asyncio.all_tasks() if t.get_name().startswith("ticker_")]
            if ticker_tasks:
                logging.info(f"Cancelling {len(ticker_tasks)} active ticker tasks...")
                for t in ticker_tasks:
                    t.cancel()
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*ticker_tasks, return_exceptions=True),
                        timeout=5.0,
                    )
                except asyncio.TimeoutError:
                    logging.warning("Ticker tasks did not finish within 5s timeout.")

            # Cancel all resting orders on Kalshi before exit.
            try:
                logging.info("Cancelling all resting orders on Kalshi...")
                resting = await kalshi.portfolio.get_orders(status="resting", limit=100)
                orders = getattr(resting, "orders", resting) if not isinstance(resting, list) else resting
                for order in orders:
                    oid = getattr(order, "order_id", None) or order.get("order_id")
                    if oid:
                        try:
                            await kalshi.portfolio.cancel_order(oid)
                            logging.info(f"Cancelled resting order {oid}")
                        except Exception as ce:
                            logging.warning(f"Could not cancel order {oid}: {ce}")
            except Exception as exc:
                logging.warning(f"Resting order cancellation failed: {exc}")

            # Safety net: if the settlement snapshot was never written, do it now.
            if not settlement_logged_today:
                log_filled_orders_to_settlement_csv()

            async with traded_tickers_lock:
                traded_set_final = sorted(traded_tickers)
            logging.info(
                f"Shutdown complete. Traded tickers today: {traded_set_final}"
            )
            logging.info(f"Unfilled orders today: {unfilled_count_ref[0]}")
            csv_file_handle.flush()
            csv_file_handle.close()
            logging.info(f"CSV log closed: {CSV_LOG_PATH}")


# ────────────────────────────────────────────────────────────────────────────────
# Active ticker tracking set (module-level, event-loop scoped)
# ────────────────────────────────────────────────────────────────────────────────

# Tracks tickers that currently have a live poll_ticker_loop coroutine running.
# Prevents the supervisor from spawning duplicate coroutines for the same ticker.
# Modified only from within the event loop — no lock needed (single-threaded asyncio).
_active_tickers: set[str] = set()

# kalshi-python PortfolioApi instance — initialised in async_main() and used
# exclusively by place_no_order() for order submission.
_kp_portfolio: KalshiPortfolioApi | None = None


async def _ticker_task_wrapper(
    kalshi: AsyncKalshiClient,
    ticker: str,
    city: str,
    writer: csv.DictWriter,
    csv_file,
    csv_lock: asyncio.Lock,
    balance_lock: asyncio.Lock,
    traded_tickers: set[str],
    traded_tickers_lock: asyncio.Lock,
    last_trade_ts: dict[str, str],
    last_trade_ts_lock: asyncio.Lock,
    unfilled_count_ref: list[int],
    unfilled_count_lock: asyncio.Lock,
    shutdown_event: asyncio.Event,
) -> None:
    """Thin wrapper around poll_ticker_loop that removes the ticker from
    _active_tickers when the coroutine exits for any reason."""
    try:
        await poll_ticker_loop(
            kalshi, ticker, city,
            writer, csv_file,
            csv_lock, balance_lock,
            traded_tickers, traded_tickers_lock,
            last_trade_ts, last_trade_ts_lock,
            unfilled_count_ref, unfilled_count_lock,
            shutdown_event,
        )
    finally:
        _active_tickers.discard(ticker)
        logging.debug(f"Ticker coroutine exited and deregistered: {city}/{ticker}")


# ────────────────────────────────────────────────────────────────────────────────
# Entry point
# ────────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """Entry point. Runs the async event loop."""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
