"""Kalshi order book data collector.

Polls all today's temperature markets (4 cities x up to 6 brackets) every
5 seconds and writes order book snapshots to per-market CSV files.

Designed to run as a concurrent asyncio task alongside the YES Bias Fade
live trading bot.  It shares the same AsyncKalshiClient and therefore the
same rate-limiter budget.

File layout
-----------
    orderbook_data/
        {YYYY-MM-DD}/
            {city}/
                orderbook_{city}_{strike_temp}.csv   -- one per market
        errors.log

Usage (standalone)
------------------
    python orderbook_collector.py

Usage (integrated)
------------------
    from orderbook_collector import OrderBookCollector
    collector = OrderBookCollector(kalshi_client, output_dir=Path("orderbook_data"))
    task = asyncio.create_task(collector.start())
    ...
    await collector.stop()
"""

from __future__ import annotations

import asyncio
import csv
import logging
import math
import os
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

# ── Logging ──────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)


# ── Constants ────────────────────────────────────────────────────────────────

POLL_INTERVAL_SEC: float = 5.0        # target start-to-start interval
BUFFER_FLUSH_ROWS: int   = 12         # flush every N rows per file
BUFFER_FLUSH_SEC: float  = 60.0       # flush every N seconds regardless
STATUS_PRINT_SEC: float  = 300.0      # console status every 5 minutes
RETRY_DELAY_SEC: float   = 2.0        # retry after one failure
RATE_LIMIT_BACKOFF_SEC: float = 10.0  # back off on HTTP 429
REQUEST_SPACING_SEC: float = 0.35     # ~2.9 req/s ceiling within each cycle

# City definitions — mirrors the CITIES dict in yes_bias_live_deployment_trial.py.
# The collector only needs the series prefix; timezone is unused here.
CITIES: dict[str, str] = {
    "LAX": "KXHIGHLAX",
    "NY":  "KXHIGHNY",
    "MIA": "KXHIGHMIA",
    "DEN": "KXHIGHDEN",
}

# CSV column order (22 columns as specified)
CSV_COLUMNS: list[str] = [
    "timestamp",
    "market_id",
    "city",
    "strike_temp",
    "time_to_close",
    "best_bid",
    "bid_qty",
    "best_ask",
    "ask_qty",
    "bid_price_2",
    "bid_qty_2",
    "bid_price_3",
    "bid_qty_3",
    "ask_price_2",
    "ask_qty_2",
    "ask_price_3",
    "ask_qty_3",
    "last_trade_price",
    "volume_24h",
    "spread",
    "mid_price",
    "imbalance",
]

# Regex to extract the strike temperature from a ticker like:
#   KXHIGHLAX-26MAR16-B72.5   -> strike = "72.5"
#   KXHIGHNY-26MAR16-T85      -> strike = "85"
# Kalshi uses B (below) / T (at-or-above) prefixes; we just want the number.
_STRIKE_RE = re.compile(r"-[BT]([\d.]+)$", re.IGNORECASE)

# Month abbreviation table (matches Kalshi ticker format)
_MONTH_ABBR = ["JAN","FEB","MAR","APR","MAY","JUN",
               "JUL","AUG","SEP","OCT","NOV","DEC"]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _utc_today_kalshi_str() -> str:
    """Return today's date in Kalshi ticker format, e.g. '26MAR17'."""
    now = datetime.now(tz=timezone.utc)
    return f"{now.strftime('%y')}{_MONTH_ABBR[now.month - 1]}{now.strftime('%d')}"


def _extract_strike(ticker: str) -> str:
    """Extract the strike temperature string from a Kalshi ticker, or 'UNK'."""
    m = _STRIKE_RE.search(ticker)
    return m.group(1) if m else "UNK"


def _extract_city(ticker: str, cities: dict[str, str]) -> str:
    """Reverse-lookup city key from ticker prefix."""
    for city, prefix in cities.items():
        if ticker.startswith(prefix):
            return city
    return "UNK"


def _seconds_to_close(close_time_iso: str | None) -> float | None:
    """Return seconds until market close, or None if close_time is unavailable."""
    if not close_time_iso:
        return None
    try:
        close_dt = datetime.fromisoformat(close_time_iso.replace("Z", "+00:00"))
        if close_dt.tzinfo is None:
            close_dt = close_dt.replace(tzinfo=timezone.utc)
        delta = (close_dt - datetime.now(tz=timezone.utc)).total_seconds()
        return delta
    except Exception:
        return None


def _dollars_to_cents(val: str | None) -> float | None:
    """Convert a dollar string (e.g. '0.45') to cents (e.g. 45.0), or None."""
    if val is None:
        return None
    try:
        return float(Decimal(val) * 100)
    except Exception:
        return None


def _fp_to_float(val: str | int | None) -> float | None:
    """Convert a fixed-point string or int to float, or None."""
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None


def _build_empty_row(
    ticker: str,
    city: str,
    strike: str,
    time_to_close: float | None,
) -> dict[str, Any]:
    """Build a row with NaN prices and 0 quantities (empty book snapshot)."""
    return {
        "timestamp":       datetime.now(tz=timezone.utc).isoformat(timespec="milliseconds"),
        "market_id":       ticker,
        "city":            city,
        "strike_temp":     strike,
        "time_to_close":   time_to_close if time_to_close is not None else "",
        "best_bid":        "",
        "bid_qty":         0,
        "best_ask":        "",
        "ask_qty":         0,
        "bid_price_2":     "",
        "bid_qty_2":       0,
        "bid_price_3":     "",
        "bid_qty_3":       0,
        "ask_price_2":     "",
        "ask_qty_2":       0,
        "ask_price_3":     "",
        "ask_qty_3":       0,
        "last_trade_price": "",
        "volume_24h":      "",
        "spread":          "",
        "mid_price":       "",
        "imbalance":       "",
    }


def _build_row(
    ticker: str,
    city: str,
    strike: str,
    close_time_iso: str | None,
    book_response: Any,   # OrderbookResponse from pykalshi
    market_obj: Any,      # AsyncMarket object — carries last_price, volume_24h
) -> dict[str, Any]:
    """Build a full snapshot row from live API data.

    Prices in the spec are in cents (0-100 scale), so we multiply dollar
    strings by 100.  Quantities are in whole contracts (fixed-point strings
    are floored to int for storage, matching the spec).

    YES bid levels come from orderbook.yes_dollars (sorted best-first).
    YES ask levels come from orderbook.no_dollars: the best NO bid price p
    implies a YES ask of (1 - p), so we sort NO bids descending and map
    each entry to its implied YES ask price.
    """
    now_iso = datetime.now(tz=timezone.utc).isoformat(timespec="milliseconds")
    ttc     = _seconds_to_close(close_time_iso)

    # ── YES bid levels (sorted descending by price = best bid first) ──────────
    yes_levels: list[tuple[str, str | int]] = book_response.orderbook.yes_dollars or []
    yes_sorted = sorted(yes_levels, key=lambda x: Decimal(x[0]), reverse=True)

    def _bid(i: int) -> tuple[float, float]:
        """Return (price_cents, qty_float) for bid level i (0-indexed), or (NaN, 0)."""
        if i < len(yes_sorted):
            p, q = yes_sorted[i]
            return (_dollars_to_cents(p) or float("nan"), _fp_to_float(q) or 0.0)
        return float("nan"), 0.0

    bid0_p, bid0_q = _bid(0)
    bid1_p, bid1_q = _bid(1)
    bid2_p, bid2_q = _bid(2)

    # ── YES ask levels — derived from NO bid levels ────────────────────────────
    # Best YES ask = 1 - best NO bid.  Sort NO bids descending (best = highest
    # NO bid = lowest YES ask = best for a YES buyer).
    no_levels: list[tuple[str, str | int]] = book_response.orderbook.no_dollars or []
    no_sorted = sorted(no_levels, key=lambda x: Decimal(x[0]), reverse=True)

    def _ask(i: int) -> tuple[float, float]:
        """Return (yes_ask_cents, qty_float) for ask level i, or (NaN, 0)."""
        if i < len(no_sorted):
            no_p, no_q = no_sorted[i]
            yes_ask_cents = float((Decimal("1") - Decimal(no_p)) * 100)
            return (yes_ask_cents, _fp_to_float(no_q) or 0.0)
        return float("nan"), 0.0

    ask0_p, ask0_q = _ask(0)
    ask1_p, ask1_q = _ask(1)
    ask2_p, ask2_q = _ask(2)

    # ── Derived fields ─────────────────────────────────────────────────────────
    bid_valid = not math.isnan(bid0_p)
    ask_valid = not math.isnan(ask0_p)

    spread   = (ask0_p - bid0_p) if (bid_valid and ask_valid) else float("nan")
    mid      = ((bid0_p + ask0_p) / 2) if (bid_valid and ask_valid) else float("nan")
    imbal    = (bid0_q / (bid0_q + ask0_q)) if (bid0_q + ask0_q > 0) else float("nan")

    # ── Market-level fields from the AsyncMarket object ────────────────────────
    last_price_cents: float = float("nan")
    volume_24h: float = float("nan")

    try:
        lp = getattr(market_obj, "last_price_dollars", None)
        if lp is not None:
            last_price_cents = float(Decimal(lp) * 100)
    except Exception:
        pass

    try:
        v24 = getattr(market_obj, "volume_24h_fp", None)
        if v24 is not None:
            volume_24h = float(v24)
    except Exception:
        pass

    def _csv_float(v: float) -> float | str:
        """Return v as-is unless it is NaN, in which case return empty string."""
        return "" if math.isnan(v) else v

    return {
        "timestamp":        now_iso,
        "market_id":        ticker,
        "city":             city,
        "strike_temp":      strike,
        "time_to_close":    ttc if ttc is not None else "",
        "best_bid":         _csv_float(bid0_p),
        "bid_qty":          bid0_q,
        "best_ask":         _csv_float(ask0_p),
        "ask_qty":          ask0_q,
        "bid_price_2":      _csv_float(bid1_p),
        "bid_qty_2":        bid1_q,
        "bid_price_3":      _csv_float(bid2_p),
        "bid_qty_3":        bid2_q,
        "ask_price_2":      _csv_float(ask1_p),
        "ask_qty_2":        ask1_q,
        "ask_price_3":      _csv_float(ask2_p),
        "ask_qty_3":        ask2_q,
        "last_trade_price": _csv_float(last_price_cents),
        "volume_24h":       _csv_float(volume_24h),
        "spread":           _csv_float(spread),
        "mid_price":        _csv_float(mid),
        "imbalance":        _csv_float(imbal),
    }


# ── Per-file writer with buffering ───────────────────────────────────────────

class _MarketWriter:
    """Manages a single CSV file for one market, with row-count and time buffering."""

    def __init__(self, path: Path) -> None:
        self._path       = path
        self._rows_since_flush: int   = 0
        self._last_flush_ts: float    = 0.0
        self._file       = None
        self._writer     = None
        self._open()

    def _open(self) -> None:
        """Open (or re-open) the CSV file in append mode."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        needs_header = not self._path.exists() or self._path.stat().st_size == 0
        self._file   = open(self._path, "a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=CSV_COLUMNS)
        if needs_header:
            self._writer.writeheader()
            self._file.flush()

    def write(self, row: dict[str, Any]) -> None:
        """Append a row, flushing if either threshold is reached."""
        self._writer.writerow(row)
        self._rows_since_flush += 1
        now = _monotonic()
        time_elapsed = now - self._last_flush_ts
        if (
            self._rows_since_flush >= BUFFER_FLUSH_ROWS
            or time_elapsed >= BUFFER_FLUSH_SEC
        ):
            self._file.flush()
            self._rows_since_flush = 0
            self._last_flush_ts = now

    def close(self) -> None:
        """Flush remaining buffered data and close the file."""
        if self._file and not self._file.closed:
            try:
                self._file.flush()
                self._file.close()
            except Exception as exc:
                logger.warning("Error closing CSV %s: %s", self._path, exc)

    def flush_and_close(self) -> None:
        """Alias for close() — flush and close the file."""
        self.close()


def _monotonic() -> float:
    """Return a monotonic clock value in seconds."""
    import time
    return time.monotonic()


# ── Main collector class ─────────────────────────────────────────────────────

class OrderBookCollector:
    """Polls all today's temperature markets and writes order book CSV snapshots.

    Args:
        kalshi:     Authenticated AsyncKalshiClient instance (shared with the bot).
        output_dir: Directory for CSV output files and errors.log.
                    Created automatically if it does not exist.

    Example::

        collector = OrderBookCollector(kalshi, output_dir=Path("orderbook_data"))
        task = asyncio.create_task(collector.start())
        # ... later ...
        await collector.stop()
    """

    def __init__(
        self,
        kalshi: Any,
        output_dir: Path = Path("orderbook_data"),
    ) -> None:
        self._kalshi     = kalshi
        self._output_dir = output_dir
        self._stop_event = asyncio.Event()

        # Per-market CSV writers keyed by (ticker, date_str)
        self._writers: dict[tuple[str, str], _MarketWriter] = {}

        # Error log file handle
        self._error_log: Any = None

        # Metrics for the 5-minute status print
        self._snapshots_total:  int = 0
        self._errors_since_status: int = 0
        self._last_status_ts: float = 0.0

    # ── Public interface ─────────────────────────────────────────────────────

    async def start(self) -> None:
        """Run the collection loop until stop() is called.

        This coroutine runs indefinitely; cancel it or call stop() to exit.
        """
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._error_log = open(
            self._output_dir / "errors.log", "a", encoding="utf-8"
        )
        logger.info(
            "OrderBookCollector started. Output: %s, interval: %ss",
            self._output_dir, POLL_INTERVAL_SEC,
        )
        self._last_status_ts = _monotonic()

        try:
            await self._run_loop()
        finally:
            self._flush_all()
            if self._error_log and not self._error_log.closed:
                self._error_log.flush()
                self._error_log.close()
            logger.info("OrderBookCollector stopped. Total snapshots: %d", self._snapshots_total)

    async def stop(self) -> None:
        """Signal the collection loop to stop and flush all pending data."""
        logger.info("OrderBookCollector stop() called.")
        self._stop_event.set()

    # ── Internal loop ────────────────────────────────────────────────────────

    async def _run_loop(self) -> None:
        """Fixed-interval polling loop.

        Each cycle:
        1. Discover today's markets for all cities.
        2. Poll each market's order book with inter-request spacing.
        3. Sleep for whatever remains of the 5-second interval.
        """
        import time

        while not self._stop_event.is_set():
            cycle_start = time.monotonic()

            # ── Discover today's tickers ──────────────────────────────────
            markets = await self._discover_todays_markets()

            # ── Poll each market ──────────────────────────────────────────
            for city, ticker, close_time_iso in markets:
                if self._stop_event.is_set():
                    break
                await self._poll_one_market(city, ticker, close_time_iso)
                # Throttle: spread 24 calls over ~5 s => ~0.21 s spacing.
                # This keeps the collector under ~4.8 req/s even in the
                # worst case where all calls return instantly.
                await asyncio.sleep(REQUEST_SPACING_SEC)

            # ── Periodic status print ─────────────────────────────────────
            now_mono = time.monotonic()
            if now_mono - self._last_status_ts >= STATUS_PRINT_SEC:
                self._print_status()
                self._last_status_ts = now_mono
                self._errors_since_status = 0

            # ── Sleep the remainder of the interval ───────────────────────
            elapsed = time.monotonic() - cycle_start
            remaining = POLL_INTERVAL_SEC - elapsed
            if remaining > 0:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self._stop_event.wait()),
                        timeout=remaining,
                    )
                    # stop_event was set during the sleep
                    break
                except asyncio.TimeoutError:
                    pass  # normal path — interval elapsed

    # ── Market discovery ─────────────────────────────────────────────────────

    async def _discover_todays_markets(
        self,
    ) -> list[tuple[str, str, str | None]]:
        """Return list of (city, ticker, close_time_iso) for today's open markets."""
        from pykalshi.enums import MarketStatus

        today_str = _utc_today_kalshi_str()
        result: list[tuple[str, str, str | None]] = []

        for city, prefix in CITIES.items():
            try:
                markets = await self._kalshi.get_markets(
                    series_ticker=prefix,
                    status=MarketStatus.OPEN,
                    limit=200,
                )
            except Exception as exc:
                self._log_error(f"discover/{city}: {exc}")
                continue

            for m in markets:
                if today_str not in m.ticker:
                    continue
                close_time = getattr(m, "close_time", None)
                result.append((city, m.ticker, close_time))

        return result

    # ── Single-market poll ───────────────────────────────────────────────────

    async def _poll_one_market(
        self,
        city: str,
        ticker: str,
        close_time_iso: str | None,
    ) -> None:
        """Fetch order book for one market and write a CSV row.

        Retry logic:
        - On any failure: retry once after RETRY_DELAY_SEC.
        - On HTTP 429: back off RATE_LIMIT_BACKOFF_SEC (logged, no retry on top).
        - After both attempts fail: write an empty/NaN row and continue.
        """
        strike = _extract_strike(ticker)

        for attempt in range(2):
            try:
                market_obj  = await self._kalshi.get_market(ticker)
                book_resp   = await market_obj.get_orderbook()
                row = _build_row(ticker, city, strike, close_time_iso, book_resp, market_obj)
                self._write_row(ticker, city, strike, row)
                self._snapshots_total += 1
                return

            except Exception as exc:
                exc_str = str(exc)
                is_rate_limit = "429" in exc_str or "rate limit" in exc_str.lower()

                if is_rate_limit:
                    self._log_error(f"429/{city}/{ticker}: {exc}")
                    await asyncio.sleep(RATE_LIMIT_BACKOFF_SEC)
                    # Do not retry after a 429 — the next scheduled cycle
                    # will pick this market up naturally.
                    row = _build_empty_row(ticker, city, strike, _seconds_to_close(close_time_iso))
                    self._write_row(ticker, city, strike, row)
                    return

                if attempt == 0:
                    self._log_error(f"attempt1/{city}/{ticker}: {exc}")
                    self._errors_since_status += 1
                    await asyncio.sleep(RETRY_DELAY_SEC)
                else:
                    # Second failure — write NaN row and move on.
                    self._log_error(f"attempt2/{city}/{ticker}: {exc}")
                    self._errors_since_status += 1
                    row = _build_empty_row(ticker, city, strike, _seconds_to_close(close_time_iso))
                    self._write_row(ticker, city, strike, row)

    # ── CSV management ───────────────────────────────────────────────────────

    def _csv_path(self, city: str, strike: str, date_str: str) -> Path:
        """Return the CSV file path for a given market and date."""
        filename = f"orderbook_{city}_{strike}.csv"
        return self._output_dir / date_str / city / filename

    def _get_writer(self, ticker: str, city: str, strike: str) -> _MarketWriter:
        """Return (creating if needed) the _MarketWriter for this market today."""
        date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        key = (ticker, date_str)
        if key not in self._writers:
            # Close and remove any stale writer for this ticker from a previous date.
            stale_keys = [k for k in self._writers if k[0] == ticker and k[1] != date_str]
            for sk in stale_keys:
                try:
                    self._writers[sk].close()
                except Exception:
                    pass
                del self._writers[sk]
            path = self._csv_path(city, strike, date_str)
            self._writers[key] = _MarketWriter(path)
            logger.debug("Opened new CSV: %s", path)
        return self._writers[key]

    def _write_row(
        self, ticker: str, city: str, strike: str, row: dict[str, Any]
    ) -> None:
        """Write a row to the appropriate per-market CSV file."""
        try:
            writer = self._get_writer(ticker, city, strike)
            writer.write(row)
        except Exception as exc:
            self._log_error(f"csv_write/{ticker}: {exc}")

    def _flush_all(self) -> None:
        """Flush and close all open CSV files."""
        for writer in self._writers.values():
            writer.flush_and_close()
        self._writers.clear()
        logger.info("OrderBookCollector: all CSV files flushed and closed.")

    # ── Error logging ─────────────────────────────────────────────────────────

    def _log_error(self, message: str) -> None:
        """Append a timestamped error line to errors.log and the Python logger."""
        ts  = datetime.now(tz=timezone.utc).isoformat(timespec="milliseconds")
        line = f"{ts} | {message}\n"
        logger.warning("OrderBookCollector error: %s", message)
        try:
            if self._error_log and not self._error_log.closed:
                self._error_log.write(line)
                self._error_log.flush()
        except Exception:
            pass

    # ── Status print ──────────────────────────────────────────────────────────

    def _print_status(self) -> None:
        """Print a brief status line to the console every STATUS_PRINT_SEC."""
        now_utc = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(
            f"[OrderBookCollector] {now_utc} | "
            f"snapshots={self._snapshots_total} | "
            f"errors_last_5min={self._errors_since_status}"
        )


# ── Standalone entry point ────────────────────────────────────────────────────

async def _standalone_main() -> None:
    """Run the collector standalone for testing.

    Reads credentials from environment variables (same as the main bot):
        KALSHI_API_KEY_ID
        KALSHI_PRIVATE_KEY_PATH
    """
    import logging as _logging

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[_logging.StreamHandler(sys.stdout)],
    )

    api_key_id        = os.environ.get("KALSHI_API_KEY_ID", "")
    private_key_path  = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")

    if not api_key_id or not private_key_path:
        print(
            "ERROR: Set KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH "
            "environment variables before running standalone.",
            file=sys.stderr,
        )
        sys.exit(1)

    from pykalshi import AsyncKalshiClient, AsyncRateLimiter

    rate_limiter = AsyncRateLimiter(requests_per_second=10)
    kalshi = AsyncKalshiClient(
        api_key_id       = api_key_id,
        private_key_path = private_key_path,
        rate_limiter     = rate_limiter,
    )

    output_dir = Path(__file__).parent / "orderbook_data"
    collector  = OrderBookCollector(kalshi, output_dir=output_dir)

    async with kalshi:
        try:
            await collector.start()
        except KeyboardInterrupt:
            pass
        finally:
            await collector.stop()


if __name__ == "__main__":
    asyncio.run(_standalone_main())
