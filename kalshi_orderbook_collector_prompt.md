# Coding Agent Prompt: Kalshi Order Book Data Collector

## Overview
Add an order book data logging module to my existing Kalshi trading bot. This module should poll the Kalshi API every 5 seconds and record order book snapshots to CSV files for later use in training a neural network. The neural network's purpose is to optimize limit order entry prices on No contracts for daily weather temperature markets.

## Markets to Track
- I am trading daily **highest temperature** markets across **4 cities**
- Each city has **6 temperature bracket markets** (e.g., "Will the high in Phoenix exceed 85°F?")
- Total: **24 markets** polled every 5 seconds
- The specific market tickers/IDs will be provided separately or pulled dynamically from the API based on the current day's available temperature markets for the 4 cities

## Collection Schedule
- **Start time:** 4:00 AM PST daily
- **End time:** 5:00 PM PST daily
- **Interval:** Every 5 seconds
- Collection should run automatically and handle overnight restarts gracefully

## Data Columns to Capture

### Raw Fields from API (per snapshot)
| Column | Name | Description |
|--------|------|-------------|
| 1 | `timestamp` | UTC timestamp of the snapshot (ISO 8601 format, millisecond precision) |
| 2 | `market_id` | The Kalshi market ticker/ID string |
| 3 | `city` | City name (derived from market ticker) |
| 4 | `strike_temp` | The temperature threshold for this contract (e.g., 85) |
| 5 | `time_to_close` | Seconds remaining until market resolution |
| 6 | `best_bid` | Highest yes bid price (in cents, 0-100) |
| 7 | `bid_qty` | Quantity at the best bid |
| 8 | `best_ask` | Lowest yes ask price (in cents, 0-100) |
| 9 | `ask_qty` | Quantity at the best ask |
| 10 | `bid_price_2` | 2nd level bid price |
| 11 | `bid_qty_2` | Quantity at 2nd level bid |
| 12 | `bid_price_3` | 3rd level bid price |
| 13 | `bid_qty_3` | Quantity at 3rd level bid |
| 14 | `ask_price_2` | 2nd level ask price |
| 15 | `ask_qty_2` | Quantity at 2nd level ask |
| 16 | `ask_price_3` | 3rd level ask price |
| 17 | `ask_qty_3` | Quantity at 3rd level ask |
| 18 | `last_trade_price` | Price of the most recent trade |
| 19 | `volume_24h` | 24-hour trading volume |

### Derived Fields (calculate before writing to CSV)
| Column | Name | Formula |
|--------|------|---------|
| 20 | `spread` | `best_ask - best_bid` |
| 21 | `mid_price` | `(best_bid + best_ask) / 2` |
| 22 | `imbalance` | `bid_qty / (bid_qty + ask_qty)` — set to `NaN` if both quantities are 0 |

## File Structure
- Store one CSV file per market per day
- Naming convention: `orderbook_{city}_{strike_temp}_{YYYY-MM-DD}.csv`
- Save all files to a configurable output directory, default: `./orderbook_data/`
- Write headers only once at file creation
- Append rows throughout the day
- Use buffered writing (flush every 60 seconds or every 12 rows) to avoid excessive disk I/O

## Handling Missing / Empty Order Books
- If a market has **no bids or asks** (common during low-liquidity hours like 4-5 AM):
  - Set price fields to `NaN`
  - Set quantity fields to `0`
  - Still write the row with the timestamp (do NOT skip it)
  - The neural network needs to see these empty periods to learn liquidity patterns
- If fewer than 3 depth levels exist, fill missing levels with `NaN` for price and `0` for quantity

## Error Handling
- If an API call fails (timeout, rate limit, network error):
  - Log the error with timestamp to a separate `errors.log` file
  - Retry once after 2 seconds
  - If retry fails, skip that snapshot and continue to the next 5-second interval
  - Do NOT crash the entire collection loop over a single failed request
- If the API returns a rate limit response (HTTP 429):
  - Back off for 10 seconds before resuming
  - Log the rate limit event

## API Details
- Use the Kalshi REST API v2
- The relevant endpoints are:
  - `GET /trade-api/v2/markets` or `GET /trade-api/v2/markets/{ticker}` for market info
  - `GET /trade-api/v2/markets/{ticker}/orderbook` for order book data
- Authentication: I already have auth set up in my existing code — integrate with my existing session/token management
- Respect rate limits — with 24 markets every 5 seconds, that's ~4.8 requests/second. Check Kalshi's current rate limits and add appropriate throttling between requests within each polling cycle

## Code Structure Preferences
- Write this as a standalone module/class (e.g., `orderbook_collector.py`) that can be imported and started from my main bot script
- Use `asyncio` + `aiohttp` if my existing code is async, otherwise use `threading` + `requests`
- Provide a simple start/stop interface like:
  ```python
  collector = OrderBookCollector(api_client, markets, output_dir="./orderbook_data/")
  collector.start()  # begins collection loop
  collector.stop()   # graceful shutdown, flushes remaining data
  ```
- Include a `__main__` block so it can also run standalone for testing
- Add logging with Python's `logging` module (INFO level for normal ops, DEBUG for troubleshooting)

## Important Notes
- All prices from Kalshi are in cents (0-100 scale for yes contracts)
- Only capture the YES side of the book — No prices are just `100 - yes_price` and recording both is redundant
- Make sure the polling loop accounts for the time spent making API calls — if 24 API calls take 3 seconds, the next cycle should start 2 seconds later, not 5 seconds later (i.e., target fixed 5-second intervals from start-to-start, not end-to-start)
- The script will run on a Windows PC — make sure file paths and scheduling are Windows-compatible
- Add a brief console status print every 5 minutes showing: number of snapshots collected, any errors in the last period, and current time
