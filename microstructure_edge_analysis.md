# Kalshi Weather Market Microstructure Edge Analysis

## Date: 2026-03-14
## Context: $500 account, 5-city weather temperature brackets (NY, LA, CHI, MIA, DEN)
## Data: ~6.5M total historical trades (Jan 2025 - Mar 2026)

---

## EXECUTIVE SUMMARY: RANKED STRATEGIES

| Rank | Strategy | Expected Edge/Trade | Implementability | Capital Required | Confidence |
|------|----------|-------------------|------------------|-----------------|------------|
| 1 | **Late-Day Certainty Harvesting** | 5-50% | High (tick data only) | $50-100 | HIGH |
| 2 | **Cross-Bracket Probability Arbitrage** | 2-8% | High (tick data only) | $100-200 | HIGH |
| 3 | **Taker-Side YES Bias Fade** | 1-3% | Medium (tick + taker_side) | $100-200 | MEDIUM-HIGH |
| 4 | **Passive Market Making (Spread Collection)** | 0.5-2% | Medium (requires live API) | $200-500 | MEDIUM |
| 5 | **Trade Flow Imbalance Signal** | 0.5-1.5% | Medium (tick data) | $100 | MEDIUM |
| 6 | **Cross-City Correlation Lag** | 0.3-1% | Low (needs real-time weather) | $100 | LOW-MEDIUM |
| 7 | **NWS Update Lag Trading** | Unknown | Low (needs real-time NWS) | $100 | LOW |

---

## STRATEGY 1: LATE-DAY CERTAINTY HARVESTING (HIGHEST PRIORITY)

### Mechanism

This is the single most important finding from this analysis. Kalshi weather markets
for NY close at **03:59 UTC** (approximately 10:59 PM EST / 11:59 PM EDT). The markets
settle based on the daily high temperature from 12:00 AM to 11:59 PM Local Standard
Time.

**The critical insight**: By late afternoon/evening on the settlement day, the daily
high temperature is almost certainly already established. High temperatures in most US
cities occur between 12 PM and 5 PM local time. By 8 PM local time, the probability
that the high temperature will change is essentially zero (it would require an
extraordinary weather event for the temperature to rise above the afternoon peak at
10 PM).

This means: **from roughly 8-11 PM local time on the settlement day, you know the
outcome with near certainty, but the market may still be trading at prices that do
not reflect certainty.**

### Why the Market Misprice Exists

1. **Thin liquidity at night**: Most retail traders are not actively monitoring at
   10-11 PM. Stale limit orders may remain on the book.
2. **Uncertainty about NWS official reading**: Some traders may not realize the
   temperature has already been effectively determined, or may worry the NWS will
   report a different number.
3. **Settlement vs. actual**: The market waits for the *official* CLI report, not
   the observed high. But the CLI almost always matches the observed high from
   standard station readings (available in real-time from weather.gov).

### Data Available to Verify

From your tick data, you can check:
- For each day, what was the final traded price in the last 2-4 hours before close?
- Compare that final traded price against the settlement value (known from markets.csv)
- If contracts that settled YES were still trading at 0.80 or 0.90 (instead of 0.97-0.99)
  in the final hours, that's the edge.

### Signal Construction

```
1. At 8 PM local time, check current temperature data from weather.gov/NWS
2. Identify the daily high so far (from hourly obs or METAR data)
3. Check NWS hourly forecast for remaining hours - is there any chance of
   the high being exceeded? (Answer: almost never after 8 PM)
4. Determine which bracket will settle YES with >99% confidence
5. Check Kalshi prices for that bracket
6. If YES price < 0.95, BUY YES (you know it settles at $1.00)
7. If NO price < 0.95 for brackets that will settle NO, BUY NO
```

### Edge Calculation

- If the winning bracket is trading at $0.90 YES at 9 PM, and you know it will
  settle at $1.00:
  - Cost: $0.90 per contract
  - Payout: $1.00
  - Gross profit: $0.10 (11.1% return)
  - Fee (taker): 0.07 * 0.90 * 0.10 = $0.0063
  - Net profit: ~$0.094 per contract (10.4% return)
  - Win rate: ~99%+ (barring extraordinary events)

- If losing brackets are trading at $0.05 YES (= $0.95 NO) and you know they
  settle NO:
  - Buy NO at $0.95, settles at $1.00
  - Gross profit: $0.05 per contract (5.3%)
  - Fee: 0.07 * 0.05 * 0.95 = $0.0033
  - Net profit: ~$0.047 per contract (4.9% return)

### Position Sizing

At 99% confidence and 10% return:
- Kelly fraction: f* = (0.99 * 0.10/0.90 - 0.01) / (0.10/0.90) = ~88% Kelly
- Half Kelly: 44% of bankroll
- Conservative: $50-100 per opportunity (10-20% of $500)
- **With 5 cities per day, this alone could generate $5-25/day**

### Risk

- **NWS reports a different high than observed** (very rare, <1% of days)
- **Late-evening temperature spike** (tropical storms, chinook winds)
- **Market closes early or is halted** (Kalshi operational risk)
- **Stale data**: Your real-time temperature source disagrees with NWS station

### Implementation

```python
# Pseudocode for Late-Day Certainty Harvester
def check_evening_opportunity(city, current_time_local):
    if current_time_local.hour < 20:  # Before 8 PM
        return None  # Too early, high could still change

    # Get current observed high from NWS/weather.gov
    observed_high = get_nws_current_high(city)

    # Get NWS hourly forecast for remaining hours
    remaining_forecast = get_hourly_forecast(city, current_time_local, end_of_day)
    max_possible = max(observed_high, max(remaining_forecast))

    # Determine which bracket contains the high
    winning_bracket = find_bracket(city, observed_high)

    # Check if any remaining forecast could push into different bracket
    if find_bracket(city, max_possible) != winning_bracket:
        return None  # Uncertainty remains

    # Get current Kalshi prices
    prices = get_kalshi_prices(city, today)

    trades = []
    for bracket in prices:
        if bracket == winning_bracket and prices[bracket]['yes'] < 0.95:
            trades.append(('BUY_YES', bracket, prices[bracket]['yes']))
        elif bracket != winning_bracket and prices[bracket]['no'] < 0.97:
            # Buy NO on losing brackets if mispriced
            trades.append(('BUY_NO', bracket, prices[bracket]['no']))

    return trades
```

### Scalability

- Viable from $500 to $5,000+
- Limited by available contracts near close (liquidity at 10 PM is thin)
- 5 cities x ~365 days = ~1,825 opportunities/year
- Even finding edge on 20% of those = 365 trades/year

---

## STRATEGY 2: CROSS-BRACKET PROBABILITY ARBITRAGE

### Mechanism

Each day per city has exactly 6 brackets (4 B-brackets + 2 T-brackets). Since
exactly one bracket settles YES per day, the true probabilities MUST sum to 1.0.

**The arbitrage**: If market-implied probabilities (YES prices) across the 6 brackets
sum to significantly more or less than 1.0, there is a guaranteed mispricing.

- If sum > 1.0: The market is overpricing YES contracts in aggregate. Sell YES
  (buy NO) on the most overpriced brackets.
- If sum < 1.0: The market is underpricing YES contracts. Buy YES on the most
  underpriced brackets.

### Why This Edge Exists

1. **Independent pricing**: Each bracket trades independently. There is no
   automated mechanism enforcing the sum-to-one constraint.
2. **Retail traders price brackets in isolation**: A retail trader thinking "will
   the high be above 70?" doesn't check if all bracket prices sum correctly.
3. **Liquidity fragmentation**: With 6 markets per city per day, each individual
   bracket may have thin liquidity, allowing pricing inconsistencies to persist.

### Signal Construction

```
For each city-day at time t:
  1. Get last-traded YES price for all 6 brackets
  2. Sum = P(B1) + P(B2) + P(B3) + P(B4) + P(T_low) + P(T_high)
  3. If Sum > 1.05:  # 5% overpricing threshold (net of fees)
     - Sell YES on highest-overpriced bracket
     - OR buy NO on all brackets (guaranteed profit if sum > 1 + total_fees)
  4. If Sum < 0.95:  # 5% underpricing
     - Buy YES on most underpriced bracket
```

### Edge Calculation

- If brackets sum to 1.08, you can buy NO on all 6 brackets for total cost
  of 6 * (1 - avg_yes_price). One will lose (settle at $0), five will pay $1.
- Net investment: sum of NO prices = 6 - 1.08 = 4.92
- Payout: 5 * $1.00 = $5.00
- Gross profit: $0.08 per $4.92 invested = 1.6%
- After fees: depends on bracket prices, but if spread is >5%, profitable.

### Better Approach: Targeted Bracket Arbitrage

Rather than buying all 6, identify the SINGLE most mispriced bracket:
- If one bracket shows YES = 0.40 but based on other brackets' prices, its
  "fair" probability is 0.25, buy NO at 0.60 when fair NO is 0.75.
- Edge: 0.15 per contract (25% return on NO purchase)

### Data to Verify

From your markets.csv and trades.csv:
1. For each day, get the last traded price for each of the 6 brackets
2. Sum them
3. Calculate the distribution of sum deviations from 1.0
4. If systematic deviations >3% exist, this is tradeable

### Risk

- **Execution risk**: You need to trade quickly when you spot the mispricing,
  and it may be ephemeral
- **Stale prices**: "Last traded" prices may be hours old; the order book may
  have moved
- **You need all 6 to have recent trades**: If one bracket hasn't traded in
  hours, its price is uninformative

### Position Sizing

- Quarter Kelly recommended due to execution uncertainty
- $25-50 per trade initially
- Max 3% of account per single bracket position

---

## STRATEGY 3: TAKER-SIDE YES BIAS FADE

### Mechanism

Academic research (Becker 2024-2025) on 72.1M Kalshi trades found a massive,
persistent bias: **takers disproportionately buy YES contracts**, especially at
longshot prices. This creates a systematic overpricing of YES contracts.

Key findings from the research:
- Takers buying YES earn -1.12% average excess returns
- Makers earn +1.12% average excess returns
- At 1-cent contracts, takers win only 0.43% vs 1% implied
- NO contracts outperform YES at 69 of 99 price levels
- At extreme prices: NO = +23% EV, YES = -41% EV

### Application to Weather Markets

Weather markets should exhibit the same YES bias because:
1. Retail traders are drawn to "YES something happens" framing
2. The T-bracket structure amplifies this: "YES the temp will be above 90!"
   is exciting; "NO it won't" is boring
3. Longshot T-brackets (extreme temperature outcomes) should be
   systematically overpriced on the YES side

### Signal Construction

```
For each bracket:
  1. Track the taker_side field from trade data
  2. Calculate: YES_taker_volume / total_volume over trailing 30min
  3. If ratio > 0.65 (heavy YES-side taking):
     - Price is likely inflated by retail optimism
     - Sell YES (buy NO) on that bracket
  4. Target extreme brackets (T-brackets, prices 0.01-0.15 and 0.85-0.99)
     where the bias is strongest
```

### Edge Calculation

Based on Becker's research:
- At price levels 0.05-0.15 (longshot YES), average overpricing is ~2-4%
- Selling NO at $0.90 when fair value is $0.93 = 3.3% edge
- After fees: ~2% net edge
- Win rate: 85-95% (since you're selling against longshots)
- Expected value per $10 risked: $0.20-0.30

### Who You're Making Money From

Retail "weather betters" who:
- Buy YES on extreme temperature brackets for entertainment value
- Are anchored to exciting extreme outcomes
- Don't account for the -EV inherent in taker YES at longshot prices

### Risk

- **Weather markets may be different**: Finance-category markets on Kalshi show
  only 0.17pp gap (near-efficient). Weather may be similar if dominated by
  informed traders.
- **Small sample per bracket**: Only 6 brackets per city-day; bias may not be
  as strong as across all Kalshi categories

### Position Sizing

- Quarter Kelly: ~5% of account per trade ($25)
- Focus on T-brackets where YES bias is highest

---

## STRATEGY 4: PASSIVE MARKET MAKING (SPREAD COLLECTION)

### Mechanism

You act as a market maker: post resting limit orders on BOTH sides of a bracket.
Post a bid (buy YES at X) and an offer (sell YES at X+spread) simultaneously.
When both fill, you earn the spread.

### Why This Works on Kalshi Weather Markets

1. **Maker fees are lower than taker fees** (historically zero, now scaled
   with probability but still lower)
2. **Weather markets have moderate but not overwhelming liquidity** -- wide
   enough spreads to make this viable, enough volume to get fills
3. **The "Optimism Tax"**: per Becker's research, makers don't need to predict
   outcomes correctly. They profit simply by providing liquidity to biased
   takers.

### Typical Spreads (Estimated from Data)

From examining the tick data, consecutive trades on the same bracket show
price variations of 1-3 cents between trades, suggesting effective spreads
of $0.02-0.04 on liquid brackets.

Example from KXHIGHNY-26MAR13-T45 data:
- Trades alternate between 0.76, 0.77, 0.78, 0.80 YES prices
- Effective spread: ~$0.03-0.04

### Signal Construction

```
1. Identify brackets with moderate volume (>50 contracts/hour)
2. Calculate 30-min VWAP for each bracket
3. Post limit orders:
   - BID: VWAP - half_spread (buy YES)
   - ASK: VWAP + half_spread (sell YES)
4. Target spread width: $0.03 minimum
5. Manage inventory: if accumulated net position > 10 contracts in one
   direction, widen spread on that side to reduce fills
```

### Edge Calculation

Per round-trip (buy + sell):
- Revenue: spread width ($0.03 per contract)
- Cost: maker fees (approximately $0.003-0.01 per side depending on price)
- Net per round trip: ~$0.01-0.02
- If completing 20 round trips per day across 5 cities: $0.20-0.40/day
- Annualized: $70-$150 on $500 capital

### Problems at $500 Scale

- **Inventory risk is the killer**: If you buy 10 YES at $0.50 and the price
  drops to $0.30, you're stuck with $2 loss. Your $0.02 spread profit is
  destroyed.
- **You can't hedge**: No way to short brackets or hedge with correlated
  instruments
- **Adverse selection**: Your limit orders get picked off by informed traders
  who know the NWS just updated their forecast
- **Capital tie-up**: Each outstanding order ties up capital. With $500, you
  can only have a few orders live at once.

### Verdict

Market making is theoretically the most robust edge (per academic research),
but at $500 scale, **inventory risk dominates spread income**. This strategy
becomes viable at $3,000+ where you can absorb single-bracket losses while
maintaining enough outstanding orders across multiple markets.

**Recommendation: DO NOT pursue pure market making at $500. Instead, use
market-making principles (limit orders, spread awareness) within the other
strategies.**

---

## STRATEGY 5: TRADE FLOW IMBALANCE SIGNAL

### Mechanism

Research shows Order Flow Imbalance (OFI) explains ~65% of short-interval
price variance. On Kalshi, you have `taker_side` data which tells you
whether each trade was a buyer or seller initiating.

If you observe a cluster of taker-YES trades (someone aggressively buying YES),
the price will temporarily spike. If this spike is driven by noise (retail
enthusiasm) rather than information, it will mean-revert, and you can sell
into the spike.

### Signal Construction

```
For each bracket, rolling 15-minute window:
  1. Count YES_taker_trades vs NO_taker_trades
  2. Calculate net_imbalance = (YES_count - NO_count) / total_count
  3. Calculate price_change over same window

  IF net_imbalance > 0.7 AND price_change > 0.03:
    # Heavy buying pressure pushed price up
    # If this is retail noise, expect mean-reversion
    SELL_YES (buy NO) at current elevated price

  IF net_imbalance < -0.7 AND price_change < -0.03:
    # Heavy selling pressure pushed price down
    BUY_YES at depressed price
```

### Problem: Is It Noise or Information?

On weather markets, large directional flow often IS informed:
- A new NWS forecast just came out
- Someone checked the observed temperature and it's running hot/cold
- The morning observation confirmed the previous day's high

**Critical test**: Using historical data, for each instance of extreme
taker imbalance, check if the price continued in that direction (informed
flow) or reverted (noise). If reversion rate > 55%, this is tradeable.

### Edge Estimate

- If mean-reversion occurs 55% of the time after noise-driven spikes
- Average reversion: 2 cents
- Average continuation: 3 cents (when you're wrong, the informed trader
  was right, and the move is larger)
- EV per trade: 0.55 * $0.02 - 0.45 * $0.03 = $0.011 - $0.0135 = -$0.0025

**This is likely NEGATIVE EV on weather markets** because the informed
traders (who monitor NWS) create flow that looks like noise but is actually
information. The "noise" reversion that works on political/sports markets
may not work here because weather is too predictable.

### Verdict

**LOW PRIORITY.** Requires extensive backtesting to determine if
reversion rate > 55%, and theoretical priors suggest it won't work on
weather markets where information is quickly available.

---

## STRATEGY 6: CROSS-CITY CORRELATION LAG

### Mechanism

Weather systems move geographically. If an unusually warm day hits Chicago
today, the same system may affect New York tomorrow. If Kalshi weather
markets for NY don't immediately price in the temperature data observed
in Chicago, there's a lag to exploit.

### Problem

This is essentially a weather forecasting approach disguised as
microstructure. The NWS already incorporates all upstream weather data
into their forecasts, and Kalshi prices track NWS forecasts closely.
Any "lag" you detect is already priced in by the NWS ensemble models.

### When It Might Work

Same-day temperature correlation:
- If Denver's observed high by 3 PM MST is running much hotter than
  forecast, this might affect expectations for LA markets (which settle
  later in the day)
- But this requires real-time NWS data, not historical tick data

### Verdict

**LOW PRIORITY.** This is fundamentally a weather prediction strategy,
which you've already determined doesn't beat the market's pricing of NWS
forecasts.

---

## STRATEGY 7: NWS UPDATE LAG TRADING

### Mechanism

NWS updates forecasts at specific times (roughly every 6 hours). If a
new NWS forecast changes the temperature outlook, Kalshi prices should
adjust immediately. If there's a lag, you can trade ahead of the
market adjustment.

### Problem

This requires:
1. Real-time NWS forecast monitoring (not in your historical data)
2. Knowing exactly when forecasts update
3. Being faster than other traders who also monitor NWS
4. The market not already pricing the updated forecast

### When It Might Work

If NWS updates at, say, 4 AM UTC, and no one is actively trading
weather brackets at 4 AM, stale limit orders might sit on the book
for minutes before being picked off. You could be the picker.

### Verdict

**CANNOT be backtested with current data.** Requires real-time
implementation and testing. Medium potential but high operational
complexity.

---

## CRITICAL DATA FINDINGS FROM YOUR TICK DATA

### What We Know

Your data has these columns: `trade_id, ticker, count_fp, yes_price_dollars,
no_price_dollars, taker_side, created_time, ts`

**The `taker_side` field is extremely valuable.** This is data that most
retail traders don't analyze. It tells you WHO initiated each trade.

### Data Volume by City

| City | Total Trades | Trades/Day (avg) |
|------|-------------|-------------------|
| NY   | 1,894,567   | ~4,300            |
| LAX  | 1,381,012   | ~3,100            |
| CHI  | 1,332,527   | ~3,000            |
| MIA  | 1,046,963   | ~2,400            |
| DEN  | 874,146     | ~2,000            |

This is substantial volume -- about 15,000 trades per day across all cities.

### Close Times

NY markets close at 03:59 UTC (10:59 PM EST standard, 11:59 PM EDT daylight).
This is AFTER the daily high temperature has been determined in almost all cases.

### Trade Patterns Observed

From the NY data sample:
- Large trades (100+ contracts) appear regularly (e.g., 274, 107, 76 contracts)
- These likely come from automated/sophisticated traders
- Small trades (1-5 contracts) are interspersed -- likely retail
- Trades cluster at certain times (many trades in same second = sweeping the book)

---

## WHAT TO BUILD NEXT: CONCRETE BACKTESTING PLAN

### Step 1: Late-Day Certainty Harvester Backtest (HIGHEST PRIORITY)

Using your existing data:

```python
import pandas as pd

# Load trade and market data
trades = pd.read_csv('KXHIGHNY_trades.csv')
markets = pd.read_csv('KXHIGHNY_markets.csv')

# For each day, find trades in last 4 hours before close
for day in markets.groupby(close_date):
    close_time = day.close_time
    # Window: close_time - 4 hours to close_time
    late_trades = trades_for_day[
        trades_for_day.created_time > (close_time - timedelta(hours=4))
    ]

    # For each bracket, get last traded price
    for bracket in day.tickers:
        last_price = late_trades[late_trades.ticker == bracket].iloc[-1].yes_price_dollars
        settlement = markets[markets.ticker == bracket].settlement_value

        # Calculate: if you bought YES at last_price and it settled at settlement
        if settlement == 1.0:
            profit = 1.0 - last_price  # per contract
        else:
            profit = -last_price  # lost your investment

        # Log: bracket, last_price, settlement, profit, time_before_close
```

**What you're looking for**: How often is the winning bracket still trading
below $0.95 in the last 2-4 hours? What's the average edge?

### Step 2: Cross-Bracket Sum Analysis

```python
# For each day, at each hour, sum up bracket YES prices
for day in all_days:
    for hour in range(24):
        bracket_prices = {}
        for bracket in day_brackets:
            recent_price = get_last_trade_before(bracket, hour)
            bracket_prices[bracket] = recent_price

        prob_sum = sum(bracket_prices.values())
        # Log: day, hour, prob_sum, individual prices

# Analyze: distribution of prob_sum deviations from 1.0
# If systematic deviations > 3%, this is tradeable
```

### Step 3: Taker Bias Analysis

```python
# For each bracket, analyze taker_side distribution
for bracket_type in ['B', 'T']:
    for price_range in [(0, 0.1), (0.1, 0.3), (0.3, 0.7), (0.7, 0.9), (0.9, 1.0)]:
        trades_in_range = trades[
            (trades.yes_price_dollars >= price_range[0]) &
            (trades.yes_price_dollars < price_range[1]) &
            (trades.ticker.str.contains(bracket_type))
        ]
        yes_takers = trades_in_range[trades_in_range.taker_side == 'yes'].count_fp.sum()
        no_takers = trades_in_range[trades_in_range.taker_side == 'no'].count_fp.sum()
        ratio = yes_takers / (yes_takers + no_takers)

        # If ratio > 0.55, there's a YES bias at this price level
        # Cross-reference with settlement outcomes to calculate actual edge
```

---

## FEE STRUCTURE IMPACT

### Current Kalshi Fees (as of 2026)

**Taker fee formula**: 0.07 * P * (1 - P) per contract, where P = YES price

| YES Price | Fee per Contract | Fee as % of Price |
|-----------|-----------------|-------------------|
| $0.01     | $0.0007         | 7.0%              |
| $0.05     | $0.0033         | 6.7%              |
| $0.10     | $0.0063         | 6.3%              |
| $0.25     | $0.0131         | 5.3%              |
| $0.50     | $0.0175         | 3.5%              |
| $0.75     | $0.0131         | 1.7%              |
| $0.90     | $0.0063         | 0.7%              |
| $0.95     | $0.0033         | 0.3%              |
| $0.99     | $0.0007         | 0.07%             |

**Key insight**: Fees are LOWEST at extreme prices (near 0 or 1), which is
exactly where the Late-Day Certainty Harvester operates (buying near-certain
outcomes at 0.90-0.99). The fee on a $0.95 contract is only $0.0033 (0.3%).

**Maker fees**: Now also exist (introduced mid-2025), scaled similarly but
typically lower than taker fees. Exact current formula unknown but
structurally similar.

---

## WHAT WILL NOT WORK (ANTI-STRATEGIES)

### 1. Pure Weather ML Forecasting
Already proven: the market efficiently prices NWS forecasts. Your Random
Forest model lost $409 over ~3,000 trades. The market is efficient at
weather prediction. Stop trying to out-predict NWS.

### 2. Momentum Trading
Binary contracts converge to 0 or 1 at settlement. Any "momentum" is just
information incorporation, not a tradeable trend. Once the price moves,
the information is already priced.

### 3. Mean Reversion on Informed Flow
On weather markets, large price moves are usually correct (NWS update,
temperature observation). Fading informed flow is negative EV.

### 4. Pair Trading Across Cities
Temperature correlations between cities are already modeled by NWS.
Exploiting "lag" between cities is just worse weather forecasting.

### 5. High-Frequency Market Making at $500
You can't absorb inventory risk. One bad bracket wipes out weeks of
spread income. Need $3,000+ minimum.

---

## RECOMMENDED ACTION PLAN

### Week 1: Backtest Late-Day Certainty Harvester
- Write Python script to analyze last 4 hours of trading for each day
- Measure: how many days had winning bracket <$0.95 in final 2 hours?
- Measure: average profit per trade if buying all near-certain brackets
- Expected outcome: identify specific windows with systematic mispricing

### Week 2: Backtest Cross-Bracket Probability Sum
- Calculate hourly bracket probability sums across all cities
- Measure: distribution of deviations from 1.0
- Identify: which times of day show largest deviations
- Expected outcome: find if 3%+ deviations occur regularly

### Week 3: Taker Bias Analysis
- Analyze taker_side field across all 6.5M trades
- Segment by bracket type (B vs T), price level, time of day
- Cross-reference with settlement outcomes
- Expected outcome: confirm or reject Becker's YES-bias findings
  specifically for weather markets

### Week 4: Paper Trade Top Strategy
- Implement the highest-edge strategy from backtesting
- Paper trade for 5 days to verify execution feasibility
- Measure slippage vs. backtest expectations

### Week 5: Go Live
- Deploy with $50-100 per trade (10-20% of account)
- Track every trade against predicted edge
- Scale if profitable after 20+ trades

---

## KEY ACADEMIC REFERENCES

1. Becker (2024-2025) - "The Microstructure of Wealth Transfer in Prediction
   Markets" - Documents maker/taker dynamics, YES bias, and longshot bias
   across 72.1M Kalshi trades. Key finding: makers earn +1.12% average
   excess returns, takers lose -1.12%.

2. Whelan (2025-2026) - "Makers and Takers: The Economics of the Kalshi
   Prediction Market" - Confirms favorite-longshot bias, shows wealth
   transfer patterns intensified after professional market makers entered.

3. Bawa (2025) - "The Mathematical Execution Behind Prediction Market Alpha"
   - Provides OFI formula (R^2 = 0.65 for 15-min price prediction),
   Kelly criterion adaptation for binary contracts, and performance
   targets (Sharpe 2.0-2.8, 52-58% win rate, 2-4% edge per trade).

---

## BOTTOM LINE

**The single most promising strategy for your $500 account is the
Late-Day Certainty Harvester.** It exploits a structural feature of weather
markets -- the outcome is knowable hours before the market closes -- combined
with thin late-night liquidity that leaves mispriced contracts on the book.

The cross-bracket probability arbitrage is a strong secondary strategy that
requires only tick data you already have.

Everything else either requires more capital (market making), is unbacktestable
with current data (NWS update lag), or is fundamentally a weather prediction
approach in disguise (cross-city correlation).

**Start by backtesting Strategy 1. If it shows >3% average edge per trade
in the last 2 hours before close, deploy it immediately.**
