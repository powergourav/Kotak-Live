# Kotak Neo Nifty 200 Real-Time Scanner

A console-only, real-time Nifty 200 market scanner built on the official Kotak Neo Python SDK. It consumes the Kotak SFeed WebSocket, infers executed buy/sell volume with a quote-and-tick rule, builds rolling one-second candles, and prints momentum, order-flow, absorption, breakout, and market-breadth alerts.

The scanner is read-only. It does not place, modify, or cancel orders.

## Features

- Automated Kotak Neo TOTP login and MPIN validation.
- Live Nifty 200 constituent download from NSE, with an embedded 200-symbol fallback.
- Exact NSE EQ-series mapping to Kotak `nse_cm` `pSymbol` instrument tokens.
- Batched depth subscriptions for all 200 instruments.
- Three-thread pipeline that keeps console I/O away from WebSocket processing.
- Executed-volume inference from cumulative traded volume, best bid/ask, and tick direction.
- Rolling 1-minute, 5-minute, and 15-minute candles built from one-second buckets.
- Fast momentum, slow trend, absorption, day milestone, and fresh day high/low alerts.
- Five-minute Nifty 200 market breadth summaries.
- Per-symbol, per-alert cooldowns and automatic in-session WebSocket reconnection.
- No Telegram or other external notification integration.

## Architecture

```text
Kotak SFeed WebSocket
        │
        │ synchronous on_message callback
        ▼
queue.Queue(maxsize=100000)             Thread 1: WebSocket producer
        │
        ▼
Tick normalization → volume delta → one-second buckets
        │
        │ rolling 60s / 300s / 900s windows
        ▼
queue.Queue(maxsize=10000)              Thread 2: quant rolling engine
        │
        ▼
Console output                           Thread 3: console alert dispatcher
```

`on_message` only timestamps the payload and performs `put_nowait()`. Network downloads, calculations, logging, and console output do not run inside the callback.

## Signal Logic

All trading alerts require:

- Weekday market time from 09:17 through 15:29:59 IST.
- A real tick received within the last five seconds.
- At least ₹20 lakh of approximate rolling 5-minute turnover.
- A 180-second cooldown for the same `(symbol, alert_type)` pair.

### Fast rolling up

Triggered when either:

- 5-minute body is at least +0.50%, inferred buy share is at least 62%, and CLV is at least 0.72; or
- 1-minute body is at least +0.30%, inferred buy share is at least 62%, and CLV is at least 0.72.

LTP must also be above Kotak ATP/VWAP.

### Fast rolling down

Triggered when either:

- 5-minute body is at most -0.50%, inferred sell share is at least 62%, buy share is at most 38%, and CLV is at most 0.28; or
- 1-minute body is at most -0.30%, inferred sell share is at least 62%, buy share is at most 38%, and CLV is at most 0.28.

LTP must also be below Kotak ATP/VWAP.

### Slow steady accumulation

The 15-minute history is split into three consecutive 5-minute windows. W3 must have a higher high, higher low, and higher close than W2, and W2 must exceed W1 on the same measures. The W1-open to W3-close gain must be at least +0.75%, with at least 55% inferred buy share over 15 minutes.

### Slow steady distribution

The inverse structure is required: strictly lower highs, lows, and closes across W1, W2, and W3, a cumulative move of at most -0.75%, and at least 55% inferred sell share over 15 minutes.

### Hidden buyer absorption

Current 5-minute volume must be at least 2.2 times the average volume of the two preceding 5-minute windows. Sell share must be at least 55%, while price either loses less than 0.10% or forms a lower rejection wick with CLV of at least 0.65.

### Hidden seller absorption

Current 5-minute volume must be at least 2.2 times the average volume of the two preceding 5-minute windows. Buy share must be at least 55%, while price either gains less than 0.10% or forms an upper rejection wick with CLV of at most 0.35.

### Day milestones and breakouts

- Emits each newly reached ±1.5% session step, including +1.5%, +3.0%, -1.5%, and -3.0%.
- After 09:25 IST, fresh day highs require positive latest-second and rolling 1-minute volume delta confirmation.
- Fresh day lows require the symmetric negative volume delta confirmation.

## Volume Classification

For each increase in Kotak's cumulative traded-volume field:

1. LTP at or above the best ask is classified as aggressive buy volume.
2. LTP at or below the best bid is classified as aggressive sell volume.
3. If neither quote rule applies, an uptick is a buy and a downtick is a sell.
4. An unchanged LTP inherits the last non-zero tick direction.
5. If no direction is available, volume remains unclassified but still contributes to total volume.

The scanner intentionally does not use total buy quantity or total sell quantity (`TBQ`/`TSQ`) as executed flow.

## Requirements

- Python 3.10 or newer.
- Kotak Neo account with Trade API access and registered TOTP authentication.
- A synchronized system clock. TOTP authentication can fail if the clock is inaccurate.
- Outbound HTTPS access to Kotak and NSE.
- Outbound WebSocket access to the Kotak SFeed endpoint.

The scanner uses data and login APIs only. Kotak's static-IP enforcement applies to order APIs, not WebSocket market-data streams. If you later add order placement, create the API session and send orders from the same whitelisted static IP.

## Installation

```bash
git clone https://github.com/powergourav/Kotak-Live.git
cd Kotak-Live
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

On Windows PowerShell, activate the environment with:

```powershell
.venv\Scripts\Activate.ps1
```

## Configuration

Copy the example file for a local deployment:

```bash
cp .env.example .env
```

Required variables:

| Variable | Description |
|---|---|
| `KOTAK_CONSUMER_KEY` | Application token from Kotak Neo → More → Trade API → API Dashboard. |
| `KOTAK_MOBILE_NUMBER` | Registered mobile number, preferably including `+91`. |
| `KOTAK_UCC` | Kotak Unique Client Code. |
| `KOTAK_MPIN` | Kotak Neo MPIN. |
| `KOTAK_TOTP_SECRET` | Base32 seed from TOTP registration, not the current six-digit code. |

Optional variable:

| Variable | Description |
|---|---|
| `KOTAK_CONSUMER_SECRET` | Compatibility value for an SDK variant that accepts it. The current Kotak Neo SDK does not require it. |

The script loads `.env` from the same directory as the script. Existing process environment variables take precedence.

### Kiro Web

Store all five required values under **Settings → Sandbox → Secrets**. Do not put brokerage credentials in chat, source code, commits, issue descriptions, or pull requests. Start a new Kiro session after changing sandbox secrets so they are injected into the new environment.

## Running

```bash
python kotak_nifty200_realtime_scanner.py
```

Stop gracefully with `Ctrl+C` or `SIGTERM`.

Startup performs authentication and instrument mapping immediately, even outside market hours. The WebSocket producer then waits for the next weekday session. The built-in schedule is 09:15–15:30 IST and does not include an NSE holiday or special-session calendar.

## Output and Logs

Alerts and five-minute market pulse summaries are printed to standard output. Operational messages are written to standard error and to:

```text
kotak_nifty200_scanner.log
```

The log rotates at 10 MiB and retains five backups. Log files, `.env`, caches, and virtual environments are excluded by `.gitignore`.

## Operational Notes

- The first cumulative-volume value observed after startup is used as a baseline. A midday start cannot reconstruct earlier executed-volume direction.
- Fast signals need enough rolling coverage to satisfy their selected window. Slow-trend and absorption signals require a complete 15-minute history.
- One-second buckets use local receipt timestamps. Missing seconds carry the last price and ATP with zero incremental volume.
- The embedded constituent list is only a fallback. Startup fails rather than silently tracking a partial or ambiguous token map.
- The scanner's “institutional” and “iceberg” labels describe inferred market behavior. Retail market data cannot identify participant type or prove the existence of hidden orders.
- State is held in memory. Restarting resets rolling history, milestones, cooldowns, and accumulated inferred flow.
- Queue overflow preserves process liveness by dropping new payloads or alerts and incrementing counters.

## Troubleshooting

### Missing environment variables

Confirm all required names are present exactly as documented. In Kiro Web, save them under Sandbox Secrets and start a new session.

### TOTP login rejected

- Confirm `KOTAK_TOTP_SECRET` is the Base32 seed, not a six-digit code.
- Confirm the mobile number and UCC belong to the same account.
- Synchronize the machine clock with NTP.
- Verify the API application token is current.

### Token mapping fails

The scanner requires exactly 200 unique EQ mappings. A newly renamed or listed constituent can briefly differ between the NSE constituent file and Kotak's scrip master. Retry after both providers publish aligned files.

### No alerts immediately after startup

This can be normal. The scanner applies a two-minute opening warm-up, liquidity and stale-feed filters, rolling-window coverage requirements, and signal-specific thresholds. Fifteen-minute signals cannot fire before a full history exists.

### WebSocket disconnects

The Kotak SDK performs internal reconnect attempts and re-subscription. An outer loop continues retrying with exponential backoff during market hours and reauthenticates when an authentication or session error is detected.

## Security

- Never commit `.env` or any real credential.
- Restrict `.env` permissions on shared systems.
- Treat MPIN and the TOTP seed as account-control secrets.
- Rotate an API token immediately if it appears in chat, logs, screenshots, or Git history.
- Review logs before sharing them with support.
- Use a dedicated, least-privilege host for unattended production operation.

## Validation Performed

The current scanner has been checked with:

- Python syntax parsing.
- Ruff static analysis.
- Kotak Neo SDK 3.0.13 model and API compatibility.
- Real `nse_cm` scrip-master mapping for 200 unique Nifty 200 tokens.
- In-memory checks for all alert families and cooldown behavior.

Live authentication and market-feed behavior still depend on valid account credentials, market availability, and broker services.

## References

- [Official Kotak Neo Python SDK](https://github.com/Kotak-Neo/kotak-neo-python)
- [Kotak SFeed WebSocket guide](https://github.com/Kotak-Neo/kotak-neo-python/blob/main/docs/guides/websocket.md)
- [Kotak static-IP guidance](https://github.com/Kotak-Neo/Kotak-Neo/blob/main/docs/static-ip-whitelisting.md)
- [Official NSE Nifty 200 constituent CSV](https://nsearchives.nseindia.com/content/indices/ind_nifty200list.csv)

Content was rephrased for compliance with licensing restrictions.

## Disclaimer

This software provides informational market-data alerts. It is not investment advice and does not guarantee that inferred trade direction or alert labels are correct. Validate behavior in a controlled environment before relying on it for any financial decision.
