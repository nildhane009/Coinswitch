import os
import re
import json
import time
import shutil
import textwrap
import threading
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone, timedelta

import requests
import socketio
from cryptography.hazmat.primitives.asymmetric import ed25519


# ============================================================
# CONFIGURATION
# ============================================================

INTERVAL = "5"          # 5-minute candles

# ================================================================
# MASTER SAFETY SWITCH
#
#   False -> paper trading only. Signals, SL, exits, PnL are all
#            simulated in memory. NOTHING is sent to CoinSwitch.
#   True  -> REAL orders are placed with REAL money on your
#            CoinSwitch PRO futures account (fixed 10x leverage,
#            MARKET orders). Do not flip this to True until you
#            have watched the bot run in paper mode and are
#            comfortable with its behaviour.
# ================================================================
ORDERS_ENABLED = False

# Leverage applied to every symbol before its first trade (fixed,
# per your instruction). Must be <= the symbol's max_leverage -
# since the watchlist only includes symbols with max_leverage
# above MIN_LEVERAGE_FILTER (10), this fixed value is always valid.
TRADE_LEVERAGE = 10

# Maximum allowed loss on trading capital when the fixed SL is hit.
# Raw price risk is multiplied by TRADE_LEVERAGE. >30% is rejected.
MAX_SL_RISK_PCT = 30.0

PRINT_INTERVAL = 2.0

# Dashboard box width (terminal columns). Auto-detects the real terminal
# size (handy on a phone terminal app, e.g. Termux on a Realme P3 5G) and
# clamps it to a comfortable mobile-portrait range so the box never
# overflows or wraps ugly mid-word.
DASHBOARD_MIN_WIDTH = 40
DASHBOARD_MAX_WIDTH = 60

# --------------------------------------------------------------
# WATCHLIST (Top Gainers / Top Losers)
# --------------------------------------------------------------

TOP_N_GAINERS = 20
TOP_N_LOSERS = 20

# Only trade instruments whose max allowed leverage is ABOVE this.
# (Read from Get Instrument Info's max_leverage field per symbol.)
MIN_LEVERAGE_FILTER = 10.0

# How often (seconds) the gainers/losers watchlist is re-scanned.
WATCHLIST_REFRESH_SECONDS = 4 * 60 * 60   # 4 hours


# --------------------------------------------------------------
# CAPITAL ALLOCATION (paper-trading sizing - ORDERS_ENABLED is
# False, so no real orders are placed, but position sizing is
# simulated so PnL in USDT terms makes sense).
#
# Quantity per trade is NOT a fixed USDT slice anymore - each
# trade uses the exchange's MINIMUM allowed order quantity for
# that symbol (Get Instrument Info -> min_base_quantity). At fixed 10x,
# margin = notional / 10. The capital safety cap is applied to MARGIN,
# not to the full notional.
# --------------------------------------------------------------

TOTAL_CAPITAL = 10000.0        # initial paper capital
MAX_ALLOCATION_PCT = 50.0      # maximum MARGIN allocation from account equity
MAX_CONCURRENT_POSITIONS = 1   # only ONE trade open at a time

# Paper-mode fee rate per executed side, applied to notional.
# Live mode never uses this value; it records exchange-reported fees.
PAPER_FEE_RATE = 0.00065

# HFT scanner snapshot produced by hft_final_watchlist_v5.py
HFT_WATCHLIST_FILE = "hft_final_watchlist_v5.json"
HFT_REFRESH_SECONDS = 30.0
HFT_MIN_QUALITY = {"CONFIRMED", "WATCH"}
HFT_ALLOW_WATCH_FALLBACK = True

# Funding protection: no NEW entry inside this blackout window.
# CoinSwitch perpetual funding is treated as a 4-hour cycle here.
# Set these UTC times to the exchange's actual funding schedule if it differs.
FUNDING_SCHEDULE_UTC = [(0, 0), (4, 0), (8, 0), (12, 0), (16, 0), (20, 0)]
FUNDING_BLACKOUT_MINUTES = 5

# Account balance refresh. The execution API balance is the source of truth
# for capital used/available calculations; the old hardcoded 10000 is fallback only.
BALANCE_REFRESH_SECONDS = 30.0

MAX_DEPLOYABLE_CAPITAL = TOTAL_CAPITAL * (MAX_ALLOCATION_PCT / 100.0)

# --------------------------------------------------------------
# WEBSOCKET (candles only - ticker/live price comes from REST)
# --------------------------------------------------------------

WS_URL = "wss://ws.coinswitch.co/"
NAMESPACE = "/exchange_2"
SOCKETIO_PATH = "/pro/realtime-rates-socket/futures/exchange_2"

# --------------------------------------------------------------
# REST API
# --------------------------------------------------------------

BASE_URL = "https://coinswitch.co"

# Fill these in with your own CoinSwitch PRO API key pair
# (Profile -> API Trading on CoinSwitch PRO). Both are hex strings.
API_KEY = os.environ.get("COINSWITCH_API_KEY", "")
SECRET_KEY = os.environ.get("COINSWITCH_SECRET_KEY", "")

# How often (seconds) to poll the REST all-pairs ticker for live
# prices of every symbol in the watchlist.
TICKER_POLL_SECONDS = 2.0


# ============================================================
# WEBSOCKET CLIENT
# ============================================================

sio = socketio.Client(
    reconnection=True,
    reconnection_attempts=0,
    reconnection_delay=2,
    reconnection_delay_max=10,
)


# ============================================================
# SHARED STATE
# ============================================================

state_lock = threading.Lock()

ws_connected = False

# The symbols we currently want NEW entries on (refreshed every
# WATCHLIST_REFRESH_SECONDS).
watchlist = set()

# Every symbol we've ever subscribed a KLine stream for. This is a
# superset of `watchlist` - a symbol stays here (and keeps getting
# its candles/price updated) even after it drops out of the
# watchlist, for as long as it still has an open position, so an
# open trade is never abandoned mid-flight.
subscribed_symbols = set()

# Per-symbol state. Keys are symbol strings (e.g. "BTCUSDT").
# See make_symbol_state() for the shape of each value.
symbol_states = {}

# Simple trade log (kept in memory since orders are disabled).
# Each entry also carries a "symbol" key.
trade_log = []

# Persistent trading history/state. Paper and live are intentionally separate.
STATE_DIR = Path(os.environ.get("COINSWITCH_STATE_DIR", str(Path.home() / ".coinswitch_v1")))
PAPER_LOG_FILE = STATE_DIR / "paper_trade_log.json"
LIVE_LOG_FILE = STATE_DIR / "live_trade_log.json"
PAPER_STATE_FILE = STATE_DIR / "paper_capital_state.json"
SESSION_STARTED_AT = time.time()

paper_capital = TOTAL_CAPITAL
paper_trade_log = []
live_trade_log = []

last_watchlist_refresh = None

# Per-symbol trading rules from Get Instrument Info: max_leverage
# and min_base_quantity. Refreshed alongside the watchlist.
instrument_info = {}

# Symbols for which we've successfully set TRADE_LEVERAGE (only
# relevant when ORDERS_ENABLED is True - leverage must be set
# before the first order on a symbol).
leverage_set_symbols = set()

# HFT scanner state. The scanner NEVER creates an entry signal. It only
# confirms/prioritizes an entry generated by the existing 5m crossover.
hft_priority = {}
hft_loaded_at = None
hft_file_mtime = None
entry_priority_symbol = None
entry_priority_score = None

# Account/equity cache used for margin accounting and dashboard display.
account_balance = None
account_available_balance = None
account_balance_updated_at = None


# ============================================================
# HELPERS
# ============================================================

def to_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def to_int(value):
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def utc_string(timestamp_ms):
    if timestamp_ms is None:
        return "N/A"

    try:
        dt = datetime.fromtimestamp(
            timestamp_ms / 1000.0,
            tz=timezone.utc,
        )

        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

    except Exception:
        return "N/A"


def now_string():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def make_symbol_state():
    """
    Fresh per-symbol state dict. Every symbol we track gets one
    of these (candle history, signal search state, position).
    """

    return {
        "current_candle": None,
        "previous_closed_candle": None,

        "live_price": None,
        "previous_live_price": None,

        "signal": "WAIT",
        "entry_price": None,
        "stop_loss": None,
        "signal_candle_start": None,
        "signal_triggered": False,

        "in_position": False,
        "position_side": None,       # "BUY" or "SELL"
        "position_entry": None,
        "position_sl": None,
        "position_size": None,       # USDT notional (qty * entry price)
        "position_qty": None,        # base-asset quantity (min_qty for the symbol)
        "position_open_time": None,
        "position_margin": None,
        "capital_used_pct": None,
        "scanner_score": None,
        "scanner_quality": None,
        "scanner_bias": None,
        "entry_fee": 0.0,
    }


def ensure_symbol_state(symbol):
    """
    Must be called under state_lock. Creates a fresh state entry
    for `symbol` if one doesn't already exist.
    """

    if symbol not in symbol_states:
        symbol_states[symbol] = make_symbol_state()


# ============================================================
# PERSISTENT PAPER/LIVE TRADE STATE
# ============================================================

def _load_json_file(path, default):
    try:
        if not path.exists():
            return default
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if data is not None else default
    except Exception as e:
        print(f"[Persistence] Could not load {path}: {e}")
        return default


def _atomic_write_json(path, data):
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
        return True
    except Exception as e:
        print(f"[Persistence] Could not save {path}: {e}")
        return False


def load_persistent_state():
    """Restore paper capital and separate paper/live histories on startup."""
    global paper_capital, paper_trade_log, live_trade_log, trade_log

    paper_trade_log = _load_json_file(PAPER_LOG_FILE, [])
    live_trade_log = _load_json_file(LIVE_LOG_FILE, [])
    saved_capital = _load_json_file(PAPER_STATE_FILE, {})

    if isinstance(saved_capital, dict) and to_float(saved_capital.get("paper_capital")) is not None:
        paper_capital = float(saved_capital["paper_capital"])
    else:
        paper_capital = TOTAL_CAPITAL

    # Keep the existing in-memory trade_log API used by the strategy.
    trade_log = []

    print(f"[Persistence] Paper capital: {paper_capital:.6f}")
    print(f"[Persistence] Paper history: {len(paper_trade_log)} trades")
    print(f"[Persistence] Live history : {len(live_trade_log)} trades")


def save_paper_capital():
    _atomic_write_json(PAPER_STATE_FILE, {
        "paper_capital": paper_capital,
        "updated_at": time.time(),
    })


def _fee_number(value):
    value = to_float(value)
    return abs(value) if value is not None else None


def extract_actual_fee(payload):
    """Extract an actual fee/commission amount when the exchange returns it in the order payload."""
    if payload is None:
        return None
    keys = {
        "fee", "fees", "trading_fee", "transaction_fee", "commission",
        "commission_amount", "fee_amount", "executed_fee", "total_fee"
    }
    if isinstance(payload, dict):
        for k, v in payload.items():
            if str(k).lower() in keys:
                if isinstance(v, dict):
                    for sub in ("amount", "value", "total", "fee", "commission"):
                        n = _fee_number(v.get(sub))
                        if n is not None:
                            return n
                else:
                    n = _fee_number(v)
                    if n is not None:
                        return n
        for v in payload.values():
            n = extract_actual_fee(v)
            if n is not None:
                return n
    elif isinstance(payload, list):
        for item in payload:
            n = extract_actual_fee(item)
            if n is not None:
                return n
    return None


def paper_net_capital_change(pnl_usdt, fee):
    return float(pnl_usdt or 0.0) - float(fee or 0.0)


def save_trade_record(record, live=False):
    global paper_capital
    target = LIVE_LOG_FILE if live else PAPER_LOG_FILE
    history = live_trade_log if live else paper_trade_log
    history.append(record)
    _atomic_write_json(target, history)
    if not live:
        save_paper_capital()


def rolling_24h_records(history):
    cutoff = time.time() - 24 * 60 * 60
    return [r for r in history if to_float(r.get("closed_at")) is not None and float(r["closed_at"]) >= cutoff]


def rolling_24h_metrics(history):
    rows = rolling_24h_records(history)
    gross = sum(float(r.get("pnl_usdt") or 0.0) for r in rows)
    fees = sum(float(r.get("fee") or 0.0) for r in rows)
    net = gross - fees
    return rows, gross, fees, net


# ============================================================
# REST API (Ed25519-signed requests)
# ============================================================

def sign_request(method, path, params=None):
    """
    Build the headers and final URL path for an authenticated
    CoinSwitch request (Ed25519 signing), per CoinSwitch PRO's
    API Trading docs.
    """

    method = method.upper()

    if params:
        sep = "&" if "?" in path else "?"
        path = path + sep + urllib.parse.urlencode(params)

    decoded_path = urllib.parse.unquote_plus(path)

    epoch = str(int(time.time() * 1000))

    message = method + decoded_path + epoch

    secret = ed25519.Ed25519PrivateKey.from_private_bytes(
        bytes.fromhex(SECRET_KEY)
    )

    signature = secret.sign(message.encode("utf-8")).hex()

    headers = {
        "Content-Type": "application/json",
        "X-AUTH-APIKEY": API_KEY,
        "X-AUTH-SIGNATURE": signature,
        "X-AUTH-EPOCH": epoch,
    }

    return headers, decoded_path


def fetch_all_pairs_ticker():
    """
    GET /trade/api/v2/futures/all-pairs/ticker

    Returns a dict: { "BTCUSDT": {"price": .., "bid": .., "ask": ..,
    "mark": .., "timestamp": .., "pct24h": ..}, ... } on success,
    or None on any failure. Never raises.
    """

    try:

        headers, path = sign_request(
            "GET",
            "/trade/api/v2/futures/all-pairs/ticker",
            params={"exchange": "EXCHANGE_2"},
        )

        response = requests.get(
            BASE_URL + path,
            headers=headers,
            timeout=10,
        )

        response.raise_for_status()

        payload = response.json()

        raw = payload.get("data")

        if not isinstance(raw, dict):
            return None

        result = {}

        for symbol, ticker in raw.items():

            if not isinstance(ticker, dict):
                continue

            price = to_float(ticker.get("last_price"))

            if price is None:
                continue

            result[symbol] = {
                "price": price,
                "bid": to_float(ticker.get("best_bid_price")),
                "ask": to_float(ticker.get("best_ask_price")),
                "mark": to_float(ticker.get("mark_price")),
                "timestamp": to_int(ticker.get("timestamp")),
                "pct24h": to_float(ticker.get("price_24h_pcnt")),
            }

        return result

    except Exception as e:

        print(f"[REST All-Pairs Ticker Error] {e}")
        return None


def fetch_instrument_info():
    """
    GET /trade/api/v2/futures/instrument_info

    Returns a dict: { "BTCUSDT": {"max_leverage": .., "min_qty": ..}, ... }
    on success, or None on any failure. Never raises.
    """

    try:

        headers, path = sign_request(
            "GET",
            "/trade/api/v2/futures/instrument_info",
            params={"exchange": "EXCHANGE_2"},
        )

        response = requests.get(
            BASE_URL + path,
            headers=headers,
            timeout=10,
        )

        response.raise_for_status()

        payload = response.json()

        raw = payload.get("data")

        if not isinstance(raw, dict):
            return None

        result = {}

        for symbol, info in raw.items():

            if not isinstance(info, dict):
                continue

            max_leverage = to_float(info.get("max_leverage"))
            min_qty = to_float(info.get("min_base_quantity"))

            if max_leverage is None or min_qty is None:
                continue

            result[symbol] = {
                "max_leverage": max_leverage,
                "min_qty": min_qty,
            }

        return result

    except Exception as e:

        print(f"[REST Instrument Info Error] {e}")
        return None


# ============================================================
# REAL ORDER PLACEMENT (only actually called when ORDERS_ENABLED)
# ============================================================

def set_symbol_leverage(symbol):
    """
    POST /trade/api/v2/futures/leverage

    Sets TRADE_LEVERAGE for `symbol`. Must be called BEFORE the
    first order on that symbol (leverage can't be changed while
    there's an open position or open order on it). Returns True
    on success, False on failure. Never raises.
    """

    try:

        headers, path = sign_request(
            "POST", "/trade/api/v2/futures/leverage"
        )

        body = {
            "symbol": symbol,
            "exchange": "EXCHANGE_2",
            "leverage": TRADE_LEVERAGE,
        }

        response = requests.post(
            BASE_URL + path,
            headers=headers,
            json=body,
            timeout=10,
        )

        response.raise_for_status()

        print(f"[Leverage] {symbol} set to {TRADE_LEVERAGE}x")
        return True

    except Exception as e:

        print(f"[Leverage Error] ({symbol}) {e}")
        return False


def place_market_order(symbol, side, quantity, reduce_only=False):
    """
    POST /trade/api/v2/futures/order (MARKET order).

    side        — "BUY" or "SELL"
    quantity    — base-asset quantity
    reduce_only — True when this order is meant to CLOSE an
                  existing position (never opens a new/opposite one)

    Returns the parsed response dict on success, or None on any
    failure. Never raises.
    """

    try:

        headers, path = sign_request(
            "POST", "/trade/api/v2/futures/order"
        )

        body = {
            "exchange": "EXCHANGE_2",
            "symbol": symbol,
            "side": side,
            "order_type": "MARKET",
            "quantity": quantity,
        }

        if reduce_only:
            body["reduce_only"] = True

        response = requests.post(
            BASE_URL + path,
            headers=headers,
            json=body,
            timeout=10,
        )

        response.raise_for_status()

        data = response.json()

        print(f"[Order] {symbol} {side} qty={quantity} reduce_only={reduce_only} -> {data}")

        return data

    except Exception as e:

        print(f"[Order Error] ({symbol} {side} qty={quantity}) {e}")
        return None


# ============================================================
# HISTORICAL KLINES (REST) - used to seed a newly-added symbol's
# "previous closed candle" immediately, instead of waiting for
# it to arrive organically over the live WebSocket stream.
# ============================================================

def fetch_recent_klines(symbol, limit=3):
    """
    GET /trade/api/v2/futures/klines

    Returns a list of candle dicts (sorted oldest -> newest), each
    with: start_time, close_time, open, high, low, close, volume.
    Returns None on any failure. Never raises.
    """

    try:

        headers, path = sign_request(
            "GET",
            "/trade/api/v2/futures/klines",
            params={
                "symbol": symbol.lower(),
                "exchange": "EXCHANGE_2",
                "interval": INTERVAL,
                "limit": limit,
            },
        )

        response = requests.get(
            BASE_URL + path,
            headers=headers,
            timeout=10,
        )

        response.raise_for_status()

        payload = response.json()

        raw = payload.get("data")

        if not isinstance(raw, list):
            return None

        candles = []

        for row in raw:

            start_time = to_int(row.get("start_time"))
            close_time = to_int(row.get("close_time"))
            o = to_float(row.get("o"))
            h = to_float(row.get("h"))
            l = to_float(row.get("l"))
            c = to_float(row.get("c"))
            v = to_float(row.get("volume"))

            if start_time is None or o is None or h is None or l is None or c is None:
                continue

            candles.append(
                {
                    "start_time": start_time,
                    "close_time": close_time,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": v,
                }
            )

        candles.sort(key=lambda x: x["start_time"])

        return candles

    except Exception as e:

        print(f"[REST Klines Error] ({symbol}) {e}")
        return None


def seed_symbol_history(symbol):
    """
    Fetches the last few candles for `symbol` via REST and, if the
    symbol doesn't already have candle data (from the live
    WebSocket stream), seeds its previous_closed_candle (and, if
    available, current_candle too) so the strategy can evaluate
    signals immediately instead of waiting 5-10 minutes for two
    live candles to pass.
    """

    candles = fetch_recent_klines(symbol, limit=3)

    if not candles:
        return

    now_ms = int(time.time() * 1000)

    # A candle counts as fully closed only if its close_time has
    # actually passed - never trust the last row blindly, since
    # some APIs include the still-forming candle as the last row.
    closed = [
        c for c in candles
        if c["close_time"] is not None and c["close_time"] <= now_ms
    ]

    if not closed:
        return

    previous = closed[-1]

    # Whatever candle (if any) starts after `previous` is the
    # current, still-forming candle.
    current_candidate = None

    for c in candles:
        if c["start_time"] > previous["start_time"]:
            current_candidate = c
            break

    def strip(c):
        return {
            "start_time": c["start_time"],
            "open": c["open"],
            "high": c["high"],
            "low": c["low"],
            "close": c["close"],
            "volume": c["volume"],
        }

    with state_lock:

        if symbol not in symbol_states:
            return

        state = symbol_states[symbol]

        # Only seed if the live WebSocket hasn't already populated
        # this - never overwrite live data with a stale REST call.
        if state["previous_closed_candle"] is None:
            state["previous_closed_candle"] = strip(previous)

        if state["current_candle"] is None and current_candidate is not None:
            state["current_candle"] = strip(current_candidate)

    print(
        f"[Seed] {symbol}: previous candle loaded from REST "
        f"(O:{previous['open']:.6f} H:{previous['high']:.6f} "
        f"L:{previous['low']:.6f} C:{previous['close']:.6f})"
    )


# ============================================================
# WATCHLIST (Top Gainers / Top Losers)
# ============================================================

def fetch_futures_balance():
    """Fetch USDT balance from the same execution API used by orders."""
    try:
        headers, path = sign_request(
            "GET",
            "/trade/api/v2/futures/wallet_balance",
        )
        response = requests.get(BASE_URL + path, headers=headers, timeout=10)
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data")
        if not isinstance(data, dict):
            return None

        balances = data.get("base_asset_balances")
        if not isinstance(balances, list):
            return None

        for item in balances:
            if not isinstance(item, dict):
                continue
            if str(item.get("base_asset", "")).upper() != "USDT":
                continue
            b = item.get("balances") or {}
            total = to_float(b.get("total_balance"))
            available = to_float(b.get("total_available_balance"))
            if total is None:
                continue
            return {"total": total, "available": available if available is not None else total}

        return None
    except Exception as e:
        print(f"[REST Balance Error] {e}")
        return None


def refresh_account_balance():
    global account_balance, account_available_balance, account_balance_updated_at
    data = fetch_futures_balance()
    if data is None:
        return False
    with state_lock:
        account_balance = data["total"]
        account_available_balance = data["available"]
        account_balance_updated_at = time.time()
    return True


def effective_total_capital():
    if ORDERS_ENABLED:
        value = account_balance
        return value if value is not None and value > 0 else TOTAL_CAPITAL
    return paper_capital


def effective_available_capital():
    if ORDERS_ENABLED:
        value = account_available_balance
        if value is not None and value >= 0:
            return value
        return effective_total_capital()
    return paper_capital


def max_deployable_margin():
    return effective_total_capital() * (MAX_ALLOCATION_PCT / 100.0)


def funding_status(now=None):
    """Return funding blackout status using the configured 4-hour schedule."""
    if now is None:
        now = datetime.now(timezone.utc)

    candidates = []
    for day_offset in (-1, 0, 1):
        day = now.date()
        base = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        base = base + __import__("datetime").timedelta(days=day_offset)
        for hour, minute in FUNDING_SCHEDULE_UTC:
            candidates.append(base.replace(hour=hour, minute=minute, second=0, microsecond=0))

    nearest = min(candidates, key=lambda x: abs((x - now).total_seconds()))
    delta_seconds = (now - nearest).total_seconds()
    blackout_seconds = FUNDING_BLACKOUT_MINUTES * 60
    blocked = abs(delta_seconds) <= blackout_seconds
    next_funding = min((x for x in candidates if x > now), default=None)
    return blocked, nearest, next_funding, delta_seconds


def load_hft_watchlist(force=False):
    """Load the scanner snapshot. Scanner data is an entry-priority layer only."""
    global hft_priority, hft_loaded_at, hft_file_mtime
    path = Path(HFT_WATCHLIST_FILE)
    if not path.exists():
        return False

    try:
        mtime = path.stat().st_mtime
        if not force and hft_file_mtime == mtime:
            return True

        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = []
        for key in ("final_long", "final_short", "top_10_long", "top_10_short"):
            value = payload.get(key)
            if isinstance(value, list):
                rows.extend(value)
        if not rows:
            rows = payload.get("all_results") or []
        new_priority = {}

        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue

            momentum = row.get("momentum") or {}
            market = row.get("market") or {}
            orderbook = row.get("orderbook") or {}
            execution = row.get("execution") or {}

            move_5m = to_float(momentum.get("momentum_5m_pct")) or 0.0
            move_1m = to_float(momentum.get("momentum_1m_pct")) or 0.0
            score = to_float(row.get("score")) or 0.0
            quality = str(row.get("quality", "")).upper()
            if not quality:
                quality = "CONFIRMED" if row.get("confirmed") else "WATCH"

            # V5 uses direction-adjusted weighted orderbook strength in WOB.
            wob = to_float(row.get("weighted_orderbook"))
            if wob is None:
                wob = to_float(row.get("wob"))
            if wob is None:
                wob = 0.0

            bias = str(row.get("signal", "")).upper()
            if bias not in {"LONG", "SHORT"}:
                bias = "LONG" if move_5m > 0 else "SHORT" if move_5m < 0 else "MIXED"

            new_priority[symbol] = {
                "score": score,
                "quality": quality,
                "bias": bias,
                "move_5m": move_5m,
                "move_1m": move_1m,
                "wob": wob,
                "turnover_24h": to_float(market.get("turnover_24h")) or 0.0,
                "execution_score": to_float(execution.get("execution_score")) or 0.0,
            }

        with state_lock:
            hft_priority = new_priority
            hft_loaded_at = time.time()
            hft_file_mtime = mtime
        return True

    except Exception as e:
        print(f"[HFT Scanner] Load error: {e}")
        return False


def hft_entry_gate(symbol, side):
    """Scanner confirmation/priority gate. Does not create signals."""
    info = hft_priority.get(symbol)
    if not info:
        return False, -1.0, "NO_SCANNER_DATA"

    quality = info["quality"]
    if quality not in HFT_MIN_QUALITY:
        return False, info["score"], f"QUALITY_{quality or 'UNKNOWN'}"

    expected_bias = "LONG" if side == "BUY" else "SHORT"
    if info["bias"] not in {expected_bias}:
        return False, info["score"], f"SCANNER_CONFLICT_{info['bias']}"

    if not HFT_ALLOW_WATCH_FALLBACK and quality != "CONFIRMED":
        return False, info["score"], "WATCH_NOT_ALLOWED"

    return True, info["score"], quality


def hft_scanner_loop():
    while True:
        load_hft_watchlist()
        time.sleep(HFT_REFRESH_SECONDS)


def balance_poll_loop():
    while True:
        refresh_account_balance()
        time.sleep(BALANCE_REFRESH_SECONDS)


def compute_watchlist(all_ticker_data, instrument_data):
    """
    Given the dicts returned by fetch_all_pairs_ticker() and
    fetch_instrument_info(), first filters to symbols whose

    max_leverage is ABOVE MIN_LEVERAGE_FILTER, then picks the
    top TOP_N_GAINERS by 24h % change and the top TOP_N_LOSERS by
    (most negative) 24h % change from that eligible set. Returns a list of symbols
    (up to TOP_N_GAINERS + TOP_N_LOSERS, no duplicates).
    """

    ranked = [
        (symbol, data["pct24h"])
        for symbol, data in all_ticker_data.items()
        if data.get("pct24h") is not None
        and instrument_data.get(symbol, {}).get("max_leverage", 0)
        > MIN_LEVERAGE_FILTER
    ]

    if not ranked:
        return [], [], []

    gainers = sorted(ranked, key=lambda x: x[1], reverse=True)[:TOP_N_GAINERS]
    losers = sorted(ranked, key=lambda x: x[1])[:TOP_N_LOSERS]

    combined = []
    seen = set()

    for symbol, _ in gainers + losers:
        if symbol not in seen:
            seen.add(symbol)
            combined.append(symbol)

    return combined, gainers, losers


def subscribe_kline(symbol):
    """
    Subscribes to the KLine WebSocket stream for `symbol` at
    INTERVAL minutes, and marks it as subscribed. Safe to call
    more than once for the same symbol (subscribing again is a
    harmless no-op on the server side).
    """

    try:

        sio.emit(
            "FETCH_CANDLESTICK_CS_PRO",
            {
                "event": "subscribe",
                "pair": f"{symbol}_{INTERVAL}",
            },
            namespace=NAMESPACE,
        )

    except Exception as e:

        print(f"[WebSocket] Kline subscribe error ({symbol}): {e}")


def watchlist_refresh_loop():
    """Build the active watchlist from the HFT scanner snapshot."""
    global watchlist, last_watchlist_refresh, instrument_info

    while True:
        load_hft_watchlist(force=True)
        instrument_data = fetch_instrument_info()

        if instrument_data:
            # Only symbols present in the scanner snapshot are eligible.
            combined = sorted(
                hft_priority.keys(),
                key=lambda s: (
                    0 if hft_priority[s]["quality"] == "CONFIRMED" else 1,
                    -hft_priority[s]["score"],
                ),
            )

            combined = [
                s for s in combined
                if instrument_data.get(s, {}).get("max_leverage", 0) >= TRADE_LEVERAGE
            ]

            with state_lock:
                instrument_info = instrument_data
                new_symbols = [s for s in combined if s not in subscribed_symbols]
                for symbol in new_symbols:
                    ensure_symbol_state(symbol)
                    subscribed_symbols.add(symbol)
                watchlist = set(combined)

            for symbol in new_symbols:
                subscribe_kline(symbol)
                seed_symbol_history(symbol)
                time.sleep(0.5)
                if ORDERS_ENABLED:
                    success = set_symbol_leverage(symbol)
                    if success:
                        with state_lock:
                            leverage_set_symbols.add(symbol)

            last_watchlist_refresh = time.time()
            print()
            print("*" * 68)
            print(">>> HFT WATCHLIST REFRESHED <<<")
            print("*" * 68)
            print(f"Scanner file       : {HFT_WATCHLIST_FILE}")
            print(f"Scanner candidates : {len(hft_priority)}")
            print(f"Active watchlist   : {len(combined)}")
            print(f"New subscriptions  : {len(new_symbols)}")
            print("*" * 68)
        else:
            print("[Watchlist] Instrument info unavailable; keeping current watchlist.")

        time.sleep(HFT_REFRESH_SECONDS)


# ============================================================
# POSITION MANAGEMENT
# ============================================================
def calculate_raw_pnl_pct(entry, price, side):
    """Return raw price-movement PnL percentage before leverage."""
    if entry is None or entry == 0 or price is None:
        return 0.0
    if side == "BUY":
        return (price - entry) / entry * 100.0
    if side == "SELL":
        return (entry - price) / entry * 100.0
    return 0.0


def calculate_leveraged_pnl_pct(entry, price, side):
    """Return PnL percentage on trading capital at TRADE_LEVERAGE."""
    return calculate_raw_pnl_pct(entry, price, side) * TRADE_LEVERAGE


def sl_risk_allowed(entry, sl, symbol=None, side=None):
    """Allow entry only when leveraged SL risk is <= MAX_SL_RISK_PCT."""
    if entry is None or sl is None or entry <= 0:
        return False

    raw_risk_pct = abs(entry - sl) / entry * 100.0
    capital_risk_pct = raw_risk_pct * TRADE_LEVERAGE

    if capital_risk_pct > MAX_SL_RISK_PCT:
        label = f"{symbol} {side}" if symbol and side else (symbol or side or "trade")
        print(
            f"[ENTRY AVOIDED] {label}: SL capital risk {capital_risk_pct:.3f}% "
            f"> max {MAX_SL_RISK_PCT:.3f}% | raw price risk {raw_risk_pct:.3f}% | "
            f"leverage {TRADE_LEVERAGE}x | entry {entry:.8f} | SL {sl:.8f}"
        )
        return False

    return True


def count_open_positions():
    """Must be called under state_lock."""

    return sum(1 for s in symbol_states.values() if s["in_position"])


def total_deployed_notional():
    """Must be called under state_lock. Sum of notional (USDT) value
    across all currently open positions."""

    return sum(
        s["position_size"] or 0.0
        for s in symbol_states.values()
        if s["in_position"]
    )


def can_afford_new_position(symbol, price):
    """Check one-position limit and 50% MAX MARGIN allocation at 10x."""
    if count_open_positions() >= MAX_CONCURRENT_POSITIONS:
        return False

    info = instrument_info.get(symbol)
    if not info or info.get("min_qty") is None:
        return False

    prospective_notional = info["min_qty"] * price
    prospective_margin = prospective_notional / TRADE_LEVERAGE
    deployed_margin = total_deployed_notional() / TRADE_LEVERAGE

    return deployed_margin + prospective_margin <= max_deployable_margin() + 1e-12


def open_position(symbol, side, entry, sl, candle_start):
    """Open only after crossover + funding + scanner + capital safety gates."""
    state = symbol_states[symbol]

    if ORDERS_ENABLED and (account_balance is None or account_balance <= 0):
        print(f"[ENTRY BLOCKED] {symbol}: execution-account USDT balance is unavailable/zero.")
        return False

    blocked, funding_time, _, _ = funding_status()
    if blocked:
        print(f"[ENTRY BLOCKED] {symbol} {side}: funding blackout around {funding_time.strftime('%H:%M UTC')}")
        return False

    allowed, scanner_score, scanner_reason = hft_entry_gate(symbol, side)
    if not allowed:
        print(f"[ENTRY BLOCKED] {symbol} {side}: HFT scanner -> {scanner_reason}")
        return False

    # If multiple symbols cross in the same ticker cycle, only the
    # highest scanner-priority candidate gets the single position slot.
    if entry_priority_symbol is not None and entry_priority_symbol != symbol:
        if scanner_score < (entry_priority_score or scanner_score):
            print(f"[ENTRY PRIORITY] {symbol} deferred: {entry_priority_symbol} has higher scanner score {entry_priority_score:.2f}")
            return False

    if not sl_risk_allowed(entry, sl, symbol=symbol, side=side):
        return False

    qty = instrument_info.get(symbol, {}).get("min_qty")
    if qty is None:
        print(f"[open_position] No instrument info for {symbol}, skipping.")
        return False

    notional = qty * entry
    margin = notional / TRADE_LEVERAGE
    capital = effective_total_capital()
    capital_used_pct = (margin / capital * 100.0) if capital > 0 else 0.0

    if not can_afford_new_position(symbol, entry):
        print(
            f"[ENTRY BLOCKED] {symbol}: margin {margin:.6f} USDT exceeds available "
            f"deployment capacity {max_deployable_margin():.6f} USDT"
        )
        return False

    order_response = None
    if ORDERS_ENABLED:
        if symbol not in leverage_set_symbols:
            if set_symbol_leverage(symbol):
                leverage_set_symbols.add(symbol)
            else:
                print(f"[open_position] Could not confirm {TRADE_LEVERAGE}x leverage for {symbol}.")
                return False

        order_response = place_market_order(symbol, side, qty)
        if order_response is None:
            print(f"[open_position] Order placement FAILED for {symbol} {side}.")
            return False

        state["entry_fee"] = extract_actual_fee(order_response) or 0.0
        if extract_actual_fee(order_response) is None:
            print(f"[FEE WARNING] {symbol}: exchange entry-order response did not expose a fee field; entry fee saved as 0. No estimated fee used.")

    state["in_position"] = True
    state["position_side"] = side
    state["position_entry"] = entry
    state["position_sl"] = sl
    state["position_qty"] = qty
    state["position_size"] = notional
    state["position_margin"] = margin
    state["capital_used_pct"] = capital_used_pct
    state["scanner_score"] = scanner_score
    state["scanner_quality"] = scanner_reason
    state["scanner_bias"] = "LONG" if side == "BUY" else "SHORT"
    state["position_open_time"] = time.time()

    print()
    print("#" * 68)
    print(f">>> POSITION OPENED: {symbol} {side} <<<")
    print("#" * 68)
    print(f"Entry            : {entry:.8f}")
    print(f"Stop Loss        : {sl:.8f}")
    print(f"Quantity         : {qty}")
    print(f"Margin Used      : {margin:.6f} USDT")
    print(f"Capital Used     : {capital_used_pct:.3f}%")
    print(f"Notional         : {notional:.6f} USDT")
    print(f"Leverage         : {TRADE_LEVERAGE}x")
    print(f"HFT Priority     : {scanner_score:.2f} ({scanner_reason})")
    print(f"Real Order       : {'YES' if ORDERS_ENABLED else 'NO (paper trade)'}")
    print("#" * 68)
    return True


def close_position(symbol, exit_price, reason):
    global paper_capital, trade_log

    state = symbol_states[symbol]
    side = state["position_side"]
    entry = state["position_entry"]
    qty = state["position_qty"]
    size = state["position_size"]
    margin = state.get("position_margin") or ((size / TRADE_LEVERAGE) if size else 0.0)

    order_response = None
    entry_fee = float(state.get("entry_fee") or 0.0)
    exit_fee = 0.0
    fee = 0.0

    if ORDERS_ENABLED:
        opposite_side = "SELL" if side == "BUY" else "BUY"
        order_response = place_market_order(symbol, opposite_side, qty, reduce_only=True)
        if order_response is None:
            print(f"[close_position] CRITICAL: closing order FAILED for {symbol}.")
            return
        actual_fee = extract_actual_fee(order_response)
        if actual_fee is not None:
            exit_fee = actual_fee
        else:
            print(f"[FEE WARNING] {symbol}: exchange close-order response did not expose a fee field; exit fee saved as 0. No estimated fee used.")

    if ORDERS_ENABLED:
        fee = entry_fee + exit_fee
    else:
        # Paper mode mirrors the configured taker fee on both entry and exit.
        fee = (size or 0.0) * PAPER_FEE_RATE + (size or 0.0) * PAPER_FEE_RATE

    raw_pnl_pct = calculate_raw_pnl_pct(entry, exit_price, side)
    pnl_pct = raw_pnl_pct * TRADE_LEVERAGE
    pnl_usdt = size * raw_pnl_pct / 100.0 if size else 0.0
    margin_roi_pct = (pnl_usdt / margin * 100.0) if margin else 0.0
    net_pnl_usdt = pnl_usdt - fee
    timestamp = time.time()

    record = {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "exit": exit_price,
        "sl": state["position_sl"],
        "size": size,
        "margin": margin,
        "capital_used_pct": state.get("capital_used_pct"),
        "reason": reason,
        "pnl_pct": pnl_pct,
        "pnl_usdt": pnl_usdt,
        "fee": fee,
        "net_pnl_usdt": net_pnl_usdt,
        "margin_roi_pct": margin_roi_pct,
        "scanner_score": state.get("scanner_score"),
        "scanner_quality": state.get("scanner_quality"),
        "opened_at": state["position_open_time"],
        "closed_at": timestamp,
        "mode": "live" if ORDERS_ENABLED else "paper",
    }

    trade_log.append(record)

    if ORDERS_ENABLED:
        live_trade_log.append(record)
        _atomic_write_json(LIVE_LOG_FILE, live_trade_log)
    else:
        paper_capital = paper_capital + net_pnl_usdt
        paper_trade_log.append({**record, "resulting_capital": paper_capital})
        _atomic_write_json(PAPER_LOG_FILE, paper_trade_log)
        save_paper_capital()

    print()
    print("#" * 68)
    print(f">>> POSITION CLOSED: {symbol} {side} <<<")
    print("#" * 68)
    print(f"Entry            : {entry:.8f}")
    print(f"Exit             : {exit_price:.8f}")
    print(f"Reason           : {reason}")
    print(f"Price Move       : {raw_pnl_pct:+.3f}%")
    print(f"Gross PnL        : {pnl_usdt:+.6f} USDT")
    print(f"Fees             : {fee:.6f} USDT")
    print(f"Net PnL          : {net_pnl_usdt:+.6f} USDT")
    print(f"Margin ROI       : {margin_roi_pct:+.3f}%")
    print(f"Leveraged PnL    : {pnl_pct:+.3f}% @ {TRADE_LEVERAGE}x")
    if not ORDERS_ENABLED:
        print(f"Paper Capital    : {paper_capital:.6f} USDT")
    print("#" * 68)

    for key in ("in_position", "position_side", "position_entry", "position_sl",
                "position_size", "position_qty", "position_open_time", "position_margin",
                "capital_used_pct", "scanner_score", "scanner_quality", "scanner_bias", "entry_fee"):
        state[key] = False if key == "in_position" else None


def check_position_exit(symbol, old_price, price, previous_candle_now):
    """
    Must be called under state_lock, with symbol_states[symbol]
    ["in_position"] True.

    Exit conditions, checked in this order:
      1) Stop loss hit (based on the SL fixed at entry time).
      2) The OPPOSITE entry condition fires:
           - In a BUY -> exit when the *current* previous closed
             candle is GREEN and price crosses BELOW its open.
           - In a SELL -> exit when the *current* previous closed
             candle is RED and price crosses ABOVE its open.

    Returns (should_exit, exit_price, reason) or (False, None, None).
    """

    state = symbol_states[symbol]
    side = state["position_side"]
    entry = state["position_entry"]
    sl = state["position_sl"]

    # --------------------------------------------------------
    # 1) STOP LOSS
    # --------------------------------------------------------

    if side == "BUY" and price <= sl:
        return True, price, "STOP LOSS HIT"

    if side == "SELL" and price >= sl:
        return True, price, "STOP LOSS HIT"

    # --------------------------------------------------------
    # 2) OPPOSITE SETUP CROSS
    # --------------------------------------------------------

    if previous_candle_now is None:
        return False, None, None

    prev_open = previous_candle_now["open"]
    prev_close = previous_candle_now["close"]

    if prev_open == prev_close:
        return False, None, None

    if side == "BUY":

        if prev_close > prev_open:

            trigger = prev_open
            crossed_down = old_price >= trigger and price < trigger

            if crossed_down:
                return True, price, "OPPOSITE SETUP EXIT (SELL trigger formed)"

    elif side == "SELL":

        if prev_close < prev_open:

            trigger = prev_open
            crossed_up = old_price <= trigger and price > trigger

            if crossed_up:
                return True, price, "OPPOSITE SETUP EXIT (BUY trigger formed)"

    return False, None, None


# ============================================================
# CORE PRICE-DRIVEN LOGIC (per symbol)
# ============================================================

def handle_new_price(symbol, price, bid=None, ask=None, mark=None, trade_time=None):
    """
    Updates live price state for `symbol`, manages its open
    position's exit, and looks for fresh entry signals. Called
    once per REST ticker poll for every tracked symbol.
    """

    try:

        if price is None:
            return

        with state_lock:

            if symbol not in symbol_states:
                return

            state = symbol_states[symbol]

            old_price = state["previous_live_price"]

            state["live_price"] = price

            if bid is not None:
                state["bid_price"] = bid
            if ask is not None:
                state["ask_price"] = ask
            if mark is not None:
                state["mark_price"] = mark
            if trade_time is not None:
                state["last_trade_time"] = trade_time

            current = (
                dict(state["current_candle"])
                if state["current_candle"] is not None
                else None
            )

            previous = (
                dict(state["previous_closed_candle"])
                if state["previous_closed_candle"] is not None
                else None
            )

            current_signal_candle = state["signal_candle_start"]
            already_triggered = state["signal_triggered"]

            # --------------------------------------------------
            # STEP 1: If a position is open on this symbol, ONLY
            # manage the exit. No new entries evaluated for it.
            # --------------------------------------------------

            if state["in_position"] and old_price is not None:

                should_exit, exit_price, reason = check_position_exit(
                    symbol, old_price, price, previous
                )

                if should_exit:
                    close_position(symbol, exit_price, reason)

                state["previous_live_price"] = price
                return

            # --------------------------------------------------
            # STEP 2: Only look for NEW entries if this symbol is
            # still part of the active watchlist.
            # --------------------------------------------------

            symbol_is_watched = symbol in watchlist

        # ----------------------------------------------------
        # Required data for cross detection
        # ----------------------------------------------------

        if (
            not symbol_is_watched
            or current is None
            or previous is None
            or old_price is None
        ):

            with state_lock:
                symbol_states[symbol]["previous_live_price"] = price

            return

        current_start = current["start_time"]

        # ----------------------------------------------------
        # Only one signal per current 5-minute candle
        # ----------------------------------------------------

        if (
            already_triggered
            and current_signal_candle == current_start
        ):

            with state_lock:
                symbol_states[symbol]["previous_live_price"] = price

            return

        # ----------------------------------------------------
        # DOJI
        # ----------------------------------------------------

        previous_open = previous["open"]
        previous_close = previous["close"]
        previous_high = previous["high"]
        previous_low = previous["low"]

        if previous_close == previous_open:

            with state_lock:
                symbol_states[symbol]["previous_live_price"] = price

            return

        # ====================================================
        # RED PREVIOUS CANDLE -> BUY when price crosses ABOVE
        # previous OPEN. Entry = previous OPEN, SL = previous LOW.
        # ====================================================

        if previous_close < previous_open:

            trigger = previous_open
            crossed_up = old_price <= trigger and price > trigger

            if crossed_up:

                with state_lock:

                    state = symbol_states[symbol]

                    can_open = can_afford_new_position(symbol, price)
                    risk_allowed = sl_risk_allowed(
                        trigger, previous_low, symbol=symbol, side="BUY"
                    )

                    if (
                        (
                            not state["signal_triggered"]
                            or state["signal_candle_start"] != current_start
                        )
                        and not state["in_position"]
                        and can_open
                        and risk_allowed
                    ):

                        state["signal"] = "BUY"
                        state["entry_price"] = trigger
                        state["stop_loss"] = previous_low
                        state["signal_candle_start"] = current_start

                        print()
                        print("=" * 68)
                        print(f">>> BUY SIGNAL TRIGGERED: {symbol} <<<")
                        print("=" * 68)
                        print(f"Previous RED Open : {trigger:.8f}")
                        print(f"Live Price        : {price:.8f}")
                        print(f"Entry             : {trigger:.8f}")
                        print(f"Stop Loss         : {previous_low:.8f}")
                        raw_risk = abs(trigger - previous_low) / trigger * 100.0
                        print(f"SL Price Risk     : {raw_risk:.3f}%")
                        print(f"SL Capital Risk   : {raw_risk * TRADE_LEVERAGE:.3f}% @ {TRADE_LEVERAGE}x")
                        print(f"Leverage          : {TRADE_LEVERAGE}x")
                        print("Quantity          : EXCHANGE MINIMUM")
                        print("Orders            : ENABLED" if ORDERS_ENABLED else "Orders            : DISABLED")
                        print("=" * 68)

                        opened = open_position(
                            symbol, "BUY", trigger, previous_low, current_start
                        )
                        if opened:
                            state["signal_triggered"] = True

            with state_lock:
                symbol_states[symbol]["previous_live_price"] = price

            return

        # ====================================================
        # GREEN PREVIOUS CANDLE -> SELL when price crosses BELOW
        # previous OPEN. Entry = previous OPEN, SL = previous HIGH.
        # ====================================================

        if previous_close > previous_open:

            trigger = previous_open
            crossed_down = old_price >= trigger and price < trigger

            if crossed_down:

                with state_lock:

                    state = symbol_states[symbol]

                    can_open = can_afford_new_position(symbol, price)
                    risk_allowed = sl_risk_allowed(
                        trigger, previous_high, symbol=symbol, side="SELL"
                    )

                    if (
                        (
                            not state["signal_triggered"]
                            or state["signal_candle_start"] != current_start
                        )
                        and not state["in_position"]
                        and can_open
                        and risk_allowed
                    ):

                        state["signal"] = "SELL"
                        state["entry_price"] = trigger
                        state["stop_loss"] = previous_high
                        state["signal_candle_start"] = current_start

                        print()
                        print("=" * 68)
                        print(f">>> SELL SIGNAL TRIGGERED: {symbol} <<<")
                        print("=" * 68)
                        print(f"Previous GREEN Open : {trigger:.8f}")
                        print(f"Live Price           : {price:.8f}")
                        print(f"Entry                : {trigger:.8f}")
                        print(f"Stop Loss            : {previous_high:.8f}")
                        raw_risk = abs(trigger - previous_high) / trigger * 100.0
                        print(f"SL Price Risk        : {raw_risk:.3f}%")
                        print(f"SL Capital Risk      : {raw_risk * TRADE_LEVERAGE:.3f}% @ {TRADE_LEVERAGE}x")
                        print(f"Leverage             : {TRADE_LEVERAGE}x")
                        print("Quantity             : EXCHANGE MINIMUM")
                        print("Orders               : ENABLED" if ORDERS_ENABLED else "Orders               : DISABLED")
                        print("=" * 68)

                        opened = open_position(
                            symbol, "SELL", trigger, previous_high, current_start
                        )
                        if opened:
                            state["signal_triggered"] = True

            with state_lock:
                symbol_states[symbol]["previous_live_price"] = price

            return

        with state_lock:
            symbol_states[symbol]["previous_live_price"] = price

    except Exception as e:

        print(f"[Price Handler Error] ({symbol}) {e}")


def detect_cross_candidate(symbol, data):
    """Read-only snapshot check used to rank simultaneous crossover candidates."""
    try:
        price = data.get("price")
        if price is None:
            return None
        with state_lock:
            state = symbol_states.get(symbol)
            if not state or symbol not in watchlist or state["in_position"]:
                return None
            if state["previous_live_price"] is None or state["previous_closed_candle"] is None or state["current_candle"] is None:
                return None
            if state["signal_triggered"]:
                return None
            old_price = state["previous_live_price"]
            prev = state["previous_closed_candle"]
            current_start = state["current_candle"]["start_time"]

        if prev["close"] == prev["open"]:
            return None

        side = None
        if prev["close"] < prev["open"] and old_price <= prev["open"] < price:
            side = "BUY"
        elif prev["close"] > prev["open"] and old_price >= prev["open"] > price:
            side = "SELL"
        if side is None:
            return None

        allowed, score, reason = hft_entry_gate(symbol, side)
        if not allowed:
            return None
        return {"symbol": symbol, "side": side, "score": score, "reason": reason, "candle": current_start}
    except Exception:
        return None


def ticker_poll_loop():
    """Poll live prices and give the strongest simultaneous crossover first priority."""
    global entry_priority_symbol, entry_priority_score

    while True:
        all_data = fetch_all_pairs_ticker()

        if all_data:
            with state_lock:
                tracked = list(subscribed_symbols)

            # Snapshot all possible crossovers BEFORE mutating previous_live_price.
            candidates = []
            blocked, _, _, _ = funding_status()
            if not blocked:
                for symbol in tracked:
                    data = all_data.get(symbol)
                    if data is None:
                        continue
                    candidate = detect_cross_candidate(symbol, data)
                    if candidate:
                        candidates.append(candidate)

            candidates.sort(key=lambda x: x["score"], reverse=True)
            winner = candidates[0] if candidates else None
            entry_priority_symbol = winner["symbol"] if winner else None
            entry_priority_score = winner["score"] if winner else None

            for symbol in tracked:
                data = all_data.get(symbol)
                if data is None:
                    continue
                handle_new_price(
                    symbol,
                    data["price"],
                    bid=data["bid"],
                    ask=data["ask"],
                    mark=data["mark"],
                    trade_time=data["timestamp"],
                )

            entry_priority_symbol = None
            entry_priority_score = None

        time.sleep(TICKER_POLL_SECONDS)


# ============================================================
# KLINE WEBSOCKET

@sio.on("FETCH_CANDLESTICK_CS_PRO", namespace=NAMESPACE)
def on_kline(data):

    try:

        if not isinstance(data, dict):
            return

        symbol = data.get("s")
        start_time = to_int(data.get("t"))
        open_price = to_float(data.get("o"))
        high_price = to_float(data.get("h"))
        low_price = to_float(data.get("l"))
        close_price = to_float(data.get("c"))
        volume = to_float(data.get("v"))

        candle_closed = bool(data.get("x", False))

        if (
            symbol is None
            or start_time is None
            or open_price is None
            or high_price is None
            or low_price is None
            or close_price is None
        ):
            return

        new_candle = {
            "start_time": start_time,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "volume": volume,
        }

        with state_lock:

            if symbol not in symbol_states:
                # Not (or no longer) something we track - ignore.
                return

            state = symbol_states[symbol]

            old_candle = (
                dict(state["current_candle"])
                if state["current_candle"] is not None
                else None
            )

            # ------------------------------------------------
            # NEW 5-MINUTE CANDLE
            # ------------------------------------------------

            if (
                old_candle is not None
                and old_candle["start_time"] != start_time
            ):

                state["previous_closed_candle"] = dict(old_candle)

                # New candle gets a fresh signal window. This does
                # NOT touch an already-open position - it stays
                # open across candle boundaries (no fixed TP).
                state["signal"] = "WAIT"
                state["entry_price"] = None
                state["stop_loss"] = None
                state["signal_candle_start"] = None
                state["signal_triggered"] = False

                if state["live_price"] is not None:
                    state["previous_live_price"] = state["live_price"]

            # ------------------------------------------------
            # EXPLICIT CLOSED CANDLE
            # ------------------------------------------------

            if candle_closed:
                state["previous_closed_candle"] = dict(new_candle)

            state["current_candle"] = new_candle

    except Exception as e:

        print(f"[Kline Handler Error] {e}")


# ============================================================
# WEBSOCKET CONNECTION
# ============================================================

@sio.event(namespace=NAMESPACE)
def connect():

    global ws_connected

    ws_connected = True

    print()
    print("[WebSocket] CONNECTED")
    print(
        "[WebSocket] Waiting for watchlist refresh to subscribe "
        "to candle streams..."
    )


@sio.event(namespace=NAMESPACE)
def disconnect():

    global ws_connected

    ws_connected = False

    print()
    print("[WebSocket] DISCONNECTED")


@sio.event(namespace=NAMESPACE)
def connect_error(data):

    global ws_connected

    ws_connected = False

    print()
    print(f"[WebSocket] CONNECTION ERROR: {data}")


# ============================================================
# DISPLAY
# ============================================================
#
# Pure presentation layer for the terminal dashboard. Nothing here
# reads/writes trading state or changes any strategy/logic — it only
# formats the same values that were already computed in
# display_market() into a colored, boxed, mobile-friendly layout.

_ANSI_RE = re.compile(r"\033\[[0-9;]*m")


class Ansi:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    WHITE = "\033[97m"
    CYAN = "\033[96m"
    YELLOW = "\033[93m"
    GREEN = "\033[92m"
    RED = "\033[91m"
    GRAY = "\033[90m"
    MAGENTA = "\033[95m"


def _vlen(text):
    """Visible length of a string, ignoring ANSI color codes."""
    return len(_ANSI_RE.sub("", text))


def _pad(text, width):
    pad = width - _vlen(text)
    return text + (" " * pad if pad > 0 else "")


def _color(text, color):
    return f"{color}{text}{Ansi.RESET}"


def _pnl_color(value):
    return Ansi.GREEN if value >= 0 else Ansi.RED


def _dashboard_width():
    try:
        cols = shutil.get_terminal_size(fallback=(80, 24)).columns
    except Exception:
        cols = 80
    return max(20, cols)
# ---- outer box (white, double border) ----

def _outer_top(w):
    return _color("╔" + "═" * (w - 2) + "╗", Ansi.WHITE)


def _outer_bottom(w):
    return _color("╚" + "═" * (w - 2) + "╝", Ansi.WHITE)


def _outer_div(w):
    return _color("╟" + "─" * (w - 2) + "╢", Ansi.WHITE)


def _outer_line(w, text=""):
    content_w = w - 4
    return (
        _color("║", Ansi.WHITE) + " " + _pad(text, content_w) + " " + _color("║", Ansi.WHITE)
    )


def _outer_title(w, text, color=Ansi.BOLD + Ansi.YELLOW):
    content_w = w - 4
    stripped = _vlen(text)
    total_pad = max(0, content_w - stripped)
    left = total_pad // 2
    right = total_pad - left
    centered = (" " * left) + _color(text, color) + (" " * right)
    return _outer_line(w, centered)


def _outer_blank(w):
    return _outer_line(w, "")


# ---- inner box (cyan, single border) nested inside the outer box ----

def _inner_top(w, label=""):
    content_w = w - 4
    if label:
        lbl = f" {label} "
        dash_total = max(0, content_w - 2 - _vlen(lbl))
        left = 2
        right = max(0, dash_total - left)
        line = "┌" + "─" * left + lbl + "─" * right + "┐"
    else:
        line = "┌" + "─" * (content_w - 2) + "┐"
    return _outer_line(w, _color(line, Ansi.CYAN))


def _inner_bottom(w):
    content_w = w - 4
    return _outer_line(w, _color("└" + "─" * (content_w - 2) + "┘", Ansi.CYAN))


def _inner_line(w, text=""):
    content_w = w - 4
    inner_w = content_w - 4
    body = (
        _color("│", Ansi.CYAN) + " " + _pad(text, inner_w) + " " + _color("│", Ansi.CYAN)
    )
    return _outer_line(w, body)


def _inner_wrapped(w, label, symbols, color, empty_text="none"):
    """Yield inner-box lines for a label + a wrapped, comma-joined symbol list."""
    content_w = w - 4
    inner_w = content_w - 4
    count = len(symbols)
    header = _color(f"{label} ({count}):", Ansi.BOLD + color)
    lines = [_inner_line(w, header)]
    if not symbols:
        lines.append(_inner_line(w, _color(f"  {empty_text}", Ansi.GRAY)))
        return lines
    joined = ", ".join(symbols)
    for wrapped in textwrap.wrap(joined, width=max(10, inner_w - 2)) or [""]:
        lines.append(_inner_line(w, "  " + wrapped))
    return lines


def _split_line(w, left, right):
    """Outer-box line with `left` flush left and `right` flush right on the
    same row, fully visible (no truncation) with the gap between filled in."""
    content_w = w - 4
    lv, rv = _vlen(left), _vlen(right)
    gap = max(1, content_w - lv - rv)
    return _outer_line(w, left + (" " * gap) + right)


def _pair_row(w, seg1, seg2, connector=" \u2502 "):
    """Two small connected 'boxes' (seg1, seg2) on one inner-box row when
    they fit; falls back to one segment per row if the combo is too wide."""
    content_w = w - 4
    inner_w = content_w - 4
    combined = seg1 + connector + seg2
    if _vlen(combined) <= inner_w:
        return [_inner_line(w, combined)]
    return [_inner_line(w, seg1), _inner_line(w, seg2)]


def display_market():
    with state_lock:
        connected = ws_connected
        watch_count = len(watchlist)
        tracked_count = len(subscribed_symbols)
        refresh_ts = last_watchlist_refresh
        open_positions = []

        for symbol, state in symbol_states.items():
            if state["in_position"]:
                price = state["live_price"]
                entry = state["position_entry"]
                side = state["position_side"]
                unreal = calculate_leveraged_pnl_pct(entry, price, side) if price is not None and entry else 0.0
                open_positions.append({
                    "symbol": symbol, "side": side, "entry": entry, "price": price,
                    "sl": state["position_sl"], "size": state["position_size"],
                    "qty": state["position_qty"], "unreal": unreal,
                    "scanner_score": state.get("scanner_score"),
                    "scanner_quality": state.get("scanner_quality"),
                })

        # Only trades created in this terminal session are displayed in the list.
        closed_trades = [t for t in trade_log if float(t.get("closed_at") or 0) >= SESSION_STARTED_AT]
        history = live_trade_log if ORDERS_ENABLED else paper_trade_log
        history_24h, gross_24h, fees_24h, net_24h = rolling_24h_metrics(history)
        historical_count = len(history)

        missing_price, missing_candles = [], []
        red_symbols, green_symbols, doji_symbols = [], [], []
        for symbol in watchlist:
            state = symbol_states.get(symbol)
            if state is None:
                missing_price.append(symbol); missing_candles.append(symbol); continue
            if state["live_price"] is None:
                missing_price.append(symbol)
            prev = state["previous_closed_candle"]
            if prev is None:
                missing_candles.append(symbol); continue
            if state["in_position"]:
                continue
            if prev["close"] < prev["open"]: red_symbols.append(symbol)
            elif prev["close"] > prev["open"]: green_symbols.append(symbol)
            else: doji_symbols.append(symbol)

    deployed = sum(p["size"] or 0 for p in open_positions)
    deployed_margin = deployed / TRADE_LEVERAGE
    total_capital = effective_total_capital()
    available_capital = effective_available_capital()
    max_margin = max_deployable_margin()
    used_pct = (deployed_margin / total_capital * 100.0) if total_capital > 0 else 0.0
    blocked, funding_time, next_funding, _ = funding_status()
    mode_label = "LIVE" if ORDERS_ENABLED else "PAPER"

    realized_pnl_pct = sum(float(t.get("pnl_pct") or 0.0) for t in closed_trades)
    realized_pnl_usdt = sum(float(t.get("pnl_usdt") or 0.0) for t in closed_trades)
    wins = sum(1 for t in closed_trades if float(t.get("pnl_pct") or 0.0) > 0)
    unrealized_pnl_pct = sum(p["unreal"] for p in open_positions)
    unrealized_pnl_usdt = 0.0
    for p in open_positions:
        if p["price"] is not None:
            raw_unreal = calculate_raw_pnl_pct(p["entry"], p["price"], p["side"])
            unrealized_pnl_usdt += (p["size"] or 0.0) * raw_unreal / 100.0

    total_pnl_pct = realized_pnl_pct + unrealized_pnl_pct
    total_pnl_usdt = realized_pnl_usdt + unrealized_pnl_usdt

    w = _dashboard_width()
    L = ["\033[2J\033[H"]

    # ---- header ----
    L.append(_outer_top(w))
    L.append(_outer_title(w, "HFT PRIORITY + 5M BODY OPEN CROSS"))
    L.append(_outer_title(w, f"FIXED {TRADE_LEVERAGE}x"))
    L.append(_outer_div(w))

    ws_color = Ansi.GREEN if connected else Ansi.RED
    ws_text = "CONNECTED" if connected else "DISCONNECTED"
    orders_tag = _color("LIVE", Ansi.BOLD + Ansi.RED) if ORDERS_ENABLED else _color("PAPER", Ansi.BOLD + Ansi.CYAN)
    L.append(_split_line(w, f"WebSocket : {_color(ws_text, Ansi.BOLD + ws_color)}", orders_tag))
    L.append(_outer_line(w, f"Time (UTC): {now_string()}"))
    L.append(_outer_line(w, f"Watchlist : {watch_count} active | {tracked_count} tracked"))
    if refresh_ts:
        next_refresh_in = max(0, int(WATCHLIST_REFRESH_SECONDS - (time.time() - refresh_ts)))
        L.append(_outer_line(w, f"Next refresh in ~{next_refresh_in // 60} min"))
    else:
        L.append(_outer_line(w, "Watchlist : not yet refreshed"))

    # ---- scan results (nested nested box = "double layer") ----
    L.append(_outer_div(w))
    data_ok = watch_count - len(set(missing_price) | set(missing_candles))
    L.append(_outer_line(w, _color(f"Scan status: {data_ok}/{watch_count} symbols ready", Ansi.BOLD + Ansi.WHITE)))
    L.append(_inner_top(w, "SCAN RESULTS"))
    if missing_price:
        L.extend(_inner_wrapped(w, "NO LIVE PRICE", missing_price, Ansi.YELLOW))
    if missing_candles:
        L.extend(_inner_wrapped(w, "NO CANDLE DATA", missing_candles, Ansi.YELLOW))
    L.extend(_inner_wrapped(w, "RED  - waiting BUY ", red_symbols, Ansi.RED))
    L.extend(_inner_wrapped(w, "GREEN - waiting SELL", green_symbols, Ansi.GREEN))
    if doji_symbols:
        L.extend(_inner_wrapped(w, "DOJI - no signal", doji_symbols, Ansi.GRAY))
    L.append(_inner_bottom(w))

    # ---- capital / account ----
    L.append(_outer_div(w))
    L.append(_outer_line(w, f"Mode     : {_color(mode_label, Ansi.BOLD + (Ansi.RED if ORDERS_ENABLED else Ansi.CYAN))}"))
    L.append(_outer_line(w, f"Capital  : {total_capital:.2f} USDT"))
    L.append(_outer_line(w, f"Available: {available_capital:.2f} USDT"))
    L.append(_outer_line(w, f"Margin   : used {deployed_margin:.2f} / max {max_margin:.2f} USDT"))
    L.append(_outer_line(w, f"          ({MAX_ALLOCATION_PCT:.0f}% cap, {used_pct:.2f}% used)"))
    L.append(_outer_line(w, f"Notional : {deployed:.2f} USDT @ {TRADE_LEVERAGE}x"))
    L.append(_outer_line(w, f"Positions: {len(open_positions)} / {MAX_CONCURRENT_POSITIONS}"))
    L.append(_outer_line(w, f"Max SL   : {MAX_SL_RISK_PCT:.1f}% of margin"))
    fund_color = Ansi.RED if blocked else Ansi.GREEN
    fund_text = "BLOCKED" if blocked else "ENTRY ACTIVE"
    L.append(_outer_line(w, f"Funding  : {_color(fund_text, Ansi.BOLD + fund_color)}"))
    L.append(_outer_line(w, f"           next {funding_time.strftime('%H:%M UTC')} (±{FUNDING_BLACKOUT_MINUTES}m)"))

    # ---- pnl summary (moved up, right under capital/account) ----
    L.append(_outer_div(w))
    L.append(_outer_line(w, f"Realized  : {_color(f'{realized_pnl_pct:.3f}% ({realized_pnl_usdt:+.2f})', _pnl_color(realized_pnl_pct))}"))
    L.append(_outer_line(w, f"Unrealized: {_color(f'{unrealized_pnl_pct:.3f}% ({unrealized_pnl_usdt:+.2f})', _pnl_color(unrealized_pnl_pct))}"))
    L.append(_outer_line(w, _color(f"TOTAL     : {total_pnl_pct:.3f}% ({total_pnl_usdt:+.2f})", Ansi.BOLD + _pnl_color(total_pnl_pct))))
    L.append(_outer_line(w, f"Fees 24h  : {fees_24h:.4f} USDT"))
    L.append(_outer_line(w, f"24h PnL   : {_color(f'{net_24h:+.4f}', _pnl_color(net_24h))} (gross {gross_24h:+.4f})"))
    if closed_trades:
        L.append(_outer_line(w, f"Win rate  : {wins}/{len(closed_trades)}"))

    # ---- open positions (numbered, compact connected-box rows) ----
    L.append(_outer_div(w))
    L.append(_inner_top(w, f"OPEN POSITIONS ({len(open_positions)})"))
    if open_positions:
        for i, p in enumerate(open_positions, start=1):
            price_str = f"{p['price']:.4f}" if p['price'] is not None else "N/A"
            side_color = Ansi.GREEN if p['side'] == "BUY" else Ansi.RED
            margin_p = (p['size'] or 0.0) / TRADE_LEVERAGE
            cap_used = (margin_p / total_capital * 100.0) if total_capital else 0.0
            unreal_str = f"{p['unreal']:.3f}%"
            L.append(_inner_line(w, _color(f"#{i} [{p['side']}] {p['symbol']}", Ansi.BOLD + side_color)))
            L.extend(_pair_row(w, f"Qty {p['qty']}", f"Entry {p['entry']:.4f}"))
            L.extend(_pair_row(w, f"Price {price_str}", f"SL {p['sl']:.4f}"))
            L.extend(_pair_row(
                w,
                f"PnL {_color(unreal_str, _pnl_color(p['unreal']))}",
                f"Margin {margin_p:.2f} ({cap_used:.2f}%)",
            ))
    else:
        L.append(_inner_line(w, _color("none", Ansi.GRAY)))
    L.append(_inner_bottom(w))

    # ---- closed trades (numbered, same compact connected-box format) ----
    L.append(_outer_div(w))
    L.append(_inner_top(w, f"CLOSED: {len(closed_trades)} sess / {historical_count} hist"))
    if closed_trades:
        for i, t in enumerate(closed_trades, start=1):
            side_color = Ansi.GREEN if t['side'] == "BUY" else Ansi.RED
            pnl_c = _pnl_color(float(t.get("pnl_pct") or 0.0))
            trade_pnl_str = f"{t['pnl_pct']:.3f}% ({t['pnl_usdt']:+.2f})"
            net = float(t.get('net_pnl_usdt') or 0.0)
            fee = float(t.get('fee') or 0.0)
            L.append(_inner_line(w, _color(f"#{i} [{t['side']}] {t['symbol']}", Ansi.BOLD + side_color)))
            L.extend(_pair_row(w, f"Entry {t['entry']:.4f}", f"Exit {t['exit']:.4f}"))
            L.extend(_pair_row(
                w,
                f"PnL {_color(trade_pnl_str, pnl_c)}",
                f"Fees {fee:.4f}",
            ))
            L.append(_inner_line(w, f"Net {_color(f'{net:+.2f}', _pnl_color(net))}"))
            for reason_line in textwrap.wrap(f"reason: {t['reason']}", width=max(10, (w - 4) - 4 - 2)) or []:
                L.append(_inner_line(w, "  " + reason_line))
    else:
        L.append(_inner_line(w, _color("none", Ansi.GRAY)))
    L.append(_inner_bottom(w))

    L.append(_outer_bottom(w))

    print("\n".join(L))


def display_loop():

    while True:

        try:
            display_market()
        except Exception as e:
            print(f"[Display Loop Error] {e}")

        time.sleep(PRINT_INTERVAL)


# ============================================================
# MAIN
# ============================================================

def main():

    load_persistent_state()
    print(f"Connecting to {WS_URL} ...")

    if not API_KEY or not SECRET_KEY:
        print(
            "[WARNING] API_KEY / SECRET_KEY not set - REST calls "
            "(watchlist + ticker) will fail. Set COINSWITCH_API_KEY "
            "and COINSWITCH_SECRET_KEY env vars, or edit the config "
            "at the top of this file."
        )

    if ORDERS_ENABLED:

        print()
        print("!" * 68)
        print("!!! LIVE TRADING IS ENABLED (ORDERS_ENABLED = True) !!!")
        print("!" * 68)
        print(f"Real MARKET orders WILL be placed at {TRADE_LEVERAGE}x leverage")
        print("with REAL money on your CoinSwitch PRO futures account.")
        print("Type YES (all caps) to continue, anything else to abort.")
        print("!" * 68)

        confirmation = input("> ").strip()

        if confirmation != "YES":
            print("Aborted. Set ORDERS_ENABLED = False to run in paper mode.")
            return

        print("Confirmed. Starting live trading...")

    try:

        sio.connect(
            WS_URL,
            namespaces=[NAMESPACE],
            socketio_path=SOCKETIO_PATH,
            transports=["websocket"],
        )

    except Exception as e:

        print(f"[Connection Error] {e}")
        return

    refresh_account_balance()
    load_hft_watchlist(force=True)

    balance_thread = threading.Thread(target=balance_poll_loop, daemon=True)
    balance_thread.start()

    scanner_thread = threading.Thread(target=hft_scanner_loop, daemon=True)
    scanner_thread.start()

    display_thread = threading.Thread(target=display_loop, daemon=True)
    display_thread.start()

    ticker_thread = threading.Thread(target=ticker_poll_loop, daemon=True)
    ticker_thread.start()

    watchlist_thread = threading.Thread(target=watchlist_refresh_loop, daemon=True)
    watchlist_thread.start()

    try:

        sio.wait()

    except KeyboardInterrupt:

        print()
        print("Stopping...")

    finally:

        try:
            sio.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    main()
