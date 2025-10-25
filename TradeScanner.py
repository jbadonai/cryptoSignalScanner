"""
This script combines BybitV2 and signalBotV2

This Version 2 of tradingBot implements both market entry @ current price and limit entry at fib 38,50 and 61%

This Version 3 include detection of order block and extra confirmation for signal
"""

import time
import json
import threading
import hmac
import hashlib
import requests
import threading

import os
import time
import pandas as pd
import requests
import numpy as np
from datetime import datetime
from dotenv import load_dotenv
import ccxt
from PIL import Image, ImageDraw, ImageFont
import io

# FOR BYBIT TRADE
BYBIT_MAINNET = "https://api.bybit.com"
BYBIT_TESTNET = "https://api-testnet.bybit.com"
RECV_WINDOW = "5000"
RESYNC_INTERVAL_MS = 300000
TIME_DRIFT_THRESHOLD_MS = 2000


# FOR SIGNAL GENWRATION
# ===================== LOAD ENV =====================
load_dotenv()
RR_RATIO = float(os.getenv("RR_RATIO", "3.0"))
STRICT_LEVEL=os.getenv("STRICT_LEVEL")

API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
# TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS")
TELEGRAM_CHAT_IDS = [cid.strip() for cid in os.getenv("TELEGRAM_CHAT_IDS", "").split(",") if cid.strip()]
SYMBOLS = os.getenv("SYMBOLS", "BTC/USDT").split(",")
TIMEFRAME = os.getenv("TIMEFRAME", "1m")
USE_PROXY = os.getenv("USE_PROXY", "false").lower() == "true"
LOOP_INTERVAL = int(os.getenv("LOOP_INTERVAL", "60"))
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
PIVOT_LEFT = int(os.getenv("PIVOT_LEFT", "3"))
PIVOT_RIGHT = int(os.getenv("PIVOT_RIGHT", "3"))
MIN_BREAK_MULT = float(os.getenv("MIN_BREAK_MULT", "0.0"))
ORDER_VALUE_USDT = float(os.getenv("ORDER_VALUE_USDT", "0.0"))
PROXY_ADDR = os.getenv("PROXY_ADDR", "socks5h://127.0.0.1:1080")
FILTER_SIGNALS = os.getenv("FILTER_SIGNALS", "true").lower() == "true"
AUTO_TRADE = os.getenv("AUTO_TRADE", "true").lower() == "true"
PLACE_TRADE_IN_THREAD = os.getenv("PLACE_TRADE_IN_THREAD", "true").lower() == "true"
ENTRY_MODE = os.getenv("ENTRY_MODE", "CURRENT_PRICE").upper()
USE_ORDER_BLOCK = os.getenv("USE_ORDER_BLOCK", "false").lower() == "true"
AUTO_SET_ORDER_VALUE = os.getenv("AUTO_SET_ORDER_VALUE", "false").lower() == "true"


# Trend Detector Config
EMA_LENGTH = int(os.getenv("EMA_LENGTH", "50"))
ADX_LENGTH = int(os.getenv("ADX_LENGTH", "14"))
ADX_THRESHOLD = float(os.getenv("ADX_THRESHOLD", "20"))

SCOPE = None

# ===================== EXCHANGE =====================

if USE_PROXY:
    session = requests.Session()
    session.trust_env = False
    session.proxies = {'http': PROXY_ADDR, 'https': PROXY_ADDR}
    EXCHANGE = ccxt.binance({"session": session, "enableRateLimit": True})
else:
    EXCHANGE = ccxt.binance()

signal_data = {}

class BybitTradeManager:
    def __init__(self, api_key: str, api_secret: str, use_testnet: bool = True):
        self.api_key = api_key.strip()
        self.api_secret = api_secret.strip()
        self.base_url = BYBIT_TESTNET if use_testnet else BYBIT_MAINNET
        self._time_offset = 0
        self.sync_server_time()
        threading.Thread(target=self._auto_resync, daemon=True).start()

    # ──────────────────────────────
    # 📡 Utility methods
    # ──────────────────────────────
    def sync_server_time(self):
        try:
            resp = requests.get(self.base_url + "/v5/public/time", timeout=30)
            data = resp.json()
            server_time = int(data.get('result', {}).get('time') or data.get('time'))
            local_time = int(time.time() * 1000)
            offset = server_time - local_time
            if abs(offset - self._time_offset) > TIME_DRIFT_THRESHOLD_MS:
                self._time_offset = offset
        except Exception:
            pass

    def _auto_resync(self):
        while True:
            time.sleep(RESYNC_INTERVAL_MS / 1000)
            self.sync_server_time()

    def _now_ms(self):
        return str(int(time.time() * 1000 + self._time_offset))

    def _sign(self, timestamp: str, body_or_query: str):
        prehash = timestamp + self.api_key + RECV_WINDOW + body_or_query
        return hmac.new(self.api_secret.encode(), prehash.encode(), hashlib.sha256).hexdigest()

    def _headers(self, signature: str, timestamp: str):
        return {
            'Content-Type': 'application/json',
            'X-BAPI-API-KEY': self.api_key,
            'X-BAPI-TIMESTAMP': timestamp,
            'X-BAPI-RECV-WINDOW': RECV_WINDOW,
            'X-BAPI-SIGN': signature
        }

    def _private_post(self, path: str, body: dict):
        body_str = json.dumps(body, separators=(',', ':'))
        ts = self._now_ms()
        sig = self._sign(ts, body_str)
        headers = self._headers(sig, ts)
        return requests.post(self.base_url + path, headers=headers, data=body_str, timeout=30).json()

    def _private_get(self, path: str, params: dict = None):
        params = params or {}
        query_items = sorted(params.items())
        query_str = '&'.join([f"{k}={v}" for k, v in query_items])
        ts = self._now_ms()
        sig = self._sign(ts, query_str)
        headers = self._headers(sig, ts)
        url = self.base_url + path
        if query_str:
            url += '?' + query_str
        return requests.get(url, headers=headers, timeout=30).json()

    def safe_post(self, func, *args, retries=3, timeout=30, **kwargs):
        """
        Retry wrapper for API calls with timeout and connection reset handling.
        """
        for attempt in range(retries):
            try:
                return func(*args, timeout=timeout, **kwargs)
            except (requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError,
                    ConnectionResetError) as e:
                if attempt < retries - 1:
                    wait_time = 2 ** attempt
                    print(f"[Network Error] {e} | Retrying in {wait_time}s... (Attempt {attempt + 1}/{retries})")
                    time.sleep(wait_time)
                else:
                    raise

    # ──────────────────────────────
    # 📊 API Wrappers
    # ──────────────────────────────
    def get_instrument_info(self, category: str, symbol: str):
        return requests.get(
            f"{self.base_url}/v5/market/instruments-info?category={category}&symbol={symbol}", timeout=30
        ).json()

    def get_market_price(self, category: str, symbol: str):
        resp = requests.get(
            f"{self.base_url}/v5/market/tickers?category={category}&symbol={symbol}", timeout=30
        ).json()
        return float(resp.get('result', {}).get('list', [{}])[0].get('lastPrice', 0))

    def get_lot_size_filter(self, category: str, symbol: str):
        resp = self.get_instrument_info(category, symbol)
        data = resp.get('result', {}).get('list', [{}])[0]
        return data.get('lotSizeFilter', {})

    def set_leverage(self, category: str, symbol: str, buy_leverage: int, sell_leverage: int):
        path = "/v5/position/set-leverage"
        body = {
            "category": category,
            "symbol": symbol,
            "buyLeverage": str(buy_leverage),
            "sellLeverage": str(sell_leverage),
        }
        return self._private_post(path, body)

    def create_order(self, category, symbol, side, order_type, qty, price=None, stop_loss=None, take_profit=None):
        path = "/v5/order/create"
        position_idx = 1 if side == "Buy" else 2
        body = {
            "category": category,
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "timeInForce": "GTC",
            "qty": str(qty),
            "positionIdx": position_idx
        }
        if price and order_type.lower() == 'limit':
            body["price"] = str(price)
        if stop_loss:
            body["stopLoss"] = str(stop_loss)
        if take_profit:
            body["takeProfit"] = str(take_profit)
        return self._private_post(path, body)

    # ──────────────────────────────
    # 🧠 High-level trade function
    # ──────────────────────────────
    def place_trade(
        self,
        symbol: str,
        category: str,
        side: str,
        entry_type: str,
        order_value: float,
        use_max_leverage: bool = True,
        leverage: int = None,
        price: float = None,
        stop_loss: float = None,
        take_profit: float = None
    ):
        """
        Place a trade on Bybit.

        Parameters
        ----------
        symbol : str
            e.g. "BTCUSDT"
        category : str
            "linear", "inverse", or "spot"
        side : str
            "Buy" or "Sell"
        entry_type : str
            "Market" or "Limit"
        order_value : float
            Value in USDT
        use_max_leverage : bool
            If True, will detect and use max leverage
        leverage : int
            Custom leverage if use_max_leverage is False
        price : float
            Required if entry_type = "Limit"
        stop_loss : float, optional
        take_profit : float, optional

        Returns
        -------
        dict : API response
        """
        # Handle leverage
        print("Placing trade in the manager....")
        autoOrderValue = 0,0
        try:
            if category != "spot":
                if use_max_leverage:
                    instr = self.get_instrument_info(category, symbol)
                    lev_filter = instr.get('result', {}).get('list', [{}])[0].get('leverageFilter', {})
                    max_lev = int(float(lev_filter.get('maxLeverage', 100)))
                    lev_resp = self.set_leverage(category, symbol, max_lev, max_lev)
                    autoOrderValue = max_lev
                else:
                    if not leverage:
                        raise ValueError("Leverage required when not using max leverage")
                    lev_resp = self.set_leverage(category, symbol, leverage, leverage)
            else:
                lev_resp = {"note": "Spot trading, no leverage applied"}

            # Quantity calculation
            lot_size_filter = self.get_lot_size_filter(category, symbol)
            min_qty = float(lot_size_filter.get('minOrderQty', 0))
            max_qty = float(lot_size_filter.get('maxOrderQty', float('inf')))
            qty_step = float(lot_size_filter.get('qtyStep', 0.001))

            if entry_type.lower() == "market":
                market_price = self.get_market_price(category, symbol)
                if market_price <= 0:
                    return {"error": "Failed to fetch market price"}
                if AUTO_SET_ORDER_VALUE:
                    qty = autoOrderValue / market_price
                else:
                    qty = order_value / market_price
            else:
                if not price:
                    return {"error": "Price required for limit order"}

                if AUTO_SET_ORDER_VALUE:
                    qty = autoOrderValue / price
                else:
                    qty = order_value / price

            # Clamp quantity to step
            qty = round(round(qty / qty_step) * qty_step, 6)

            if qty < min_qty:
                return {"error": f"Quantity {qty} below minimum {min_qty}"}
            if qty > max_qty:
                return {"error": f"Quantity {qty} exceeds maximum {max_qty}"}

            # Place the order
            order_resp = self.create_order(
                category=category,
                symbol=symbol,
                side=side,
                order_type=entry_type,
                qty=qty,
                price=price,
                stop_loss=stop_loss,
                take_profit=take_profit
            )

            return {
                "leverage_response": lev_resp,
                "order_response": order_resp,
                "calculated_qty": qty
            }
        except Exception as e:
            print(f"An error occurred in Place Trade function: {e}")


"""
# example_trade.py
from bybit_trade_manager import BybitTradeManager
"""

# initialize bot that auto place detected signal on bybit
bot = BybitTradeManager(API_KEY, API_SECRET, use_testnet=False)

def place_order(symbol, category, side, entry_type, order_value, use_max_leverage, stop_loss, take_profit, price):
    # def place_order():
    """
    result = bot.place_trade(
            symbol="AVAXUSDT",
            category="linear",
            side="Sell",
            entry_type="Market",
            order_value=20,  # in USDT
            use_max_leverage=True,
            stop_loss=27.3764,
            take_profit=26.3100
        )"""
    try:
        result = bot.place_trade(
                symbol=symbol,
                category=category,
                side=side,
                entry_type=entry_type,
                order_value=order_value,  # in USDT
                use_max_leverage=use_max_leverage,
                price=price,
                stop_loss=stop_loss,
                take_profit=take_profit
            )
        print()
        print("<**************************************************************************>")
        print(result)
        print("<**************************************************************************>")
        print()
    except Exception as e:
        print(f"An error occurred in Place Trade function: {e}")

# ===================== HELPERS =====================
def fetch_ohlcv(symbol, timeframe, limit=300):
    data = EXCHANGE.fetch_ohlcv(symbol, timeframe, limit=limit)
    df = pd.DataFrame(data, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
    df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
    return df

def detect_pip_size(price):
    s = f"{price:.10f}".rstrip('0')
    decimals = len(s.split('.')[1]) if '.' in s else 0
    if decimals >= 4:
        return 0.0001
    elif decimals == 3:
        return 0.001
    elif decimals == 2:
        return 0.01
    else:
        return 0.0001

def send_telegram_message(msg):
    proxies = {"http": PROXY_ADDR, "https": PROXY_ADDR} if USE_PROXY else None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for chat_id in TELEGRAM_CHAT_IDS:
        payload = {"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}
        try:
            r = requests.post(url, data=payload, proxies=proxies, timeout=30)
            if r.status_code != 200:
                print(f"⚠️ Telegram error for {chat_id}: {r.status_code} {r.text}")
        except Exception as e:
            print(f"⚠️ Telegram exception for {chat_id}: {e}")

def send_telegram_photo(image_bytes, caption=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    image_data = image_bytes.getvalue()  # 💾 store the raw bytes once

    for chat_id in TELEGRAM_CHAT_IDS:
        files = {"photo": ("header.png", image_data)}  # new file for each send
        data = {"chat_id": chat_id}
        if caption:
            data["caption"] = caption
            data["parse_mode"] = "HTML"
        try:
            r = requests.post(url, data=data, files=files, timeout=30)
            if r.status_code != 200:
                print(f"⚠️ Telegram photo error for {chat_id}: {r.status_code} {r.text}")
        except Exception as e:
            print(f"⚠️ Telegram photo exception for {chat_id}: {e}")


# ===================== RMA =====================

def rma(series: pd.Series, length: int) -> pd.Series:
    alpha = 1 / length
    result = [series.iloc[0]]
    for i in range(1, len(series)):
        prev = result[-1]
        curr = alpha * series.iloc[i] + (1 - alpha) * prev
        result.append(curr)
    return pd.Series(result, index=series.index)


# ===================== BUYER vs SELLER CONTROL — Optimized =====================
def buyer_seller_control(df, len_candle=50, len_struct=100, len_sr=200, len_mom=50,
                         w_candle=0.3, w_struct=0.25, w_sr=0.2, w_mom=0.2, w_vol=0.05):
    df = df.copy().reset_index(drop=True)

    # Normalize weights
    wsum = w_candle + w_struct + w_sr + w_mom + w_vol
    w_candle /= wsum;
    w_struct /= wsum;
    w_sr /= wsum;
    w_mom /= wsum;
    w_vol /= wsum

    # ------------------- 1) Candlestick aggression -------------------
    df_candle = df.iloc[-len_candle:]
    rng = (df_candle['high'] - df_candle['low']).replace(0, 1e-10)
    body = (df_candle['close'] - df_candle['open']).abs()
    body_prop = body / rng
    close_pos = np.where(df_candle['close'] > df_candle['open'],
                         (df_candle['close'] - df_candle['low']) / rng,
                         (df_candle['high'] - df_candle['close']) / rng)
    bull_mask = df_candle['close'] > df_candle['open']
    sum_bull = (body_prop * close_pos * bull_mask).sum()
    sum_bear = (body_prop * close_pos * (~bull_mask)).sum()
    tot_candle = sum_bull + sum_bear
    candle_bull = sum_bull / tot_candle if tot_candle != 0 else 0.5

    # ------------------- 2) Market Structure -------------------
    highs = df['high']
    lows = df['low']
    ph = (highs.rolling(5, center=True).apply(lambda x: x[2] == max(x), raw=True) == 1)
    pl = (lows.rolling(5, center=True).apply(lambda x: x[2] == min(x), raw=True) == 1)

    ph_idx = np.where(ph)[0]
    pl_idx = np.where(pl)[0]

    struct_bull_count = 0
    struct_bear_count = 0
    for idxs, prices in [(ph_idx, highs), (pl_idx, lows)]:
        last = None
        for i in idxs[-len_struct:]:
            curr = prices.iloc[i]
            if last is not None:
                if curr > last:
                    struct_bull_count += 1
                else:
                    struct_bear_count += 1
            last = curr
    tot_struct = struct_bull_count + struct_bear_count
    struct_bull = struct_bull_count / tot_struct if tot_struct != 0 else 0.5

    # ------------------- 3) Support/Resistance Reaction -------------------
    sr_df = df.iloc[-len_sr:]
    high_max = sr_df['high'].rolling(len_sr).max()
    low_min = sr_df['low'].rolling(len_sr).min()

    sr_bull_points = ((sr_df['high'] >= high_max) & (sr_df['close'] >= sr_df['high'])).sum() \
                     + ((sr_df['low'] <= low_min) & (sr_df['close'] > sr_df['low'])).sum()
    sr_bear_points = ((sr_df['high'] >= high_max) & (sr_df['close'] < sr_df['high'])).sum() \
                     + ((sr_df['low'] <= low_min) & (sr_df['close'] <= sr_df['low'])).sum()
    sr_tests = sr_bull_points + sr_bear_points
    sr_bull = sr_bull_points / sr_tests if sr_tests != 0 else 0.5

    # ------------------- 4) Momentum -------------------
    delta = df['close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(len_mom).mean()
    avg_loss = loss.rolling(len_mom).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    rsi_norm = ((rsi.iloc[-1] - 50) / 50) if not np.isnan(rsi.iloc[-1]) else 0

    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    macd_hist = (ema12 - ema26) - (ema12 - ema26).ewm(span=9, adjust=False).mean()
    atr_val = (df['high'] - df['low']).rolling(14).mean().iloc[-1]
    macd_norm = macd_hist.iloc[-1] / (atr_val if atr_val != 0 else 1)

    mom_raw = rsi_norm * 0.6 + macd_norm * 0.4
    mom_bull = mom_raw if mom_raw > 0 else 0
    mom_bear = -mom_raw if mom_raw < 0 else 0
    mom_tot = mom_bull + mom_bear
    mom_bull = mom_bull / mom_tot if mom_tot != 0 else 0.5

    # ------------------- 5) Volume -------------------
    vol_bull = df_candle.loc[df_candle['close'] > df_candle['open'], 'volume'].sum()
    vol_bear = df_candle.loc[df_candle['close'] < df_candle['open'], 'volume'].sum()
    tot_vol = vol_bull + vol_bear
    vol_bull = vol_bull / tot_vol if tot_vol != 0 else 0.5

    # ------------------- Composite -------------------
    bull_score_raw = candle_bull * w_candle + struct_bull * w_struct + sr_bull * w_sr + mom_bull * w_mom + vol_bull * w_vol
    bear_score_raw = (1 - candle_bull) * w_candle + (1 - struct_bull) * w_struct + (1 - sr_bull) * w_sr + (
                1 - mom_bull) * w_mom + (1 - vol_bull) * w_vol
    tot_final = bull_score_raw + bear_score_raw
    bull_pct = 100 * bull_score_raw / tot_final if tot_final != 0 else 50
    bear_pct = 100 * bear_score_raw / tot_final if tot_final != 0 else 50

    if bull_pct > bear_pct + 5:
        return {"control": "Buyer Control", "ratio": bull_pct / 100, "icon": "🟢"}
    elif bear_pct > bull_pct + 5:
        return {"control": "Seller Control", "ratio": bear_pct / 100, "icon": "🔴"}
    else:
        return {"control": "Neutral", "ratio": 0.5, "icon": "⚪"}


# ===================== TREND DETECTOR =====================
def detect_trend(df: pd.DataFrame, ema_length=EMA_LENGTH, adx_length=ADX_LENGTH, adx_threshold=ADX_THRESHOLD):
    if len(df) < ema_length + adx_length + 5:
        return {"trend": "Unknown", "icon": "⚪"}

    df = df.iloc[:-1].copy()  # Exclude live candle
    high = df['high'];
    low = df['low'];
    close = df['close']

    # === EMA ===
    ema = close.ewm(span=ema_length, adjust=False).mean()
    ema_diff = ema - ema.shift(1)
    ema_slope = ema_diff.rolling(window=3).mean()

    # === ADX ===
    up_move = high - high.shift(1)
    down_move = low.shift(1) - low
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    trur = rma(tr, adx_length)
    plus_di = 100 * rma(pd.Series(plus_dm, index=df.index), adx_length) / trur
    minus_di = 100 * rma(pd.Series(minus_dm, index=df.index), adx_length) / trur
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di))
    adx = rma(dx.fillna(0), adx_length)

    last_close = close.iloc[-1]
    last_ema = ema.iloc[-1]
    last_slope = ema_slope.iloc[-1]
    last_adx = adx.iloc[-1]

    is_uptrend = last_close > last_ema and last_slope > 0 and last_adx > adx_threshold
    is_downtrend = last_close < last_ema and last_slope < 0 and last_adx > adx_threshold
    is_ranging = not is_uptrend and not is_downtrend

    trend_label = "Uptrend" if is_uptrend else "Downtrend" if is_downtrend else "Ranging"
    trend_icon = "🟢" if is_uptrend else "🔴" if is_downtrend else "⚪"

    bs = buyer_seller_control(df)

    return {
        "trend": trend_label,
        "icon": trend_icon,
        "ema": last_ema,
        "ema_slope": last_slope,
        "adx": last_adx,
        "is_uptrend": is_uptrend,
        "is_downtrend": is_downtrend,
        "is_ranging": is_ranging,
        "buyer_seller": bs
    }

def calculate_fibonacci_levels(signal_price: float, current_price: float, signal_side: str):
    """
    Calculate Fibonacci retracement levels (38.2%, 50%, 61.8%)
    between signal (protected level) and current price.
    LIMIT1 = 38.2%, LIMIT2 = 50%, LIMIT3 = 61.8%
    Always returns levels in correct order for both long and short.
    """
    high = max(signal_price, current_price)
    low = min(signal_price, current_price)

    # Calculate fib levels from low to high (standard)
    fib_382 = low + (high - low) * 0.382
    fib_50  = low + (high - low) * 0.5
    fib_618 = low + (high - low) * 0.618

    if signal_side.lower() == 'sell':
        # For shorts, the 38.2% retracement is *closer to the top (high)*,
        # so we need to invert the order to match LIMIT mapping
        return fib_382, fib_50, fib_618
    else:
        # For longs, 38.2% is closer to bottom (low)
        return fib_618, fib_50, fib_382  # Limit1 = 38.2% down from top


# ===================== TELEGRAM MESSAGE =====================
def format_telegram_message(pair, signal_type, detected_price, detected_time, current_price, atr, df, ob):
    global SCOPE
    pip = detect_pip_size(detected_price)

    trend_info = detect_trend(df)
    trend_label = trend_info["trend"]
    trend_icon = trend_info["icon"]
    bs = trend_info["buyer_seller"]
    bs_icon = bs["icon"]
    bs_label = bs["control"]

    if signal_type == "Protected High":
        trade_type = "Short"
        trade_side = "Sell"
        stop_loss = detected_price + atr
        risk = stop_loss - current_price
        take_profit = current_price - (risk * RR_RATIO)
    else:
        trade_type = "Long"
        trade_side = "Buy"
        stop_loss = detected_price - atr
        risk = current_price - stop_loss
        take_profit = current_price + (risk * RR_RATIO)

    fib_38, fib_50, fib_61 = calculate_fibonacci_levels(
        signal_price=detected_price,
        current_price=current_price,
        signal_side=trade_side
    )

    # store in signal data
    signal_data['fib_38'] = fib_38
    signal_data['fib_50'] = fib_50
    signal_data['fib_61'] = fib_61

    if trade_side.lower() == 'buy':
        fib_msg = (
            f"  LIMIT ENTRY 1 (38.2%): `{signal_data['fib_38']}`\n"
            f"  LIMIT ENTRY 2 (50.0%): `{signal_data['fib_50']}`\n"
            f"  LIMIT ENTRY 3 (61.8%): `{signal_data['fib_61']}`\n"
        )
    else:  # sell signal
        fib_msg = (
            f"  LIMIT ENTRY 3 (61.8%): `{signal_data['fib_61']}`\n"
            f"  LIMIT ENTRY 2 (50.0%): `{signal_data['fib_50']}`\n"
            f"  LIMIT ENTRY 1 (38.2%): `{signal_data['fib_38']}`\n"

        )

    """
    sample ob data
      return {
                        "type": "Bearish OB",
                        "index": sidx,
                        "price": float((ob['top'] + ob['bottom']) / 2.0),
                        "top": float(ob['top']),
                        "bottom": float(ob['bottom']),
                        "meta": ob
                    }
    """

    if ob:
        msg = (
            f"<pre><b>📢 NEW SIGNAL DETECTED - {TIMEFRAME}</b></pre>\n"
            f"<b>PAIR: </b> <i>🪙 {pair} 🪙</i>\n"
            f"<b>{signal_type} :  </b> <i>@{detected_price:.4f} </i>\n"
            f"ORDER BLOCK: {ob['type']} @ {ob['price']:.4f} \n\n"
            # f"<b>Detected:</b> <i>{detected_price:.4f} ({detected_time})</i>\n\n"
            # f"<pre><b>Market Context</b></pre>\n"
            # f"===============\n"
            # f"<b>TREND: </b> <i>{trend_icon} {trend_label}</i>\n"
            # f"<b>CONTROL: </b> <i>{bs_icon} {bs_label}</i>\n\n"
            f"<pre><b>Trade Setup</b></pre>\n"
            # f"===============\n"
            # f"*Active Entry Mode*: `{ENTRY_MODE}`\n"
            f"<b>SIGNAL:</b> <i>{trade_type}</i>\n"
            f"<b>MARKET ENTRY:</b> <i>{current_price:.4f}</i>\n"
            f"{fib_msg}\n"
            f"<b>STOP LOSS:</b> <i>{stop_loss:.4f}</i>\n"
            f"<b>TAKE PROFIT:</b> <i>{take_profit:.4f}</i>\n\n"
            f"lines: {detected_price:.4f},{current_price:.4f}, {stop_loss:.4f},{take_profit:.4f}\n"
            f"ob box: {ob['top']:.4f},{ob['bottom']:.4f}, \n\n\n"
            f"<i>@jbadonaiventures V3</i>"
        )
    else:
        msg = (
            f"<pre><b>📢 NEW SIGNAL DETECTED - {TIMEFRAME}</b></pre>\n"
            f"<b>PAIR: </b> <i>🪙 {pair} 🪙</i>\n"
            f"<b>{signal_type} : </b> <i> {detected_price:.4f} </i>\n"
            f"ORDER BLOCK: NONE \n\n"
            # f"<b>Detected:</b> <i>{detected_price:.4f} ({detected_time})</i>\n\n"
            # f"<pre><b>Market Context</b></pre>\n"
            # f"===============\n"
            # f"<b>TREND: </b> <i>{trend_icon} {trend_label}</i>\n"
            # f"<b>CONTROL: </b> <i>{bs_icon} {bs_label}</i>\n\n"
            f"<pre><b>Trade Setup</b></pre>\n"
            # f"===============\n"
            # f"*Active Entry Mode*: `{ENTRY_MODE}`\n"
            f"<b>SIGNAL:</b> <i>{trade_type}</i>\n"
            f"<b>MARKET ENTRY:</b> <i>{current_price:.4f}</i>\n"
            f"{fib_msg}\n"
            f"<b>STOP LOSS:</b> <i>{stop_loss:.4f}</i>\n"
            f"<b>TAKE PROFIT:</b> <i>{take_profit:.4f}</i>\n\n"
            f"lines: {detected_price:.4f},{current_price:.4f}, {stop_loss:.4f},{take_profit:.4f}\n\n\n"
            f"<i>@jbadonaiventures v3</i>"
        )


    if ENTRY_MODE == "CURRENT_PRICE":
        entry_type = "Market"
        entry_price = current_price
    elif ENTRY_MODE == "AUTO":
        if SCOPE == "Continuation":
            entry_type = "Market"
            entry_price = current_price
        else:
            entry_type = "Limit"
            entry_price = signal_data['fib_38']
    elif ENTRY_MODE == "LIMIT1":
        entry_type = "Limit"
        entry_price = signal_data['fib_38']
    elif ENTRY_MODE == "LIMIT2":
        entry_type = "Limit"
        entry_price = signal_data['fib_50']
    elif ENTRY_MODE == "LIMIT3":
        entry_type = "Limit"
        entry_price = signal_data['fib_61']
    else:
        entry_type = "Market"
        entry_price = current_price

    # Round to tick size if needed
    # tick_size = signal_data.get("tick_size", 4)
    # entry_price = round(entry_price, tick_size)

    if AUTO_TRADE:

        if USE_ORDER_BLOCK:
            # if use order block is set to true, place trade only if order block is found near protected
            if ob:
                if not PLACE_TRADE_IN_THREAD:
                    place_order(symbol=pair, category="linear", side=trade_side, entry_type="Market", order_value=ORDER_VALUE_USDT,  use_max_leverage=True, stop_loss=stop_loss, take_profit=take_profit, price=entry_price)
                else:
                    # """
                    threading.Thread(
                        target=place_order,
                        kwargs={
                            "symbol": pair,
                            "category": "linear",
                            "side": trade_side,
                            "entry_type": entry_type,
                            "order_value": ORDER_VALUE_USDT,
                            "use_max_leverage": True,
                            "stop_loss": stop_loss,
                            "take_profit": take_profit,
                            "price": entry_price
                        },
                        daemon=True  # ✅ this allows the thread to close automatically when your script exits
                    ).start()
                    # """
            else:
                print("Trade Placing Skipped. USE ORDER BLOCK is enforced. but no order block around protected")
        else:
            # if use order block is set to false. do not consider order block near protected before placing trade
            if not PLACE_TRADE_IN_THREAD:
                place_order(symbol=pair, category="linear", side=trade_side, entry_type="Market",
                            order_value=ORDER_VALUE_USDT, use_max_leverage=True, stop_loss=stop_loss,
                            take_profit=take_profit, price=entry_price)
            else:
                # """
                threading.Thread(
                    target=place_order,
                    kwargs={
                        "symbol": pair,
                        "category": "linear",
                        "side": trade_side,
                        "entry_type": entry_type,
                        "order_value": ORDER_VALUE_USDT,
                        "use_max_leverage": True,
                        "stop_loss": stop_loss,
                        "take_profit": take_profit,
                        "price": entry_price
                    },
                    daemon=True  # ✅ this allows the thread to close automatically when your script exits
                ).start()
                # """
    else:
        print("Trade Placing Skipped. AUTO TRADE = False")

    return msg

 # ===================== HEADER IMAGE GENERATOR =====================


def create_header_image(text="📢 NEW SIGNAL DETECTED", bg_color=(0, 102, 255), text_color=(255, 255, 255)):
    # Load font
    font_size = 60
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
    except:
        font = ImageFont.load_default()

    # Estimate text size
    dummy_img = Image.new("RGB", (1, 1))
    draw = ImageDraw.Draw(dummy_img)
    text_bbox = draw.textbbox((0, 0), text, font=font)
    text_w = text_bbox[2] - text_bbox[0]
    text_h = text_bbox[3] - text_bbox[1]

    # Add padding around text
    padding_x = 40
    padding_y = 20
    width = text_w + padding_x * 2
    height = text_h + padding_y * 2

    # Create final image
    img = Image.new("RGB", (width, height), color=bg_color)
    draw = ImageDraw.Draw(img)
    position = (padding_x, padding_y)
    draw.text(position, text, fill=text_color, font=font)

    # Convert to bytes
    img_bytes = io.BytesIO()
    img.save(img_bytes, format="PNG")
    img_bytes.seek(0)

    return img_bytes


# ===================== PROTECTED LEVEL DETECTOR =====================
class ProtectedLevelDetector:
    def __init__(self, left=3, right=3, atr_length=14, min_break_mult=0.0):
        self.left = left
        self.right = right
        self.atr_length = atr_length
        self.min_break_mult = min_break_mult
        self.last_ph_price = None
        self.last_ph_idx = None
        self.last_pl_price = None
        self.last_pl_idx = None
        self.protected_highs = []
        self.protected_lows = []

    def _calculate_atr(self, df):
        high = df['high']
        low = df['low']
        close = df['close']
        tr = pd.concat([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)
        return tr.rolling(self.atr_length).mean().iloc[-1]

    def _detect_pivots(self, df):
        highs = df['high'].values
        lows = df['low'].values
        ph_price, ph_idx, pl_price, pl_idx = None, None, None, None
        for i in range(self.left, len(df) - self.right):
            if highs[i] == max(highs[i - self.left:i + self.right + 1]): ph_price, ph_idx = highs[i], i
            if lows[i] == min(lows[i - self.left:i + self.right + 1]): pl_price, pl_idx = lows[i], i
        return ph_price, ph_idx, pl_price, pl_idx

    def _update_state(self, ph_price, ph_idx, pl_price, pl_idx, close, atr):
        if ph_price is not None: self.last_ph_price, self.last_ph_idx = ph_price, ph_idx
        if pl_price is not None: self.last_pl_price, self.last_pl_idx = pl_price, pl_idx
        if self.last_ph_price and self.last_pl_price and self.last_pl_idx < self.last_ph_idx and close > self.last_ph_price:
            if self.min_break_mult <= 0 or (close - self.last_ph_price) >= self.min_break_mult * atr:
                if not any(pl['index'] == self.last_pl_idx for pl in self.protected_lows):
                    self.protected_lows.append({'price': self.last_pl_price, 'index': self.last_pl_idx})
        if self.last_ph_price and self.last_pl_price and self.last_ph_idx < self.last_pl_idx and close < self.last_pl_price:
            if self.min_break_mult <= 0 or (self.last_pl_price - close) >= self.min_break_mult * atr:
                if not any(ph['index'] == self.last_ph_idx for ph in self.protected_highs):
                    self.protected_highs.append({'price': self.last_ph_price, 'index': self.last_ph_idx})

    def _remove_mitigated(self, high, low):
        mitigated = None
        self.protected_lows = [pl for pl in self.protected_lows if not (low < pl['price'] and (
            mitigated := {'type': 'Protected Low', 'price': pl['price'], 'index': pl['index']}))]
        self.protected_highs = [ph for ph in self.protected_highs if not (high > ph['price'] and (
            mitigated := {'type': 'Protected High', 'price': ph['price'], 'index': ph['index']}))]
        if mitigated and 'type' not in mitigated:
            mitigated['type'] = 'Unknown'
        return mitigated

    def _latest_signal(self):
        if not self.protected_highs and not self.protected_lows: return None
        latest_high = max(self.protected_highs, key=lambda x: x['index']) if self.protected_highs else None
        latest_low = max(self.protected_lows, key=lambda x: x['index']) if self.protected_lows else None
        signal = None
        if latest_high and latest_low:
            signal = latest_high if latest_high['index'] > latest_low['index'] else latest_low
        else:
            signal = latest_high or latest_low
        if signal and 'type' not in signal:
            if signal in self.protected_highs:
                signal['type'] = 'Protected High'
            elif signal in self.protected_lows:
                signal['type'] = 'Protected Low'
            else:
                signal['type'] = 'Unknown'
        return signal

    def process(self, df):
        ph_price, ph_idx, pl_price, pl_idx = self._detect_pivots(df)
        atr = self._calculate_atr(df)
        close, high, low = df['close'].iloc[-1], df['high'].iloc[-1], df['low'].iloc[-1]
        self._update_state(ph_price, ph_idx, pl_price, pl_idx, close, atr)
        mitigated = self._remove_mitigated(high, low)
        signal = self._latest_signal()
        if signal: signal['time'] = df['ts'].iloc[signal['index']].strftime('%Y-%m-%d %H:%M:%S UTC')
        if mitigated: mitigated['time'] = df['ts'].iloc[mitigated['index']].strftime('%Y-%m-%d %H:%M:%S UTC')
        return signal, mitigated, atr, close

class OrderBlockDetector:
    """
    Robust 1:1 Python port of the TradingView Order Block logic with:
      - swing detection
      - OB creation
      - breaker / invalidation logic (Wick-based)
      - combination of overlapping OBs
      - find_ob_near_signal() to check OB existence near a protected pivot
    Simple print() logging when debug=True.
    """

    def __init__(self, swing_length=10, max_atr_mult=3.5, max_order_blocks=30, overlap_threshold=0.0, debug=False):
        self.swing_length = int(swing_length)
        self.max_atr_mult = float(max_atr_mult)
        self.max_order_blocks = int(max_order_blocks)
        self.overlap_threshold = float(overlap_threshold)
        self.debug = bool(debug)
        self.last_result = None

    def _log(self, *args):
        if self.debug:
            print("[OrderBlockDetector]", *args)

    def _calculate_atr(self, df, period=10):
        # Standard ATR (rolling mean of TR)
        high_low = df['high'] - df['low']
        high_close = np.abs(df['high'] - df['close'].shift(1))
        low_close = np.abs(df['low'] - df['close'].shift(1))
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.rolling(period, min_periods=1).mean()  # min_periods=1 -> avoids all-NaN; we still skip early bars later
        return atr

    def _find_swings(self, df):
        """
        Find swing highs/lows using window = swing_length on each side (like Pine's findOBSwings_ob).
        Returns lists of dicts: highs and lows with integer positional indexes.
        """
        highs, lows = [], []
        n = len(df)
        L = self.swing_length

        if n < (L * 2 + 1):
            return highs, lows

        # Use integer positions, not df.index values
        for i in range(L, n - L):
            window_high = df['high'].iloc[i - L: i + L + 1].max()
            window_low = df['low'].iloc[i - L: i + L + 1].min()

            if df['high'].iat[i] >= window_high:
                highs.append({'index': i, 'price': float(df['high'].iat[i]), 'volume': float(df['volume'].iat[i])})
            if df['low'].iat[i] <= window_low:
                lows.append({'index': i, 'price': float(df['low'].iat[i]), 'volume': float(df['volume'].iat[i])})
        return highs, lows

    def _safe_break_index(self, ob, df_len):
        """Return numeric break index or df_len if None."""
        bi = ob.get('break_index', None)
        return int(df_len) if bi is None else int(bi)

    def _area_and_overlap(self, a, b, df_len):
        """
        Compute intersection percentage between rectangles representing OBs.
        Each OB rectangle: x from start_index to break_index_or_len, y from bottom to top.
        Return overlap_percentage (0..100). Safe to None or zero sizes.
        """
        xa1 = int(a['start_index'])
        xa2 = self._safe_break_index(a, df_len)
        xb1 = int(b['start_index'])
        xb2 = self._safe_break_index(b, df_len)

        ya1 = float(a['top'])
        ya2 = float(a['bottom'])
        yb1 = float(b['top'])
        yb2 = float(b['bottom'])

        # compute intersection extents
        inter_x = max(0, min(xa2, xb2) - max(xa1, xb1))
        inter_y = max(0, min(ya1, yb1) - max(ya2, yb2))
        intersection_area = inter_x * inter_y

        # union area = areaA + areaB - intersection
        area_a = max(0, (xa2 - xa1)) * max(0, (ya1 - ya2))
        area_b = max(0, (xb2 - xb1)) * max(0, (yb1 - yb2))
        union_area = area_a + area_b - intersection_area

        if union_area <= 0:
            return 0.0
        return (intersection_area / union_area) * 100.0

    def _combine_order_blocks(self, all_blocks, df):
        """
        Combine overlapping OBs of same type according to overlap_threshold.
        Returns combined list.
        """
        df_len = len(df)
        combined = []
        used = [False] * len(all_blocks)

        for i, a in enumerate(all_blocks):
            if used[i]:
                continue
            merged = a.copy()
            # iterate subsequent blocks to allow symmetric merging
            for j, b in enumerate(all_blocks):
                if i == j or used[j]:
                    continue
                if a['type'] != b['type']:
                    continue
                overlap_pct = self._area_and_overlap(a, b, df_len)
                if overlap_pct > self.overlap_threshold:
                    # merge b into merged
                    merged['top'] = float(max(merged['top'], b['top']))
                    merged['bottom'] = float(min(merged['bottom'], b['bottom']))
                    merged['volume'] = float(merged.get('volume', 0) + b.get('volume', 0))
                    merged['obHighVolume'] = float(merged.get('obHighVolume', 0) + b.get('obHighVolume', 0))
                    merged['obLowVolume'] = float(merged.get('obLowVolume', 0) + b.get('obLowVolume', 0))
                    merged['combined'] = True
                    # unify start_index to earliest, break_index to latest non-None
                    merged['start_index'] = int(min(merged['start_index'], b['start_index']))
                    bi_merged = merged.get('break_index', None)
                    bi_b = b.get('break_index', None)
                    if bi_merged is None and bi_b is not None:
                        merged['break_index'] = bi_b
                    elif bi_b is None and bi_merged is not None:
                        pass
                    else:
                        # both numeric or both None -> take max numeric or leave None
                        if bi_merged is not None or bi_b is not None:
                            merged['break_index'] = int(max(bi_merged or -1, bi_b or -1))
                    used[j] = True
            combined.append(merged)
            used[i] = True
        return combined

    def process(self, df):
        """
        Detect order blocks across the provided dataframe df.
        df must contain columns: ['open','high','low','close','volume'].
        Returns dict with 'bullish', 'bearish', 'combined' lists of OB dicts.
        """
        try:
            df = df.reset_index(drop=True).copy()
            n = len(df)
            if n == 0:
                self._log("Empty dataframe passed to process()")
                return {"bullish": [], "bearish": [], "combined": []}

            atr = self._calculate_atr(df, period=10)
            highs, lows = self._find_swings(df)

            bullish_blocks = []
            bearish_blocks = []

            # loop over bars similar to Pine script (use integer positions)
            for i in range(self.swing_length, n):
                # skip early bars where ATR isn't reliable (this prevents NaN comparisons)
                atr_val = atr.iat[i]
                if pd.isna(atr_val):
                    continue

                o = float(df.at[i, 'open'])
                h = float(df.at[i, 'high'])
                l = float(df.at[i, 'low'])
                c = float(df.at[i, 'close'])
                v = float(df.at[i, 'volume'])

                # --- Handle existing bullish_blocks (breaker & removal)
                for ob in bullish_blocks[:]:  # iterate a shallow copy to allow safe removals
                    if not ob.get('breaker', False):
                        # obEndMethod = "Wick" (hard-coded): low wick invalidates bullish OB
                        if l < ob['bottom']:
                            ob['breaker'] = True
                            ob['break_index'] = i
                            ob['bbVolume'] = v
                            self._log("Bullish OB breaker set at index", i, "OB start", ob['start_index'])
                    else:
                        # once breaker set, if price later closes above top, remove OB
                        if h > ob['top']:
                            try:
                                bullish_blocks.remove(ob)
                                self._log("Bullish OB removed after break and breach at index", i)
                            except ValueError:
                                pass

                # --- Create bullish OBs based on swings (swing highs detection used as in Pine)
                for swing in highs:
                    sidx = int(swing['index'])
                    if sidx >= i:
                        break
                    # in Pine: if close > top_of_swing and not top.crossed_ob
                    if c > swing['price']:
                        # find last opposite candle (bearish candle) before the move, Pine uses prior bars logic
                        if i - 1 >= 0 and df.at[i-1, 'close'] < df.at[i-1, 'open']:
                            # in Pine they used max_ob/min_ob which can be wick or body depending on useBody flag.
                            # The Pine script uses useBody_ob = false => uses high/low (wick). But OB zone is built using previous bars body/wicks logic.
                            box_top = float(max(df.at[i-1, 'open'], df.at[i-1, 'close']))
                            box_bottom = float(min(df.at[i-1, 'open'], df.at[i-1, 'close']))
                            box_loc = i - 1
                            # Pine sums volumes of current + prev 1 + prev 2
                            vol_i_1 = float(df.at[i-1, 'volume']) if i-1 >= 0 else 0.0
                            vol_i_2 = float(df.at[i-2, 'volume']) if i-2 >= 0 else 0.0
                            ob_volume = v + vol_i_1 + vol_i_2
                            ob_low_vol = vol_i_2
                            ob_high_vol = v + vol_i_1
                            ob_size = abs(box_top - box_bottom)

                            # ensure numeric before comparison
                            if (ob_size is not None) and (not pd.isna(ob_size)) and (atr_val is not None) and (not pd.isna(atr_val)):
                                if ob_size <= float(atr_val) * self.max_atr_mult:
                                    new_ob = {
                                        'type': 'Bull',
                                        'top': box_top,
                                        'bottom': box_bottom,
                                        'volume': float(ob_volume),
                                        'start_index': int(box_loc),
                                        'breaker': False,
                                        'break_index': None,
                                        'bbVolume': 0.0,
                                        'obHighVolume': float(ob_high_vol),
                                        'obLowVolume': float(ob_low_vol),
                                        'combined': False
                                    }
                                    # insert newest at front (unshift behavior)
                                    bullish_blocks.insert(0, new_ob)
                                    if len(bullish_blocks) > self.max_order_blocks:
                                        bullish_blocks.pop()
                                    self._log("New Bullish OB created at bar", i, "from candle", box_loc)
                        # once matched to this swing, Pine sets crossed flag; we emulate by breaking to avoid duplicate OBs for same i
                        break

                # --- Handle existing bearish_blocks (breaker & removal)
                for ob in bearish_blocks[:]:
                    if not ob.get('breaker', False):
                        # obEndMethod = "Wick": if high > ob.top -> breaker
                        if h > ob['top']:
                            ob['breaker'] = True
                            ob['break_index'] = i
                            ob['bbVolume'] = v
                            self._log("Bearish OB breaker set at index", i, "OB start", ob['start_index'])
                    else:
                        # once breaker set, if price later closes below bottom, remove OB
                        if l < ob['bottom']:
                            try:
                                bearish_blocks.remove(ob)
                                self._log("Bearish OB removed after break and breach at index", i)
                            except ValueError:
                                pass

                # --- Create bearish OBs based on swings (swing lows)
                for swing in lows:
                    sidx = int(swing['index'])
                    if sidx >= i:
                        break
                    if c < swing['price']:
                        if i - 1 >= 0 and df.at[i-1, 'close'] > df.at[i-1, 'open']:
                            box_top = float(max(df.at[i-1, 'open'], df.at[i-1, 'close']))
                            box_bottom = float(min(df.at[i-1, 'open'], df.at[i-1, 'close']))
                            box_loc = i - 1
                            vol_i_1 = float(df.at[i-1, 'volume']) if i-1 >= 0 else 0.0
                            vol_i_2 = float(df.at[i-2, 'volume']) if i-2 >= 0 else 0.0
                            ob_volume = v + vol_i_1 + vol_i_2
                            # Pine sets obLowVolume/obHighVolume differently for bearish; follow same assignments
                            ob_low_vol = v + vol_i_1
                            ob_high_vol = vol_i_2
                            ob_size = abs(box_top - box_bottom)

                            if (ob_size is not None) and (not pd.isna(ob_size)) and (atr_val is not None) and (not pd.isna(atr_val)):
                                if ob_size <= float(atr_val) * self.max_atr_mult:
                                    new_ob = {
                                        'type': 'Bear',
                                        'top': box_top,
                                        'bottom': box_bottom,
                                        'volume': float(ob_volume),
                                        'start_index': int(box_loc),
                                        'breaker': False,
                                        'break_index': None,
                                        'bbVolume': 0.0,
                                        'obHighVolume': float(ob_high_vol),
                                        'obLowVolume': float(ob_low_vol),
                                        'combined': False
                                    }
                                    bearish_blocks.insert(0, new_ob)
                                    if len(bearish_blocks) > self.max_order_blocks:
                                        bearish_blocks.pop()
                                    self._log("New Bearish OB created at bar", i, "from candle", box_loc)
                        break

            all_blocks = bullish_blocks + bearish_blocks
            combined_blocks = self._combine_order_blocks(all_blocks, df)

            result = {
                "bullish": bullish_blocks,
                "bearish": bearish_blocks,
                "combined": combined_blocks
            }
            self.last_result = result
            return result

        except Exception as e:
            # helpful debug output
            print("[OrderBlockDetector] An error occurred in process function:", e)
            raise

    def find_ob_near_signal(self, df, swing_index, signal_type, lookback=5):
        """
        Check whether an OB exists within the last `lookback` bars of swing_index.
        Returns a dict with OB meta if found, otherwise None.
        Uses integer position indexes (0..len(df)-1).
        """
        if swing_index is None:
            return None

        # ensure we have latest result
        if self.last_result is None:
            self._log("Running process() automatically from find_ob_near_signal()")
            _ = self.process(df)

        # convert swing_index to integer position
        pos = int(swing_index)
        start_pos = max(pos - int(lookback), 0)
        end_pos = pos

        # Use positional comparison to match earlier simple function behavior
        if signal_type == "Protected Low":
            # look for bullish order blocks whose start_index falls in the lookback window
            for ob in self.last_result.get("bullish", []):
                sidx = int(ob['start_index'])
                if sidx >= start_pos and sidx <= end_pos:
                    return {
                        "type": "Bullish OB",
                        "index": sidx,
                        "price": float((ob['top'] + ob['bottom']) / 2.0),
                        "top": float(ob['top']),
                        "bottom": float(ob['bottom']),
                        "meta": ob
                    }
        elif signal_type == "Protected High":
            for ob in self.last_result.get("bearish", []):
                sidx = int(ob['start_index'])
                if sidx >= start_pos and sidx <= end_pos:
                    return {
                        "type": "Bearish OB",
                        "index": sidx,
                        "price": float((ob['top'] + ob['bottom']) / 2.0),
                        "top": float(ob['top']),
                        "bottom": float(ob['bottom']),
                        "meta": ob
                    }
        return None

# ===================== MAIN LOOP =====================
def main_loop():
    global SCOPE
    last_signal = {s: None for s in SYMBOLS}
    last_mitigated = {s: None for s in SYMBOLS}
    detectors = {s: ProtectedLevelDetector(left=PIVOT_LEFT, right=PIVOT_RIGHT, atr_length=ATR_PERIOD,
                                           min_break_mult=MIN_BREAK_MULT) for s in SYMBOLS}

    ob_detector = OrderBlockDetector(swing_length=3, max_atr_mult=3.5, debug=False)


    print(f"Proxy: {'ON' if USE_PROXY else 'OFF'}")
    print(f"TimeFrame: {TIMEFRAME} |  Interval: {LOOP_INTERVAL} | AUTO TRADE: {AUTO_TRADE} | Strict Level: {STRICT_LEVEL} | Entry Mode: {ENTRY_MODE} | Enforce Order Block: {USE_ORDER_BLOCK}")
    print()
    print(f"Monitoring {', '.join(SYMBOLS)} on {TIMEFRAME} timeframe...")
    print()

    while True:
        try:
            for symbol in SYMBOLS:
                df = fetch_ohlcv(symbol, TIMEFRAME)
                detector = detectors[symbol]
                signal, mitigated, atr_value, current_price = detector.process(df)

                if mitigated: last_mitigated[symbol] = mitigated

                if signal != last_signal[symbol]:
                    if signal is not None:
                        trade_type = "Short" if signal['type'] == "Protected High" else "Long"

                        if FILTER_SIGNALS:
                            trend_info = detect_trend(df)
                            trend_label = trend_info["trend"]
                            control_label = trend_info["buyer_seller"]["control"]


                            allowed_old = (
                                    (trend_label == "Downtrend" and control_label == "Seller Control") or
                                    (trend_label == "Uptrend" and control_label == "Buyer Control")
                            )

                            allowed = allow_by_strict_level(STRICT_LEVEL, trend_label, control_label, trade_type)

                            if not allowed:
                                print(f"[{symbol}] Signal filtered — trend/control/trade_type mismatch")
                                last_signal[symbol] = signal
                                continue

                        scope = continuation_or_reversal(trend_label, control_label, trade_type)
                        SCOPE = scope

                        # detecting if order block exists around the protected low or high
                        # ob = detect_order_block(df, signal["index"], signal["type"])

                        order_blocks = ob_detector.process(df)
                        # print(f"order blocks 1st principle: {order_blocks}")

                        # print(f"Detecting near order block to:  {signal['index']} -- {signal['type']}")
                        ob_near_signal = ob_detector.find_ob_near_signal(df, signal["index"], signal["type"])

                        # trying to override ob value without distrupting original context and usage of ob in case of reverting
                        # ob = ob_near_signal


                        if ob_near_signal:
                            print("Order Block near signal:", ob_near_signal)
                        else:
                            print("No OB near protected level.")

                        # prepare and format message then place trade if auto
                        msg = format_telegram_message(symbol, signal['type'], signal['price'], signal['time'],
                                                      current_price, atr_value, df, ob_near_signal)


                        # Determine banner text and color dynamically
                        if signal['type'] == "Protected High":
                            banner_text = f"SHORT SIGNAL \n{symbol} \n[ {scope} ]"
                            banner_color = (220, 20, 60)  # red
                        else:
                            banner_text = f"LONG SIGNAL \n{symbol} \n[ {scope} ]"
                            banner_color = (0, 153, 51)  # green


                        # 1. Create the image header
                        header_img = create_header_image(banner_text, bg_color=banner_color)



                        # # 🔍 NEW: Check for Order Block near swing
                        # if USE_ORDER_BLOCK:
                        #     # ob = detect_order_block(df, signal["index"], signal["type"])
                        #     if ob:
                        #         print(f"[{symbol}] ✅ Order Block found near {signal['type']} ({ob['type']} @ {ob['price']:.4f})")
                        #         # send_telegram_message(msg)
                        #         send_telegram_photo(header_img, caption=msg)
                        #     else:
                        #         print(f"[{symbol}] ❌ No Order Block near {signal['type']} — skipping signal.")
                        #         # last_signal[symbol] = signal
                        #         # continue
                        # else:
                        #     # send_telegram_message(msg)
                        #     send_telegram_photo(header_img, caption=msg)

                        if ob_near_signal:
                            print(
                                f"[{symbol}] ✅ Order Block found near {signal['type']} ({ob_near_signal['type']} @ {ob_near_signal['price']:.4f})")
                        else:
                            print(f"[{symbol}] ❌ No Order Block near {signal['type']} — skipping signal.")

                        # send_telegram_message(msg)
                        send_telegram_photo(header_img, caption=msg)

                        status = f"New {signal['type']} at {signal['price']:.4f} ({signal['time']})"
                    elif last_mitigated[symbol] is not None:
                        status = f"No signal — Last mitigated: {last_mitigated[symbol]['type']} {last_mitigated[symbol]['price']:.4f} @ {last_mitigated[symbol]['time']}"
                    else:
                        status = "No new signal"
                    last_signal[symbol] = signal
                else:
                    if signal is not None:
                        status = f"No new signal — {signal['type']} {signal['price']:.4f} @ {signal['time']}"
                    else:
                        status = "No new signal"

                print(f"[{symbol}] {status}")

            for sec in range(LOOP_INTERVAL, 0, -1):
                print(f"\rNext check in {sec}s", end="", flush=True)
                time.sleep(1)

        except KeyboardInterrupt:
            try:
                print("\n\nBot stopped by user.")
                ans = input("\nPress 'r' to restart or 'q' to quit: ")
                if ans == 'r':
                    os.system('cls')
                    main_loop()

                break
            except Exception as e:
                print(e)
                input("press any key to continue...")
                break
        except Exception as e:
            print(f"\nError: {e}")
            time.sleep(LOOP_INTERVAL)

def allow_by_strict_level(level, trend_label, control_label, trade_type):
    if level == "low": # ANY
        return (
            (trend_label == "Downtrend" and control_label == "Seller Control") or
            (trend_label == "Uptrend" and control_label == "Buyer Control")
        )
    elif level == "medium": # CAPTURES CONTINUATION
        return (
            (trend_label == "Downtrend" and control_label == "Seller Control" and trade_type == "Short") or
            (trend_label == "Uptrend" and control_label == "Buyer Control" and trade_type == "Long")
        )
    elif level == "high": # CAPTURES REVERSAL
        return (
            (trend_label == "Downtrend" and control_label == "Seller Control" and trade_type == "Long") or
            (trend_label == "Uptrend" and control_label == "Buyer Control" and trade_type == "Short")
        )
    else:
        # Fallback or log warning
        return False

def continuation_or_reversal(trend_label, control_label, trade_type):
    if trend_label == "Downtrend" and control_label == "Seller Control":
        if trade_type == "Short":
            return "Continuation"
        else:
            return "Reversal"

    elif trend_label == "Uptrend" and control_label == "Buyer Control":
        if trade_type == "Short":
            return "Reversal"
        else:
            return "Continuation"
    else:
        return ""








# ===================== RUN =====================
if __name__ == "__main__":
    main_loop()
