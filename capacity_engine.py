#!/usr/bin/env python3
"""
Capacity Engine — Structural Capacity Monitor
=====================================================
Replaces silence-only engine with full validated system:
  - W4+ crisis signal (10/11 accuracy, 8.5% FP)
  - Regime classification (UPTREND/CORRECTION/BEAR)
  - Capacity assessment (elastic/viscous/rigid)
  - Velocity indicators (the key finding)
  - Multi-detector severity (F1/F5/F6a/F6b)
  - Position sizing recommendation
  - Structural health timeseries (for dual-line chart)
  - Historical analog matching

Data: FRED API + TwelveData API + yfinance
Output: JSON (latest + history) + CSV + Supabase + email
"""

import os
import json
import time
import warnings
import smtplib
import math
from datetime import datetime, timedelta
from pathlib import Path
from email.mime.text import MIMEText

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

from dotenv import load_dotenv
load_dotenv(override=True)

TWELVEDATA_API_KEY = os.getenv('TWELVEDATA_API_KEY', '')
FRED_API_KEY = os.getenv('FRED_API_KEY', '')
SUPABASE_URL = os.getenv('SUPABASE_URL', '')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', '')
NOTIFY_EMAIL = os.getenv('NOTIFY_EMAIL', '')
GMAIL_ADDRESS = os.getenv('GMAIL_ADDRESS', '')
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD', '')

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "Data"
CACHE_FILE = SCRIPT_DIR / "twelvedata_cache.json"
JSON_LATEST = SCRIPT_DIR / "flow_data_latest.json"
JSON_HISTORY = SCRIPT_DIR / "flow_data_history.json"
JSON_HEALTH = SCRIPT_DIR / "structural_health.json"  # for the dual-line chart
CSV_FILE = SCRIPT_DIR / "data" / "flow_data.csv"
CACHE_MAX_AGE_HOURS = 12

# Terminal colors
GREEN = "\033[92m"; YELLOW = "\033[93m"; RED = "\033[91m"
BOLD = "\033[1m"; DIM = "\033[2m"; RESET = "\033[0m"


# ============================================================
# DATA FETCHING
# ============================================================
def fetch_fred_series(series_id, start_days_back=2500):
    """Fetch one FRED series via API."""
    if not FRED_API_KEY:
        return pd.Series(dtype=float)
    try:
        from fredapi import Fred
        fred = Fred(api_key=FRED_API_KEY)
        end = datetime.now()
        start = end - timedelta(days=start_days_back)
        s = fred.get_series(series_id, observation_start=start, observation_end=end)
        return s.dropna().astype(float)
    except Exception as e:
        print(f"  {RED}FRED {series_id}: {e}{RESET}")
        return pd.Series(dtype=float)

def fetch_twelvedata(symbol, start_days_back=2500):
    """Fetch one TwelveData symbol. Uses cache."""
    if not TWELVEDATA_API_KEY:
        return pd.Series(dtype=float)

    # Check cache
    cached_data = {}
    if CACHE_FILE.exists():
        age = (time.time() - CACHE_FILE.stat().st_mtime) / 3600
        if age < CACHE_MAX_AGE_HOURS:
            with open(CACHE_FILE) as f:
                cached_data = json.load(f)
            if symbol in cached_data and cached_data[symbol]:
                s = pd.Series(cached_data[symbol], dtype=float)
                s.index = pd.to_datetime(s.index)
                return s.sort_index()

    try:
        import requests
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=start_days_back)).strftime("%Y-%m-%d")
        url = (f'https://api.twelvedata.com/time_series?symbol={symbol}'
               f'&interval=1day&start_date={start}&end_date={end}'
               f'&outputsize=5000&apikey={TWELVEDATA_API_KEY}')
        r = requests.get(url, timeout=30)
        data = r.json()
        if 'values' not in data:
            print(f"  {RED}TD {symbol}: {data.get('message', 'error')}{RESET}")
            return pd.Series(dtype=float)

        records = {row['datetime']: float(row['close']) for row in data['values']}
        # Update cache
        cached_data[symbol] = records
        with open(CACHE_FILE, 'w') as f:
            json.dump(cached_data, f)

        s = pd.Series(records, dtype=float)
        s.index = pd.to_datetime(s.index)
        return s.sort_index()
    except Exception as e:
        print(f"  {RED}TD {symbol}: {e}{RESET}")
        return pd.Series(dtype=float)

def fetch_yfinance(ticker, start='2000-01-01'):
    """Fetch from yfinance as fallback."""
    try:
        import yfinance as yf
        df = yf.download(ticker, start=start, progress=False)
        if len(df) > 100:
            if hasattr(df, 'columns') and df.ndim > 1:
                close = df['Close'] if 'Close' in df.columns else df.iloc[:, 0]
                if hasattr(close, 'columns') and close.ndim > 1:
                    close = close.iloc[:, 0]
                return close.dropna()
            return df.dropna()
        return pd.Series(dtype=float)
    except:
        return pd.Series(dtype=float)


# ============================================================
# DATA LOADING (with fallbacks)
# ============================================================
def load_all_data():
    """Load all required data from APIs and local files."""
    print(f"\n{BOLD}Loading data...{RESET}")
    data = {}

    # FRED series
    print(f"  [FRED]")
    fred_map = {
        'hy_oas': 'BAMLH0A0HYM2',
        'dgs10': 'DGS10',
        'vixcls': 'VIXCLS',
        'tb3ms': 'TB3MS',
        'nfci': 'NFCI',
        'stlfsi': 'STLFSI4',
        'bbb_spread': 'BAMLC0A4CBBB',
        't10y2y': 'T10Y2Y',
        'baa_spread': 'BAAFFM',
    }
    for key, series_id in fred_map.items():
        s = fetch_fred_series(series_id)
        data[key] = s
        n = len(s)
        print(f"    {key:>15s}: {n:>5d} obs" if n > 0 else f"    {key:>15s}: FAILED")

    # TwelveData / yfinance
    print(f"  [Market data]")
    # SPX — critical, try multiple sources
    data['spx'] = fetch_yfinance('^GSPC', '1997-01-01')
    print(f"    {'SPX':>15s}: {len(data['spx']):>5d} obs")

    # VIX — fallback to FRED VIXCLS if yfinance fails
    yf_vix = fetch_yfinance('^VIX', '1997-01-01')
    data['vix'] = yf_vix if len(yf_vix) > len(data.get('vixcls', pd.Series())) else data.get('vixcls', yf_vix)
    print(f"    {'VIX':>15s}: {len(data['vix']):>5d} obs")

    # TB3M — for yield curve
    yf_tb3m = fetch_yfinance('^IRX', '1997-01-01')
    data['tb3m'] = yf_tb3m if len(yf_tb3m) > 100 else pd.Series(dtype=float)
    print(f"    {'TB3M':>15s}: {len(data['tb3m']):>5d} obs")

    # KRE, XLF, GLD, TLT — from TwelveData with yfinance fallback
    for sym in ['KRE', 'XLF', 'GLD', 'TLT']:
        td = fetch_twelvedata(sym)
        if len(td) < 200:
            td = fetch_yfinance(sym, '2000-01-01')
        data[sym.lower()] = td
        print(f"    {sym:>15s}: {len(td):>5d} obs")
        if sym != 'TLT':
            time.sleep(1)  # rate limit

    # Dollar — from local file (DTWEXB)
    dtwexb_path = DATA_DIR / 'DTWEXB-1.csv'
    if dtwexb_path.exists():
        df = pd.read_csv(dtwexb_path, parse_dates=['observation_date'], index_col='observation_date')
        data['dollar'] = df.iloc[:, 0].replace('.', np.nan).astype(float).dropna()
        print(f"    {'Dollar':>15s}: {len(data['dollar']):>5d} obs")
    else:
        data['dollar'] = pd.Series(dtype=float)

    return data


# ============================================================
# COMPUTATION ENGINE
# ============================================================
def compute_all(data):
    """Compute all signals, regime, capacity assessment from raw data."""
    print(f"\n{BOLD}Computing signals...{RESET}")
    result = {}

    # Build common date index
    dates = pd.date_range('2000-01-01', pd.Timestamp.now(), freq='B')

    # Reindex everything
    hy = data['hy_oas'].reindex(dates, method='ffill')
    y10 = data['dgs10'].reindex(dates, method='ffill')
    spx = data['spx'].reindex(dates, method='ffill')
    vix = data['vix'].reindex(dates, method='ffill')
    kre = data['kre'].reindex(dates, method='ffill')
    xlf = data['xlf'].reindex(dates, method='ffill')
    gld = data['gld'].reindex(dates, method='ffill')
    tlt = data['tlt'].reindex(dates, method='ffill')
    dollar = data['dollar'].reindex(dates, method='ffill')
    nfci = data['nfci'].reindex(dates, method='ffill')
    stlfsi = data['stlfsi'].reindex(dates, method='ffill')
    bbb = data['bbb_spread'].reindex(dates, method='ffill')

    # Yield curve
    tb3m_raw = data['tb3m']
    if hasattr(tb3m_raw, 'columns') and tb3m_raw.ndim > 1:
        tb3m_raw = tb3m_raw.iloc[:, 0]
    tb3m = tb3m_raw.reindex(dates, method='ffill')
    if len(tb3m.dropna()) > 500:
        tb3m_c = tb3m.copy()
        med = float(tb3m_c.dropna().median())
        if med > 10:
            tb3m_c = tb3m_c / 10
        yc_slope = y10 - tb3m_c
    else:
        yc_slope = y10 - y10.rolling(63).mean()

    # ── W4+ SIGNAL ──
    hy_pct_5y = hy.rolling(1260, min_periods=504).rank(pct=True)
    hy_pct_vel = hy_pct_5y.diff(21)

    w4 = ((hy_pct_vel > 0) & (yc_slope < 1.0) & (vix > 18)).fillna(False)

    f6b = pd.Series(False, index=dates)
    if len(dollar.dropna()) > 100 and len(spx.dropna()) > 100:
        dollar_3m = dollar.pct_change(63) * 100
        spx_3m = spx.pct_change(63) * 100
        f6b = ((dollar_3m - spx_3m) > 10).fillna(False)

    w4plus = (w4 | f6b).fillna(False)

    # ── REGIME ──
    spx_200 = spx.rolling(200).mean()
    spx_dd = spx / spx.rolling(252).max() - 1

    regime = 'UNKNOWN'
    if len(spx.dropna()) > 252:
        latest_dd = spx_dd.dropna().iloc[-1]
        latest_above = spx.dropna().iloc[-1] > spx_200.dropna().iloc[-1]
        if latest_dd < -0.20:
            regime = 'BEAR'
        elif not latest_above and latest_dd < -0.05:
            regime = 'CORRECTION'
        elif latest_above:
            regime = 'UPTREND'
        else:
            regime = 'TRANSITION'

    # ── MULTI-DETECTOR (F1/F5/F6a/F6b) ──
    f1_fires = bool(w4.dropna().iloc[-1]) if len(w4.dropna()) > 0 else False

    f5_fires = False
    if len(kre.dropna()) > 252:
        kre_dd = (kre / kre.rolling(252).max() - 1).dropna().iloc[-1]
        f5_fires = kre_dd < -0.15 and vix.dropna().iloc[-1] > 18

    f6a_fires = False
    if len(gld.dropna()) > 100 and len(spx.dropna()) > 100:
        gld_3m_val = gld.pct_change(63).dropna().iloc[-1] * 100
        spx_3m_val = spx.pct_change(63).dropna().iloc[-1] * 100
        f6a_fires = gld_3m_val > 5 and spx_3m_val < -3

    f6b_fires = bool(f6b.dropna().iloc[-1]) if len(f6b.dropna()) > 0 else False

    detectors_firing = sum([f1_fires, f5_fires, f6a_fires, f6b_fires])

    # ── VELOCITIES ──
    vix_vel = vix.diff(21).dropna().iloc[-1] if len(vix.dropna()) > 21 else 0
    hy_vel = hy_pct_vel.dropna().iloc[-1] if len(hy_pct_vel.dropna()) > 0 else 0
    nfci_vel = nfci.diff(21).dropna().iloc[-1] if len(nfci.dropna()) > 21 else 0
    stlfsi_vel = stlfsi.diff(21).dropna().iloc[-1] if len(stlfsi.dropna()) > 21 else 0
    yc_vel = yc_slope.diff(63).dropna().iloc[-1] if len(yc_slope.dropna()) > 63 else 0

    # ── CAPACITY ASSESSMENT ──
    # Elastic: credit capacity (HY pct + velocity)
    hy_pct_now = hy_pct_5y.dropna().iloc[-1] if len(hy_pct_5y.dropna()) > 0 else 0.5
    elastic_state = 'STABLE'
    if hy_vel > 0.02:
        elastic_state = 'DEGRADING FAST'
    elif hy_vel > 0:
        elastic_state = 'DEGRADING'
    elif hy_vel < -0.02:
        elastic_state = 'EXPANDING FAST'
    elif hy_vel < 0:
        elastic_state = 'EXPANDING'

    # Viscous: monetary policy capacity (yield curve)
    yc_now = yc_slope.dropna().iloc[-1] if len(yc_slope.dropna()) > 0 else 1.0
    viscous_state = 'AMPLE' if yc_now > 2.0 else ('MODERATE' if yc_now > 1.0 else ('CONSTRAINED' if yc_now > 0 else 'INVERTED'))

    # Rigid: backstop capacity (always present in US — Fed/FDIC)
    rigid_state = 'PRESENT'

    # Information coupling: velocity vs level disagreement
    w4_fires = bool(w4plus.dropna().iloc[-1]) if len(w4plus.dropna()) > 0 else False
    nfci_fires = nfci.dropna().iloc[-1] > -0.3 if len(nfci.dropna()) > 0 else False
    info_state = 'ALIGNED' if w4_fires == nfci_fires else 'DIVERGENT'

    # ── STRUCTURAL HEALTH SCORE (0-100) ──
    # Composite for the dual-line chart
    # Higher = healthier. Components weighted by validated importance.
    elastic_score = max(0, min(100, 50 - hy_vel * 500))  # velocity-based
    viscous_score = max(0, min(100, yc_now * 40))  # yield curve
    vol_score = max(0, min(100, 100 - vix.dropna().iloc[-1] * 2.5)) if len(vix.dropna()) > 0 else 50
    health_score = int(elastic_score * 0.40 + viscous_score * 0.30 + vol_score * 0.30)

    # ── POSITION SIZING ──
    if regime == 'UPTREND' and not w4_fires:
        position = 1.0
        position_label = 'FULL RISK'
    elif regime == 'UPTREND' and w4_fires:
        position = 0.0
        position_label = 'EXIT — UPTREND+W4 ON'
    elif regime == 'CORRECTION' and not w4_fires:
        position = 0.50
        position_label = 'REDUCED'
    elif regime == 'CORRECTION' and w4_fires:
        position = 0.30
        position_label = 'DEFENSIVE'
    elif regime == 'BEAR':
        position = 0.20
        position_label = 'MINIMAL'
    else:
        position = 0.50
        position_label = 'NEUTRAL'

    # ── HISTORICAL ANALOG ──
    analog = 'No strong analog'
    if regime == 'CORRECTION' and w4_fires and hy_vel > 0:
        analog = 'Similar to Q3 2007 / Q4 2019 pre-phases — credit velocity rising in correction'
    elif regime == 'UPTREND' and w4_fires:
        analog = 'Similar to late 2006 / early 2020 — uptrend masking structural fragility'
    elif regime == 'BEAR' and not w4_fires:
        analog = 'Similar to Q1 2003 / Q1 2009 — bear with clearing fragility (recovery likely)'

    # ── COLLECT RESULTS ──
    result['date'] = datetime.utcnow().strftime('%Y-%m-%d')
    result['time'] = datetime.utcnow().strftime('%H:%M')

    # Core signal
    result['w4plus'] = bool(w4_fires)
    result['w4_component'] = bool(f1_fires)
    result['f6b_component'] = bool(f6b_fires)
    result['regime'] = regime
    result['combined_state'] = f"{regime} + {'Stress Detected' if w4_fires else 'Clear'}"

    # Multi-detector
    result['detectors'] = {
        'f1_credit': bool(f1_fires),
        'f5_banks': bool(f5_fires),
        'f6a_gold': bool(f6a_fires),
        'f6b_dollar': bool(f6b_fires),
        'count': detectors_firing,
    }

    # Capacity assessment
    result['capacity'] = {
        'elastic': {'state': elastic_state, 'hy_pct': round(float(hy_pct_now), 4),
                    'hy_velocity': round(float(hy_vel), 4)},
        'viscous': {'state': viscous_state, 'yc_slope': round(float(yc_now), 2),
                    'yc_velocity': round(float(yc_vel), 4)},
        'rigid': {'state': rigid_state},
        'information': {'state': info_state, 'w4_says': 'STRESS' if w4_fires else 'CLEAR',
                        'nfci_says': 'STRESS' if nfci_fires else 'CLEAR'},
    }

    # Velocities
    result['velocities'] = {
        'hy_pct': round(float(hy_vel), 4),
        'vix': round(float(vix_vel), 2),
        'nfci': round(float(nfci_vel), 4),
        'stlfsi': round(float(stlfsi_vel), 4),
        'yc': round(float(yc_vel), 4),
    }

    # Raw levels
    result['levels'] = {
        'vix': round(float(vix.dropna().iloc[-1]), 1) if len(vix.dropna()) > 0 else None,
        'hy_oas': round(float(hy.dropna().iloc[-1]), 2) if len(hy.dropna()) > 0 else None,
        'yc_slope': round(float(yc_now), 2),
        'spx': round(float(spx.dropna().iloc[-1]), 0) if len(spx.dropna()) > 0 else None,
        'spx_dd': round(float(spx_dd.dropna().iloc[-1] * 100), 1) if len(spx_dd.dropna()) > 0 else None,
        'nfci': round(float(nfci.dropna().iloc[-1]), 3) if len(nfci.dropna()) > 0 else None,
        'stlfsi': round(float(stlfsi.dropna().iloc[-1]), 3) if len(stlfsi.dropna()) > 0 else None,
        'dollar': round(float(dollar.dropna().iloc[-1]), 2) if len(dollar.dropna()) > 0 else None,
    }

    # Detectors detail
    result['detector_detail'] = {
        'f5_kre_dd': round(float(kre_dd * 100), 1) if len(kre.dropna()) > 252 else None,
        'f6a_gold_3m': round(float(gld_3m_val), 1) if len(gld.dropna()) > 100 else None,
        'f6a_spx_3m': round(float(spx_3m_val), 1) if len(spx.dropna()) > 100 else None,
        'f6b_divergence': round(float((dollar_3m - spx_3m).dropna().iloc[-1]), 1) if len(dollar.dropna()) > 100 else None,
    }

    # Position sizing
    result['position'] = {
        'equity_pct': round(position * 100),
        'bond_pct': round((1 - position) * 100),
        'label': position_label,
    }

    # Health score (for dual-line chart)
    result['health_score'] = health_score

    # Analog
    result['analog'] = analog

    # Build health timeseries (last 252 days for chart)
    health_ts = []
    if len(hy_pct_5y.dropna()) > 252 and len(yc_slope.dropna()) > 252 and len(vix.dropna()) > 252:
        for i in range(-min(252, len(dates)), 0):
            try:
                hv = hy_pct_5y.diff(21).iloc[i]
                yc = yc_slope.iloc[i]
                vi = vix.iloc[i]
                sp = spx.iloc[i]
                if any(pd.isna(x) for x in [hv, yc, vi, sp]):
                    continue
                e = max(0, min(100, 50 - float(hv) * 500))
                v = max(0, min(100, float(yc) * 40))
                vo = max(0, min(100, 100 - float(vi) * 2.5))
                h = int(e * 0.40 + v * 0.30 + vo * 0.30)
                health_ts.append({
                    'date': dates[i].strftime('%Y-%m-%d'),
                    'health': h,
                    'spx': round(float(sp), 0),
                })
            except:
                pass

    result['health_timeseries'] = health_ts

    return result


# ============================================================
# OUTPUT
# ============================================================
def write_outputs(result):
    """Write JSON, CSV, Supabase, email."""
    print(f"\n{BOLD}Writing outputs...{RESET}")

    # JSON latest
    try:
        with open(JSON_LATEST, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        print(f"  JSON latest: OK")
    except Exception as e:
        print(f"  {RED}JSON latest: {e}{RESET}")

    # JSON health timeseries (for chart)
    try:
        with open(JSON_HEALTH, 'w') as f:
            json.dump(result.get('health_timeseries', []), f)
        print(f"  JSON health: OK ({len(result.get('health_timeseries', []))} points)")
    except Exception as e:
        print(f"  {RED}JSON health: {e}{RESET}")

    # JSON history (append today, compress)
    try:
        history = []
        if JSON_HISTORY.exists():
            with open(JSON_HISTORY) as f:
                history = json.load(f)

        today_str = result['date']
        history = [e for e in history if e.get('date') != today_str]

        history_entry = {
            'date': today_str,
            'w4plus': result['w4plus'],
            'regime': result['regime'],
            'health_score': result['health_score'],
            'detectors': result['detectors']['count'],
            'position_pct': result['position']['equity_pct'],
            'vix': result['levels']['vix'],
            'hy_oas': result['levels']['hy_oas'],
            'yc_slope': result['levels']['yc_slope'],
            'hy_velocity': result['velocities']['hy_pct'],
        }
        history.insert(0, history_entry)

        # Keep last 1000 entries max
        history = history[:1000]

        # Convert numpy types for JSON
        def convert(obj):
            if isinstance(obj, (np.integer,)): return int(obj)
            if isinstance(obj, (np.floating,)): return float(obj)
            if isinstance(obj, (np.bool_,)): return bool(obj)
            return obj

        clean_history = json.loads(json.dumps(history, default=convert))
        with open(JSON_HISTORY, 'w') as f:
            json.dump(clean_history, f, indent=2)
        print(f"  JSON history: OK ({len(history)} entries)")
    except Exception as e:
        print(f"  {RED}JSON history: {e}{RESET}")

    # Email if state is concerning
    if result['w4plus'] and result['regime'] in ('UPTREND', 'CORRECTION'):
        subject = f"Flow Theory {result['date']} — {result['combined_state']}"
        body = build_email(result)
        send_email(subject, body)


def build_email(result):
    lines = [
        "FLOW THEORY — STRUCTURAL CAPACITY MONITOR",
        f"Date: {result['date']} {result['time']} UTC",
        "",
        f"SIGNAL:   W4+ {'■ FIRING' if result['w4plus'] else '□ CLEAR'}",
        f"REGIME:   {result['regime']}",
        f"STATE:    {result['combined_state']}",
        f"HEALTH:   {result['health_score']}/100",
        "",
        "CAPACITY:",
        f"  Elastic:  {result['capacity']['elastic']['state']} (HY vel: {result['velocities']['hy_pct']:+.4f})",
        f"  Viscous:  {result['capacity']['viscous']['state']} (YC: {result['levels']['yc_slope']}%)",
        f"  Rigid:    {result['capacity']['rigid']['state']}",
        f"  Info:     {result['capacity']['information']['state']}",
        "",
        f"POSITION: {result['position']['equity_pct']}% equity / {result['position']['bond_pct']}% bonds ({result['position']['label']})",
        "",
        f"DETECTORS: {result['detectors']['count']}/4 firing",
        f"  F1 Credit: {'■' if result['detectors']['f1_credit'] else '□'}",
        f"  F5 Banks:  {'■' if result['detectors']['f5_banks'] else '□'}",
        f"  F6a Gold:  {'■' if result['detectors']['f6a_gold'] else '□'}",
        f"  F6b Dollar:{'■' if result['detectors']['f6b_dollar'] else '□'}",
        "",
        f"ANALOG: {result['analog']}",
        "",
        "---",
        "Capacity Engine",
    ]
    return "\n".join(lines)


def send_email(subject, body):
    if not GMAIL_ADDRESS or GMAIL_ADDRESS == 'placeholder':
        return
    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = GMAIL_ADDRESS
        msg['To'] = NOTIFY_EMAIL
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as s:
            s.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            s.send_message(msg)
        print(f"  Email: sent")
    except Exception as e:
        print(f"  {RED}Email: {e}{RESET}")


# ============================================================
# TERMINAL REPORT
# ============================================================
def print_report(result):
    bar = "━" * 60

    # Color based on state
    if result['regime'] == 'BEAR' or (result['regime'] == 'UPTREND' and result['w4plus']):
        sc = RED
    elif result['w4plus'] or result['regime'] == 'CORRECTION':
        sc = YELLOW
    else:
        sc = GREEN

    print(f"\n{BOLD}{bar}")
    print(f"CAPACITY — Structural Health Monitor")
    print(f"{result['date']} {result['time']} UTC")
    print(f"{bar}{RESET}\n")

    # Signal
    w4_mark = f"{RED}■ FIRING{RESET}" if result['w4plus'] else f"{GREEN}□ CLEAR{RESET}"
    print(f"  {BOLD}W4+ SIGNAL:{RESET}  {w4_mark}")
    print(f"  {BOLD}REGIME:{RESET}      {sc}{result['regime']}{RESET}")
    print(f"  {BOLD}STATE:{RESET}       {sc}{result['combined_state']}{RESET}")
    print(f"  {BOLD}HEALTH:{RESET}      {result['health_score']}/100")
    print()

    # Capacity
    print(f"  {BOLD}CAPACITY ASSESSMENT:{RESET}")
    e = result['capacity']['elastic']
    v = result['capacity']['viscous']
    r = result['capacity']['rigid']
    i = result['capacity']['information']
    print(f"    Elastic (credit):   {e['state']:>18s}  HY vel: {e['hy_velocity']:+.4f}")
    print(f"    Viscous (monetary): {v['state']:>18s}  YC: {v['yc_slope']}%")
    print(f"    Rigid (backstop):   {r['state']:>18s}")
    print(f"    Information:        {i['state']:>18s}  W4→{i['w4_says']}, NFCI→{i['nfci_says']}")
    print()

    # Velocities
    print(f"  {BOLD}VELOCITIES (21-day):{RESET}")
    vel = result['velocities']
    for vname, vval in vel.items():
        direction = '↑' if vval > 0.001 else ('↓' if vval < -0.001 else '→')
        print(f"    {vname:>10s}: {vval:>+8.4f} {direction}")
    print()

    # Detectors
    d = result['detectors']
    print(f"  {BOLD}DETECTORS:{RESET} {d['count']}/4 firing")
    print(f"    F1 Credit regime:  {'■' if d['f1_credit'] else '□'}")
    print(f"    F5 Banking stress: {'■' if d['f5_banks'] else '□'}")
    print(f"    F6a Gold/SPX:      {'■' if d['f6a_gold'] else '□'}")
    print(f"    F6b Dollar/SPX:    {'■' if d['f6b_dollar'] else '□'}")
    print()

    # Position
    p = result['position']
    print(f"  {BOLD}POSITION:{RESET} {p['equity_pct']}% equity / {p['bond_pct']}% bonds — {p['label']}")
    print()

    # Analog
    print(f"  {BOLD}ANALOG:{RESET} {result['analog']}")

    # Key levels
    print(f"\n  {DIM}Levels: VIX={result['levels']['vix']} HY={result['levels']['hy_oas']}% "
          f"YC={result['levels']['yc_slope']}% SPX={result['levels']['spx']} "
          f"dd={result['levels']['spx_dd']}% NFCI={result['levels']['nfci']}{RESET}")

    print(f"\n{BOLD}{bar}{RESET}\n")


# ============================================================
# MAIN
# ============================================================
def main():
    print(f"\n{BOLD}{'='*60}")
    print(f"  CAPACITY ENGINE")
    print(f"{'='*60}{RESET}")

    data = load_all_data()
    result = compute_all(data)
    print_report(result)
    write_outputs(result)

    print(f"{BOLD}Engine complete.{RESET}\n")

if __name__ == "__main__":
    main()
