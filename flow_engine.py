#!/usr/bin/env python3
"""
Silence Horizon Engine — Flow Theory
Monitors variance suppression across four financial channels
to detect pre-crisis silence accumulation.
"""

import os
from dotenv import load_dotenv
load_dotenv(override=True)

TWELVEDATA_API_KEY = os.getenv('TWELVEDATA_API_KEY', '')
FRED_API_KEY = os.getenv('FRED_API_KEY', '')
SUPABASE_URL = os.getenv('SUPABASE_URL', '')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', '')
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY', '')
NOTIFY_EMAIL = os.getenv('NOTIFY_EMAIL', '')
GMAIL_ADDRESS = os.getenv('GMAIL_ADDRESS', '')
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD', '')

# ── Imports ──────────────────────────────────────────────────────────────────
import json, time, warnings, smtplib, math
from datetime import datetime, timedelta
from pathlib import Path
from email.mime.text import MIMEText

import numpy as np
import pandas as pd
from fredapi import Fred
from twelvedata import TDClient

warnings.filterwarnings("ignore")

SCRIPT_DIR = Path(__file__).resolve().parent
CACHE_FILE = SCRIPT_DIR / "twelvedata_cache.json"
CSV_FILE = SCRIPT_DIR / "data" / "flow_data.csv"
JSON_LATEST = SCRIPT_DIR / "flow_data_latest.json"
JSON_HISTORY = SCRIPT_DIR / "flow_data_history.json"
CACHE_MAX_AGE_HOURS = 24

# Silence Horizon parameters
BASELINE_DAYS = 1500      # ~4 years calendar to get 756+ trading days (captures 2022 vol)
VARIANCE_WINDOW = 63      # ~3 months of trading days
BASELINE_WINDOW = 756     # 3 years for rolling mean of variance (captures 2022 vol)
CYCLE_TIME = 24           # months — SOC cycling period
SUPPRESSION_THRESHOLD = 0.10
STRAIN_THRESHOLD = 0.50

# ── Terminal formatting ──────────────────────────────────────────────────────
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

# ── Channel definitions ─────────────────────────────────────────────────────
# Channel 4: Twelve Data (KRE)
TD_SYMBOLS = {
    "kre": {"symbol": "KRE", "label": "KRE (Regional Banks)"},
}

# Channel 1, 2, 3: FRED (VIX, HY OAS, 10Y Treasury)
FRED_SERIES = {
    "vix":     {"id": "VIXCLS",        "label": "VIX (Equity)"},
    "hy_oas":  {"id": "BAMLH0A0HYM2", "label": "HY OAS (Credit)"},
    "dgs10":   {"id": "DGS10",         "label": "10Y Treasury Yield"},
}

CHANNELS = {
    "equity": {"source": "fred", "key": "vix",    "name": "Equity (VIX)"},
    "credit": {"source": "fred", "key": "hy_oas", "name": "Credit (HY OAS)"},
    "rates":  {"source": "fred", "key": "dgs10",  "name": "Rates (10Y Treasury)"},
    "banks":  {"source": "td",   "key": "kre",    "name": "Banks (KRE)"},
}

# ── Cache helpers ────────────────────────────────────────────────────────────
def load_cache():
    if CACHE_FILE.exists():
        age_hours = (time.time() - CACHE_FILE.stat().st_mtime) / 3600
        if age_hours < CACHE_MAX_AGE_HOURS:
            with open(CACHE_FILE) as f:
                return json.load(f), True
    return None, False

def save_cache(data):
    with open(CACHE_FILE, "w") as f:
        json.dump(data, f)

# ── Twelve Data fetching ─────────────────────────────────────────────────────
def fetch_twelvedata():
    if not TWELVEDATA_API_KEY:
        print(f"  {YELLOW}WARNING: Twelve Data API key not set.{RESET}")
        return {k: {} for k in TD_SYMBOLS}, False

    cached, was_cached = load_cache()
    needed_keys = set(TD_SYMBOLS.keys())
    if cached and needed_keys.issubset(set(cached.keys())):
        # Check if cached data is sufficient
        all_good = True
        for k in needed_keys:
            if not cached.get(k):
                all_good = False
                break
        if all_good:
            print(f"  {DIM}[twelvedata] Using cached data (< 24h old){RESET}")
            return cached, True

    print(f"  [twelvedata] Fetching fresh data from API...")
    td = TDClient(apikey=TWELVEDATA_API_KEY)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=BASELINE_DAYS)).strftime("%Y-%m-%d")

    result = cached if cached else {}
    fetched_count = 0

    for key, info in TD_SYMBOLS.items():
        if cached and key in cached and cached[key]:
            print(f"    {DIM}{info['label']} — cached{RESET}")
            continue

        symbol = info["symbol"]
        print(f"    Fetching {info['label']} ({symbol})...")
        try:
            ts = td.time_series(
                symbol=symbol,
                interval="1day",
                start_date=start_date,
                end_date=end_date,
                outputsize=5000,
            )
            df = ts.as_pandas()
            records = {}
            for idx, row in df.iterrows():
                date_str = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)[:10]
                records[date_str] = {"close": float(row["close"])}
            result[key] = records
            fetched_count += 1
            if fetched_count < len(TD_SYMBOLS):
                time.sleep(8)
        except Exception as e:
            print(f"    {RED}FAILED: {e}{RESET}")
            if cached and key in cached:
                result[key] = cached[key]
                print(f"    {YELLOW}Using cached data for {info['label']}{RESET}")
            else:
                result[key] = {}

    save_cache(result)
    return result, False

# ── FRED fetching ────────────────────────────────────────────────────────────
def fetch_fred():
    if not FRED_API_KEY:
        print(f"  {YELLOW}WARNING: FRED API key not set.{RESET}")
        return {k: {} for k in FRED_SERIES}, list(FRED_SERIES.keys())

    print(f"  [FRED] Fetching data...")
    fred = Fred(api_key=FRED_API_KEY)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=BASELINE_DAYS)

    result = {}
    failed = []
    for key, info in FRED_SERIES.items():
        series_id = info["id"]
        try:
            s = fred.get_series(series_id, observation_start=start_date,
                                observation_end=end_date)
            s = s.dropna()
            records = {}
            for idx, val in s.items():
                records[idx.strftime("%Y-%m-%d")] = float(val)
            result[key] = records
            count = len(records)
            print(f"    {info['label']:30s} {count:>5} pts")
        except Exception as e:
            print(f"    {RED}{info['label']:30s} FAILED: {e}{RESET}")
            result[key] = {}
            failed.append(key)

    return result, failed

# ── Build price series for each channel ──────────────────────────────────────
def build_channel_series(td_data, fred_data):
    """Convert raw fetched data into pandas Series for each channel."""
    series = {}

    for ch_name, ch_info in CHANNELS.items():
        source = ch_info["source"]
        key = ch_info["key"]

        if source == "td":
            raw = td_data.get(key, {})
            if not raw:
                series[ch_name] = pd.Series(dtype=float)
                continue
            prices = {}
            for date_str, vals in raw.items():
                if isinstance(vals, dict):
                    prices[date_str] = vals["close"]
                else:
                    prices[date_str] = float(vals)
            s = pd.Series(prices, dtype=float)
            s.index = pd.to_datetime(s.index)
            series[ch_name] = s.sort_index()

        elif source == "fred":
            raw = fred_data.get(key, {})
            if not raw:
                series[ch_name] = pd.Series(dtype=float)
                continue
            s = pd.Series(raw, dtype=float)
            s.index = pd.to_datetime(s.index)
            series[ch_name] = s.sort_index()

    return series

# ── Silence Horizon computation ──────────────────────────────────────────────
def compute_channel_metrics(price_series):
    """
    Compute suppression ratio, duration, sigma_ratio, and state for one channel.
    Returns dict with all channel metrics.
    """
    result = {
        "sigma_ratio": 0.0,
        "suppression_pct": 0,
        "duration_months": 0,
        "state": "NORMAL",
        "var_current": None,
        "var_baseline": None,
    }

    min_required = VARIANCE_WINDOW * 4  # ~252 trading days minimum
    if len(price_series) < min_required:
        print(f"    {YELLOW}Insufficient data ({len(price_series)} points, "
              f"need {min_required}){RESET}")
        return result

    # Daily returns
    returns = price_series.pct_change().dropna()
    if len(returns) < BASELINE_WINDOW:
        return result

    # Step 1: Realized variance
    # Rolling 63-day variance of daily returns
    rolling_var = returns.rolling(window=VARIANCE_WINDOW).var()

    # Current variance = latest 63-day variance
    var_current = rolling_var.iloc[-1]
    if pd.isna(var_current) or var_current <= 0:
        return result

    # Baseline variance = rolling mean of 63-day variance
    # Use available data up to BASELINE_WINDOW
    actual_baseline = min(BASELINE_WINDOW, len(rolling_var.dropna()) - 1)
    if actual_baseline < VARIANCE_WINDOW:
        actual_baseline = len(rolling_var.dropna()) - 1
    var_baseline_series = rolling_var.rolling(window=max(actual_baseline, 1)).mean()
    var_baseline = var_baseline_series.iloc[-1]
    if pd.isna(var_baseline) or var_baseline <= 0:
        return result

    result["var_current"] = float(var_current)
    result["var_baseline"] = float(var_baseline)

    # Step 2: Suppression ratio
    suppression = max(0.0, 1.0 - (var_current / var_baseline))
    result["suppression_pct"] = int(round(suppression * 100))

    # Step 3: Duration — consecutive months where suppression > threshold
    # Walk backwards through monthly windows to find when suppression started
    duration_months = 0
    duration_start_date = None
    if suppression > SUPPRESSION_THRESHOLD:
        trading_days_per_month = 21
        max_months_back = min(24, len(rolling_var) // trading_days_per_month)

        for m in range(max_months_back):
            idx = -(1 + m * trading_days_per_month)
            if abs(idx) >= len(rolling_var):
                break
            v = rolling_var.iloc[idx]
            if pd.isna(v) or var_baseline <= 0:
                break
            month_suppression = max(0.0, 1.0 - (v / var_baseline))
            if month_suppression > SUPPRESSION_THRESHOLD:
                duration_months += 1
                # Track the date this month-window corresponds to
                actual_idx = len(rolling_var) + idx
                if actual_idx >= 0 and actual_idx < len(rolling_var):
                    duration_start_date = rolling_var.index[actual_idx].strftime("%Y-%m-%d")
            else:
                break

    result["duration_months"] = duration_months
    result["duration_start"] = duration_start_date

    # Step 4: Sigma ratio (silence accumulation)
    sigma_ratio = suppression * duration_months / CYCLE_TIME
    result["sigma_ratio"] = round(sigma_ratio, 4)

    # Step 5: Channel state
    if sigma_ratio > 0.60:
        result["state"] = "DEEP SILENCE"
    elif sigma_ratio > 0.30:
        result["state"] = "SILENCE"
    elif sigma_ratio >= 0.10:
        result["state"] = "WATCH"
    else:
        result["state"] = "NORMAL"

    return result

def compute_sdi(channel_metrics):
    """Compute Silence Divergence Index."""
    ratios = {ch: m["sigma_ratio"] for ch, m in channel_metrics.items()}
    if not ratios:
        return 0.0, None, "NONE"

    max_ch = max(ratios, key=ratios.get)
    max_val = ratios[max_ch]
    others = [v for ch, v in ratios.items() if ch != max_ch]
    mean_others = sum(others) / len(others) if others else 0.0

    sdi = max_val - mean_others

    origin = None
    confidence = "NONE"
    if sdi > 0.40:
        origin = max_ch
        confidence = "HIGH"
    elif sdi > 0.20:
        origin = max_ch
        confidence = "MODERATE"

    return round(sdi, 4), origin, confidence

def compute_cfi(channel_metrics):
    """Compute Composite Fragility Index."""
    ratios = [m["sigma_ratio"] for m in channel_metrics.values()]
    if not ratios:
        return 1.15, "RESILIENT"

    # Kappa ratios
    kappa_ratios = [sr / STRAIN_THRESHOLD for sr in ratios]
    max_kappa = max(kappa_ratios)
    max_sigma = max(ratios)

    headroom = max(0.0, 1.0 - max_kappa)
    rigidity = 1.0    # No live rigidity data for financial markets
    info = 1.0         # No live R_I data
    silence = max(0.0, 1.0 - max_sigma)
    floor = 1.15       # Fed backstop — CCR historically > 1

    cfi = headroom * rigidity * info * silence * floor

    # Override: if CFI <= 0.05 but all channels < 0.30, downgrade
    all_below_030 = all(sr < 0.30 for sr in ratios)

    if cfi <= 0.01:
        state = "CATASTROPHIC"
        if all_below_030:
            state = "STRAINED"
    elif cfi <= 0.05:
        state = "CRITICAL"
        if all_below_030:
            state = "STRAINED"
    elif cfi <= 0.15:
        state = "STRAINED"
    else:
        state = "RESILIENT"

    return round(cfi, 4), state

# ── Supabase storage ─────────────────────────────────────────────────────────
def save_to_supabase(scan_record):
    if not SUPABASE_URL or SUPABASE_URL == 'placeholder':
        print(f"  {DIM}Supabase not configured, skipping.{RESET}")
        return False

    try:
        import requests as req
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        }
        url = f"{SUPABASE_URL}/rest/v1/scans?on_conflict=scan_date,scan_number"
        clean = {}
        for k, v in scan_record.items():
            if isinstance(v, (bool, np.bool_)):
                clean[k] = bool(v)
            elif isinstance(v, (np.integer,)):
                clean[k] = int(v)
            elif isinstance(v, (np.floating,)):
                clean[k] = float(v)
            else:
                clean[k] = v
        resp = req.post(url, headers=headers, json=clean, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"  {RED}Supabase failed: {e}{RESET}")
        return False

# ── Email notification ───────────────────────────────────────────────────────
def send_email(subject, body):
    if not GMAIL_ADDRESS or GMAIL_ADDRESS == 'placeholder':
        print(f"  {DIM}Email not configured, skipping.{RESET}")
        return False

    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = GMAIL_ADDRESS
        msg['To'] = NOTIFY_EMAIL
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as s:
            s.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            s.send_message(msg)
        return True
    except Exception as e:
        print(f"  {RED}Email failed: {e}{RESET}")
        return False

# ── History management with tiered compression ───────────────────────────────
def compress_history(history):
    """
    Apply tiered compression:
      Last 90 days: daily resolution
      91 days to 2 years: weekly (keep Sundays)
      Beyond 2 years: monthly (keep 1st of month)
    Never delete earliest record.
    """
    if not history:
        return history

    now = datetime.utcnow().date()
    day_90 = now - timedelta(days=90)
    day_730 = now - timedelta(days=730)

    daily = []
    weekly_candidates = {}
    monthly_candidates = {}

    for entry in history:
        try:
            d = datetime.strptime(entry["date"], "%Y-%m-%d").date()
        except (ValueError, KeyError):
            daily.append(entry)
            continue

        if d >= day_90:
            # Keep all daily
            entry["resolution"] = "daily"
            daily.append(entry)
        elif d >= day_730:
            # Weekly: keep Sunday of each week
            # Find the Sunday of this week
            week_key = d - timedelta(days=d.weekday() + 1)  # Previous Sunday
            if d.weekday() == 6:
                week_key = d
            if week_key not in weekly_candidates:
                entry["resolution"] = "weekly"
                weekly_candidates[week_key] = entry
        else:
            # Monthly: keep 1st of month
            month_key = d.replace(day=1)
            if month_key not in monthly_candidates:
                entry["resolution"] = "monthly"
                monthly_candidates[month_key] = entry

    # Combine and sort newest first
    result = daily + list(weekly_candidates.values()) + list(monthly_candidates.values())
    result.sort(key=lambda e: e.get("date", ""), reverse=True)

    # Ensure earliest record survives
    if history:
        earliest = min(history, key=lambda e: e.get("date", "9999"))
        earliest_date = earliest.get("date")
        if earliest_date and not any(e.get("date") == earliest_date for e in result):
            earliest["resolution"] = "monthly"
            result.append(earliest)

    return result

# ── JSON output ──────────────────────────────────────────────────────────────
def write_json_files(channel_metrics, cfi, cfi_state, sdi, origin, confidence):
    now = datetime.utcnow()

    # Build latest JSON
    latest = {
        "generated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "overall": {
            "cfi": cfi,
            "state": cfi_state,
            "sdi": sdi,
            "origin_channel": origin.upper() if origin else None,
            "origin_confidence": confidence,
        },
        "channels": {},
        "historical_context": {
            "last_silence_event": "2007-2008 GFC",
            "pattern_match": _get_pattern_match(channel_metrics, origin),
        },
    }

    for ch_name, ch_info in CHANNELS.items():
        m = channel_metrics[ch_name]
        latest["channels"][ch_name] = {
            "name": ch_info["name"],
            "sigma_ratio": m["sigma_ratio"],
            "suppression_pct": m["suppression_pct"],
            "duration_months": m["duration_months"],
            "state": m["state"],
            "var_current": round(m["var_current"], 6) if m["var_current"] else 0.0,
            "var_baseline": round(m["var_baseline"], 6) if m["var_baseline"] else 0.0,
        }

    # Write latest
    try:
        with open(JSON_LATEST, "w") as f:
            json.dump(latest, f, indent=2)
    except Exception as e:
        print(f"  {RED}JSON latest write failed: {e}{RESET}")

    # Build history record
    today_record = {
        "date": now.strftime("%Y-%m-%d"),
        "resolution": "daily",
        "cfi": cfi,
        "state": cfi_state,
        "sdi": sdi,
        "origin_channel": origin.upper() if origin else None,
        "sigma": {
            ch: channel_metrics[ch]["sigma_ratio"] for ch in CHANNELS
        },
    }

    # Load existing history, dedup, append, compress
    try:
        history = []
        if JSON_HISTORY.exists():
            with open(JSON_HISTORY) as f:
                history = json.load(f)

        # Dedup: remove any existing entry for today
        today_str = now.strftime("%Y-%m-%d")
        history = [e for e in history if e.get("date") != today_str]

        # Prepend today's record
        history.insert(0, today_record)

        # Apply tiered compression
        history = compress_history(history)

        with open(JSON_HISTORY, "w") as f:
            json.dump(history, f, indent=2)
    except Exception as e:
        print(f"  {RED}JSON history write failed: {e}{RESET}")

    return latest

def _get_pattern_match(metrics, origin):
    if not origin:
        return "No divergence detected"
    state = metrics.get(origin, {}).get("state", "NORMAL")
    patterns = {
        "credit": "Credit silence preceding equity blowup",
        "equity": "Equity variance suppression — watch for VIX spike",
        "rates": "Rate volatility collapse — watch for yield shock",
        "banks": "Banking sector silence — watch for credit event",
    }
    if state in ("SILENCE", "DEEP SILENCE"):
        return patterns.get(origin, "Channel silence detected")
    return "Mild divergence — monitoring"

# ── CSV output ───────────────────────────────────────────────────────────────
def append_csv(channel_metrics, cfi, cfi_state, sdi, origin):
    now = datetime.utcnow()
    date_str = now.strftime("%Y-%m-%d")

    row = {
        "date": date_str,
        "time": now.strftime("%H:%M"),
        "cfi": cfi,
        "state": cfi_state,
        "sdi": sdi,
        "origin_channel": origin or "",
    }
    for ch_name in CHANNELS:
        m = channel_metrics[ch_name]
        row[f"{ch_name}_sigma"] = m["sigma_ratio"]
        row[f"{ch_name}_suppression"] = m["suppression_pct"]
        row[f"{ch_name}_duration"] = m["duration_months"]
        row[f"{ch_name}_state"] = m["state"]
        row[f"{ch_name}_var_current"] = m["var_current"] or ""
        row[f"{ch_name}_var_baseline"] = m["var_baseline"] or ""

    CSV_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Dedup: remove existing entry for today
    if CSV_FILE.exists():
        try:
            existing = pd.read_csv(CSV_FILE)
            mask = existing['date'] == date_str
            if mask.any():
                existing = existing[~mask]
                existing.to_csv(CSV_FILE, index=False)
        except Exception:
            pass

    header_needed = not CSV_FILE.exists() or CSV_FILE.stat().st_size == 0
    cols = list(row.keys())
    with open(CSV_FILE, "a") as f:
        if header_needed:
            f.write(",".join(cols) + "\n")
        f.write(",".join(str(row[c]) for c in cols) + "\n")

# ── Terminal report ──────────────────────────────────────────────────────────
def print_report(channel_metrics, cfi, cfi_state, sdi, origin, confidence):
    bar = "━" * 55

    # State color
    state_colors = {"RESILIENT": GREEN, "STRAINED": YELLOW, "CRITICAL": RED, "CATASTROPHIC": RED}
    sc = state_colors.get(cfi_state, DIM)

    print(f"\n{BOLD}{bar}")
    print(f"SILENCE HORIZON — Flow Theory Engine")
    print(f"Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC")
    print(f"{bar}{RESET}\n")

    # Overall
    print(f"  {BOLD}COMPOSITE FRAGILITY INDEX:{RESET}")
    print(f"  {sc}{BOLD}  CFI = {cfi:.4f} — {cfi_state}{RESET}")
    print(f"    SDI = {sdi:.4f}", end="")
    if origin:
        print(f"  →  Origin: {origin.upper()} ({confidence})")
    else:
        print(f"  →  No divergence")
    print()

    # Channel details
    print(f"  {BOLD}CHANNEL STATUS:{RESET}")
    ch_state_colors = {
        "NORMAL": GREEN, "WATCH": YELLOW,
        "SILENCE": RED, "DEEP SILENCE": RED,
    }
    for ch_name, ch_info in CHANNELS.items():
        m = channel_metrics[ch_name]
        cc = ch_state_colors.get(m["state"], DIM)
        sigma_bar = "█" * int(m["sigma_ratio"] * 20) + "░" * (20 - int(m["sigma_ratio"] * 20))
        print(f"    {ch_info['name']:22s}  {cc}{m['state']:14s}{RESET}  "
              f"σ/σ* = {m['sigma_ratio']:.4f}  [{sigma_bar}]")
        if m["var_current"] is not None:
            start = m.get("duration_start", "?")
            print(f"      suppression: {m['suppression_pct']}%  "
                  f"duration: {m['duration_months']}mo (since {start})  "
                  f"var: {m['var_current']:.6f} / {m['var_baseline']:.6f}")
    print()

    print(f"{BOLD}{bar}{RESET}\n")

# ── Email body builder ───────────────────────────────────────────────────────
def build_email_body(channel_metrics, cfi, cfi_state, sdi, origin, confidence):
    lines = [
        "SILENCE HORIZON — DAILY SCAN",
        f"Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC",
        "",
        f"CFI: {cfi:.4f} — {cfi_state}",
        f"SDI: {sdi:.4f}",
    ]
    if origin:
        lines.append(f"Origin: {origin.upper()} ({confidence})")
    lines.append("")
    lines.append("CHANNELS:")
    for ch_name, ch_info in CHANNELS.items():
        m = channel_metrics[ch_name]
        lines.append(f"  {ch_info['name']:22s}  {m['state']:14s}  "
                     f"σ/σ* = {m['sigma_ratio']:.4f}  "
                     f"supp: {m['suppression_pct']}%  "
                     f"dur: {m['duration_months']}mo")
    lines.extend(["", "---", "Silence Horizon — Flow Theory Engine"])
    return "\n".join(lines)

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{BOLD}Fetching data...{RESET}")
    td_data, cache_used = fetch_twelvedata()
    fred_data, fred_failed = fetch_fred()

    print(f"\n{BOLD}Building channel series...{RESET}")
    channel_series = build_channel_series(td_data, fred_data)

    for ch_name, s in channel_series.items():
        pts = len(s)
        print(f"  {CHANNELS[ch_name]['name']:22s}  {pts:>5} data points")

    # Compute metrics for each channel
    print(f"\n{BOLD}Computing Silence Horizon metrics...{RESET}")
    channel_metrics = {}
    for ch_name in CHANNELS:
        print(f"  {CHANNELS[ch_name]['name']}:")
        s = channel_series.get(ch_name, pd.Series(dtype=float))
        channel_metrics[ch_name] = compute_channel_metrics(s)

    # Compute composite indices
    sdi, origin, confidence = compute_sdi(channel_metrics)
    cfi, cfi_state = compute_cfi(channel_metrics)

    # Terminal report
    print_report(channel_metrics, cfi, cfi_state, sdi, origin, confidence)

    # CSV (always write — resilience rule)
    append_csv(channel_metrics, cfi, cfi_state, sdi, origin)
    print(f"  CSV appended to {CSV_FILE.name}")

    # JSON files
    json_ok = True
    try:
        write_json_files(channel_metrics, cfi, cfi_state, sdi, origin, confidence)
    except Exception as e:
        print(f"  {RED}JSON write failed: {e}{RESET}")
        json_ok = False

    # Supabase
    scan_record = {
        'scan_date': datetime.utcnow().strftime('%Y-%m-%d'),
        'scan_time': datetime.utcnow().strftime('%H:%M'),
        'scan_number': 1,
        'system_state': cfi_state,
        'score': sum(1 for m in channel_metrics.values() if m["state"] != "NORMAL"),
        'cfi': cfi,
        'sdi': sdi,
        'assessment': f"CFI={cfi:.4f} {cfi_state}. SDI={sdi:.4f}.",
        'full_data': json.dumps({
            ch: channel_metrics[ch]["sigma_ratio"] for ch in CHANNELS
        }),
    }
    db_ok = save_to_supabase(scan_record)

    # Email
    email_ok = send_email(
        f"Silence Horizon {datetime.utcnow().strftime('%Y-%m-%d')} — {cfi_state} (CFI {cfi:.3f})",
        build_email_body(channel_metrics, cfi, cfi_state, sdi, origin, confidence)
    )

    # Final status
    print(f"\n{BOLD}STATUS:{RESET}")
    print(f"  Supabase: {'ok' if db_ok else 'failed'}")
    print(f"  JSON:     {'ok' if json_ok else 'failed'}")
    print(f"  Email:    {'ok' if email_ok else 'failed'}")
    print()

if __name__ == "__main__":
    main()
