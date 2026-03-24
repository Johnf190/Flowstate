#!/usr/bin/env python3
"""
Flowstate — Assessment Engine
Fetches, normalizes, scores, and reports structural conditions
in the global financial system using the Flow Theory framework.
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
import json, time, warnings, textwrap, smtplib
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
VOICE_FILE = SCRIPT_DIR / "FLOWSTATE_VOICE.md"
CACHE_MAX_AGE_HOURS = 24
LOOKBACK_DAYS = 730  # 2 years

# ── Terminal formatting ──────────────────────────────────────────────────────
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

# ── Scan numbering ───────────────────────────────────────────────────────────
def get_scan_number():
    hour = datetime.utcnow().hour
    if hour == 6:
        return 1
    elif hour == 12:
        return 2
    elif hour == 18:
        return 3
    elif hour == 22:
        return 4
    else:
        return 1  # manual run

def get_next_scan_time():
    now = datetime.utcnow()
    scan_hours = [6, 12, 18, 22]
    for h in scan_hours:
        candidate = now.replace(hour=h, minute=0, second=0, microsecond=0)
        if candidate > now:
            return candidate.strftime("%Y-%m-%dT%H:%M:00Z")
    # Next day 06:00
    tomorrow = now + timedelta(days=1)
    return tomorrow.replace(hour=6, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:00Z")

# ── Known update frequencies ─────────────────────────────────────────────────
SERIES_FREQUENCY = {
    # FRED daily
    "fred_sofr": "daily", "fred_fed_funds": "daily", "fred_vix": "daily",
    "fred_dgs10": "daily", "fred_dgs2": "daily", "fred_yield_curve": "daily",
    "fred_hy_spread": "daily", "fred_ig_spread": "daily",
    "fred_discount_window": "daily", "fred_inflation_exp": "daily",
    "fred_trade_dollar": "daily",
    "fred_brent_fred": "daily", "fred_wti_fred": "daily",
    "fred_natgas_fred": "daily",
    "fred_baa10y": "daily",
    # FRED weekly
    "fred_fed_assets": "weekly", "fred_bank_reserves": "weekly",
    "fred_bank_credit": "weekly", "fred_m2": "weekly",
    "fred_retail_gas": "weekly",
    "fred_nfci": "weekly", "fred_anfci": "weekly",
    # FRED monthly
    "fred_consumer_conf": "monthly", "fred_m2_velocity": "monthly",
    "fred_cpi_headline": "monthly", "fred_core_cpi": "monthly",
    "fred_michigan_infl": "monthly",
    "fred_foreign_treas": "monthly",
    # FRED weekly (stress)
    "fred_stlfsi4": "weekly",
    # Twelve Data (daily market data)
    "td_spy": "daily", "td_tlt": "daily", "td_gold": "daily",
    "td_usoil": "daily", "td_copper": "daily",
    # Derived (inherit daily)
    "derived_sofr_ff_gap": "daily", "derived_hy_ig_gap": "daily",
    "derived_yield_slope": "daily",
}

# ── Series definitions ───────────────────────────────────────────────────────
TD_SYMBOLS = {
    "spy":    {"symbol": "SPY",      "label": "S&P 500 ETF"},
    "tlt":    {"symbol": "TLT",      "label": "Long Bond ETF"},
    "gold":   {"symbol": "XAU/USD",  "label": "Gold Price"},
    "usoil":  {"symbol": "USO",      "label": "US Oil ETF"},
    "copper": {"symbol": "CPER",     "label": "Copper ETF (CPER)"},
}

FRED_TRUST = {
    "yield_curve":       {"id": "T10Y2Y",        "label": "Yield Curve (2y10y)"},
    "discount_window":   {"id": "DPCREDIT",       "label": "Discount Window"},
    "consumer_conf":     {"id": "UMCSENT",        "label": "Consumer Confidence"},
    "inflation_exp":     {"id": "EXPINF1YR",      "label": "Inflation Expectations"},
    "hy_spread":         {"id": "BAMLH0A0HYM2",   "label": "HY Credit Spread"},
    "ig_spread":         {"id": "BAMLC0A0CM",      "label": "IG Credit Spread"},
}

FRED_CAPACITY = {
    "fed_assets":        {"id": "WALCL",          "label": "Fed Balance Sheet"},
    "bank_reserves":     {"id": "WRESBAL",        "label": "Bank Reserves"},
    "m2":                {"id": "M2SL",           "label": "M2 Money Supply"},
    "bank_credit":       {"id": "TOTBKCR",        "label": "Bank Credit"},
    "m2_velocity":       {"id": "M2V",            "label": "M2 Velocity"},
}

FRED_PRESSURE = {
    "sofr":              {"id": "SOFR",           "label": "SOFR Rate"},
    "fed_funds":         {"id": "DFF",            "label": "Fed Funds Rate"},
    "vix":               {"id": "VIXCLS",         "label": "VIX"},
    "dgs10":             {"id": "DGS10",          "label": "10Y Treasury Yield"},
    "dgs2":              {"id": "DGS2",           "label": "2Y Treasury Yield"},
}

FRED_FLOW = {
    "foreign_treas":     {"id": "FDHBFIN",        "label": "Foreign Treasury Holdings"},
    "trade_dollar":      {"id": "DTWEXBGS",       "label": "Trade Weighted Dollar"},
}

FRED_ENERGY_SHOCK = {
    "cpi_headline":      {"id": "CPIAUCSL",       "label": "CPI (headline)"},
    "core_cpi":          {"id": "CPILFESL",       "label": "Core CPI"},
    "michigan_infl":     {"id": "MICH",           "label": "Inflation Expectations (Mich)"},
    "brent_fred":        {"id": "DCOILBRENTEU",   "label": "Brent Crude"},
    "wti_fred":          {"id": "DCOILWTICO",     "label": "WTI Crude"},
    "natgas_fred":       {"id": "DHHNGSP",        "label": "Natural Gas (Henry Hub)"},
    "retail_gas":        {"id": "GASREGCOVW",     "label": "US Retail Gas Price"},
    "stlfsi4":           {"id": "STLFSI4",        "label": "Financial Stress Index (STLFSI4)"},
    "baa10y":            {"id": "BAA10Y",         "label": "Baa Corporate Spread"},
    "nfci":              {"id": "NFCI",           "label": "Financial Conditions (NFCI)"},
    "anfci":             {"id": "ANFCI",          "label": "Adjusted NFCI"},
}

ALL_FRED = {}
ALL_FRED.update(FRED_TRUST)
ALL_FRED.update(FRED_CAPACITY)
ALL_FRED.update(FRED_PRESSURE)
ALL_FRED.update(FRED_FLOW)
ALL_FRED.update(FRED_ENERGY_SHOCK)

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
    if not TWELVEDATA_API_KEY or TWELVEDATA_API_KEY == "paste_your_key_here":
        print(f"  {YELLOW}WARNING: Twelve Data API key not set. "
              f"Skipping market data. FRED data only.{RESET}")
        return {k: {} for k in TD_SYMBOLS}, False

    cached, was_cached = load_cache()

    needed_keys = set(TD_SYMBOLS.keys())
    if cached and needed_keys.issubset(set(cached.keys())):
        print(f"  {DIM}[twelvedata] Using cached data (< 24h old){RESET}")
        return cached, True

    print(f"  [twelvedata] Fetching fresh data from API...")
    td = TDClient(apikey=TWELVEDATA_API_KEY)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

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
                rec = {"close": float(row["close"])}
                if "volume" in row and pd.notna(row["volume"]):
                    rec["volume"] = float(row["volume"])
                records[date_str] = rec
            result[key] = records
            fetched_count += 1
            if fetched_count < len(TD_SYMBOLS):
                time.sleep(8)
        except Exception as e:
            print(f"    {RED}FAILED: {e}{RESET}")
            # Resilience: use cache if available
            if cached and key in cached:
                result[key] = cached[key]
                print(f"    {YELLOW}Using cached data for {info['label']}{RESET}")
            else:
                result[key] = {}

    save_cache(result)
    return result, False

# ── FRED fetching ────────────────────────────────────────────────────────────
def fetch_fred():
    if not FRED_API_KEY or FRED_API_KEY == "paste_your_key_here":
        print(f"  {YELLOW}WARNING: FRED API key not set. "
              f"Skipping economic data. Twelve Data only.{RESET}")
        return {k: {} for k in ALL_FRED}, list(ALL_FRED.keys())

    print(f"  [FRED] Fetching data...")
    fred = Fred(api_key=FRED_API_KEY)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)

    result = {}
    failed = []
    for key, info in ALL_FRED.items():
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
            freq = "daily" if count > 200 else ("weekly" if count > 50 else "monthly/quarterly")
            print(f"    {info['label']:30s} {count:>5} pts  ({freq})")
        except Exception as e:
            print(f"    {RED}{info['label']:30s} FAILED: {e}{RESET}")
            result[key] = {}
            failed.append(key)

    return result, failed

# ── Build aligned DataFrame ──────────────────────────────────────────────────
def build_dataframe(td_data, fred_data):
    all_series = {}

    for key, records in td_data.items():
        if not records:
            continue
        close_data = {}
        vol_data = {}
        for date_str, vals in records.items():
            if isinstance(vals, dict):
                close_data[date_str] = vals["close"]
                if "volume" in vals:
                    vol_data[date_str] = vals["volume"]
            else:
                close_data[date_str] = float(vals)

        s = pd.Series(close_data, dtype=float)
        s.index = pd.to_datetime(s.index)
        all_series[f"td_{key}"] = s.sort_index()

        if vol_data:
            sv = pd.Series(vol_data, dtype=float)
            sv.index = pd.to_datetime(sv.index)
            all_series[f"td_{key}_volume"] = sv.sort_index()

    for key, records in fred_data.items():
        if not records:
            continue
        s = pd.Series(records, dtype=float)
        s.index = pd.to_datetime(s.index)
        all_series[f"fred_{key}"] = s.sort_index()

    df = pd.DataFrame(all_series)
    df = df.sort_index()
    df = df.ffill(limit=3)

    return df

# ── Frequency-aware staleness detection ──────────────────────────────────────
def detect_stale(df):
    stale = []
    missing = []
    for col in df.columns:
        s = df[col].dropna()
        if len(s) == 0:
            missing.append(col)
            continue

        freq = SERIES_FREQUENCY.get(col, "daily")
        if freq == "daily":
            tail_window = 3
        elif freq == "weekly":
            tail_window = 10
        else:
            tail_window = 35

        if len(df) >= tail_window:
            tail = df[col].iloc[-tail_window:]
            if tail.isna().all():
                last_valid = s.index[-1].strftime("%Y-%m-%d")
                stale.append((col, freq, last_valid))

    return stale, missing

# ── Normalization ────────────────────────────────────────────────────────────
def normalize_0_10(series):
    valid = series.dropna()
    if len(valid) < 2:
        return pd.Series(np.nan, index=series.index)
    mn, mx = valid.min(), valid.max()
    if mx - mn < 1e-10:
        return pd.Series(5.0, index=series.index)
    return (series - mn) / (mx - mn) * 10.0

def calc_zscore(series):
    valid = series.dropna()
    if len(valid) < 2:
        return pd.Series(np.nan, index=series.index)
    m, s = valid.mean(), valid.std()
    if s < 1e-10:
        return pd.Series(0.0, index=series.index)
    return (series - m) / s

# ── Derived pressure series ──────────────────────────────────────────────────
def add_derived(df):
    if "fred_sofr" in df.columns and "fred_fed_funds" in df.columns:
        df["derived_sofr_ff_gap"] = df["fred_sofr"] - df["fred_fed_funds"]
    if "fred_hy_spread" in df.columns and "fred_ig_spread" in df.columns:
        df["derived_hy_ig_gap"] = df["fred_hy_spread"] - df["fred_ig_spread"]
    if "fred_dgs10" in df.columns and "fred_dgs2" in df.columns:
        df["derived_yield_slope"] = df["fred_dgs10"] - df["fred_dgs2"]
    return df

# ── Framework map ────────────────────────────────────────────────────────────
FRAMEWORK_MAP = {
    "trust_components": {
        "yield_curve":      {"col": "fred_yield_curve",   "label": "Yield Curve (2y10y)"},
        "consumer_conf":    {"col": "fred_consumer_conf",  "label": "Consumer Confidence"},
        "inflation_exp":    {"col": "fred_inflation_exp",  "label": "Inflation Expectations"},
        "hy_spread":        {"col": "fred_hy_spread",      "label": "HY Credit Spread"},
        "ig_spread":        {"col": "fred_ig_spread",      "label": "IG Credit Spread"},
        "discount_window":  {"col": "fred_discount_window","label": "Discount Window"},
    },
    "capacity_components": {
        "fed_assets":       {"col": "fred_fed_assets",     "label": "Fed Balance Sheet"},
        "bank_reserves":    {"col": "fred_bank_reserves",  "label": "Bank Reserves"},
        "m2":               {"col": "fred_m2",             "label": "M2 Money Supply"},
        "bank_credit":      {"col": "fred_bank_credit",    "label": "Bank Credit"},
        "m2_velocity":      {"col": "fred_m2_velocity",    "label": "M2 Velocity"},
    },
    "pressure_components": {
        "vix":              {"col": "fred_vix",            "label": "VIX"},
        "sofr_ff_gap":      {"col": "derived_sofr_ff_gap", "label": "SOFR/FF Gap"},
        "hy_ig_gap":        {"col": "derived_hy_ig_gap",   "label": "HY/IG Gap"},
        "yield_slope":      {"col": "derived_yield_slope", "label": "Yield Curve Slope"},
    },
    "flow_components": {
        "gold":             {"col": "td_gold",             "label": "Gold Price"},
        "copper":           {"col": "td_copper",            "label": "Copper (Growth Proxy)"},
        "foreign_treas":    {"col": "fred_foreign_treas",  "label": "Foreign Treasury Holdings"},
        "trade_dollar":     {"col": "fred_trade_dollar",   "label": "Trade Weighted Dollar"},
        "usoil_etf":        {"col": "td_usoil",           "label": "US Oil ETF (USO)"},
    },
    "energy_shock_components": {
        "brent_crude":      {"col": "fred_brent_fred",     "label": "Brent Crude"},
        "wti_crude":        {"col": "fred_wti_fred",       "label": "WTI Crude"},
        "natural_gas":      {"col": "fred_natgas_fred",    "label": "Natural Gas"},
        "retail_gas":       {"col": "fred_retail_gas",     "label": "US Retail Gas Price"},
        "cpi_headline":     {"col": "fred_cpi_headline",   "label": "CPI (headline)"},
        "core_cpi":         {"col": "fred_core_cpi",       "label": "Core CPI"},
        "inflation_expectations": {"col": "fred_michigan_infl", "label": "Inflation Expectations"},
        "stress_index":     {"col": "fred_stlfsi4",        "label": "Financial Stress (STLFSI4)"},
        "financial_conditions": {"col": "fred_nfci",       "label": "Financial Conditions"},
    },
    "derived_series": {
        "sofr_ff_gap":      {"col": "derived_sofr_ff_gap",   "label": "SOFR/FF Gap"},
        "hy_ig_gap":        {"col": "derived_hy_ig_gap",     "label": "HY/IG Gap"},
        "yield_slope":      {"col": "derived_yield_slope",   "label": "Yield Curve Slope"},
    },
}

def get_latest_values(df, norm_df, z_df):
    latest = {}
    for group_name, series_map in FRAMEWORK_MAP.items():
        latest[group_name] = {}
        for key, info in series_map.items():
            col = info["col"]
            norm_col = f"{col}_norm"
            z_col = f"{col}_zscore"

            raw_val = None
            norm_val = None
            z_val = None
            raw_min = None
            raw_max = None

            if col in df.columns:
                valid = df[col].dropna()
                if len(valid) > 0:
                    raw_val = float(valid.iloc[-1])
                    raw_min = float(valid.min())
                    raw_max = float(valid.max())
            if norm_col in norm_df.columns:
                valid = norm_df[norm_col].dropna()
                if len(valid) > 0:
                    norm_val = round(float(valid.iloc[-1]), 2)
            if z_col in z_df.columns:
                valid = z_df[z_col].dropna()
                if len(valid) > 0:
                    z_val = round(float(valid.iloc[-1]), 2)

            latest[group_name][key] = {
                "label": info["label"],
                "raw": raw_val,
                "norm": norm_val,
                "zscore": z_val,
                "raw_min": raw_min,
                "raw_max": raw_max,
            }
    return latest

# ── CSV output ───────────────────────────────────────────────────────────────
def append_csv(date_str, scan_number, df, norm_df, z_df):
    row = {"date": date_str, "scan_number": scan_number}

    relevant = set()
    for group in FRAMEWORK_MAP.values():
        for info in group.values():
            relevant.add(info["col"])

    for col in sorted(relevant):
        if col in df.columns:
            valid = df[col].dropna()
            row[f"{col}_raw"] = float(valid.iloc[-1]) if len(valid) > 0 else ""
        else:
            row[f"{col}_raw"] = ""
        nc = f"{col}_norm"
        if nc in norm_df.columns:
            valid = norm_df[nc].dropna()
            row[nc] = round(float(valid.iloc[-1]), 4) if len(valid) > 0 else ""
        else:
            row[nc] = ""
        zc = f"{col}_zscore"
        if zc in z_df.columns:
            valid = z_df[zc].dropna()
            row[zc] = round(float(valid.iloc[-1]), 4) if len(valid) > 0 else ""
        else:
            row[zc] = ""

    # Ensure data directory exists
    CSV_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Dedup: check if this date+scan_number already exists in CSV
    if CSV_FILE.exists():
        try:
            existing = pd.read_csv(CSV_FILE)
            mask = (existing['date'] == date_str) & (existing['scan_number'] == scan_number)
            if mask.any():
                # Drop old row(s) for this date+scan, rewrite
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

# ── Assessment generation ────────────────────────────────────────────────────
def generate_assessment(scan_data, system_state):
    # Load voice file
    system_prompt = ""
    try:
        if VOICE_FILE.exists():
            system_prompt = VOICE_FILE.read_text()
        else:
            system_prompt = ("You are the Flowstate assessment engine. "
                             "State mechanical realities only. Maximum 120 words. "
                             "Two parts: ASSESSMENT (2-3 sentences) and WATCH (2-3 bullets).")
    except Exception:
        system_prompt = ("You are the Flowstate assessment engine. "
                         "State mechanical realities only. Maximum 120 words.")

    # Try Anthropic API
    if ANTHROPIC_API_KEY and ANTHROPIC_API_KEY != 'placeholder':
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=300,
                system=system_prompt,
                messages=[{"role": "user", "content": json.dumps(scan_data)}]
            )
            return response.content[0].text
        except Exception as e:
            print(f"  {YELLOW}Anthropic API failed: {e}. Using fallback.{RESET}")

    # Fallback text
    fallbacks = {
        "STABLE": "All structural conditions within normal parameters.",
        "STRESSED": "Multiple pressure channels elevated. Monitor thresholds.",
        "CRITICAL": "Pre-cascade configuration. Four or more conditions active. Effective capacity degraded.",
        "CASCADE": "Maximum stress configuration. All six conditions active.",
    }
    return fallbacks.get(system_state, fallbacks["STABLE"])

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
        # Convert booleans and non-serializable types for JSON
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

# ── JSON output ──────────────────────────────────────────────────────────────
def write_json_files(scan_number, system_state, score,
                     cond_energy, cond_fear, cond_inflation,
                     cond_buffer, cond_bonds, cond_banks,
                     brent_z, vix_z, cpi_n, fed_bs_z, tlt_z, stlfsi4_r,
                     brent_raw, vix_raw, gold_raw, gold_z,
                     copper_z, cpi_z, uso_z, spy_z,
                     det_energy_shock, det_stagflation,
                     det_inflationary_boom, det_simultaneous_selloff,
                     det_interbank_stress,
                     assessment_text, what_to_watch,
                     stale_info, missing_series, cache_used,
                     total_series):
    now = datetime.utcnow()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")

    latest_obj = {
        "date": date_str,
        "time": f"{time_str} UTC",
        "scan_number": scan_number,
        "system_state": system_state,
        "score": score,
        "max_score": 6,
        "conditions": {
            "energy_shock":      {"active": bool(cond_energy),    "value": round(brent_z, 4) if brent_z is not None else 0.0, "label": "Brent z-score"},
            "fear_elevated":     {"active": bool(cond_fear),      "value": round(vix_z, 4) if vix_z is not None else 0.0, "label": "VIX z-score"},
            "inflation_ceiling": {"active": bool(cond_inflation), "value": round(cpi_n, 4) if cpi_n is not None else 0.0, "label": "CPI normalized"},
            "buffer_thin":       {"active": bool(cond_buffer),    "value": round(fed_bs_z, 4) if fed_bs_z is not None else 0.0, "label": "Fed BS z-score"},
            "bonds_selling":     {"active": bool(cond_bonds),     "value": round(tlt_z, 4) if tlt_z is not None else 0.0, "label": "TLT z-score"},
            "banks_stressed":    {"active": bool(cond_banks),     "value": round(stlfsi4_r, 4) if stlfsi4_r is not None else 0.0, "label": "STLFSI4 raw"},
        },
        "key_readings": {
            "brent":   {"raw": round(brent_raw, 2) if brent_raw else 0.0, "zscore": round(brent_z, 4) if brent_z is not None else 0.0, "norm": 0.0},
            "vix":     {"raw": round(vix_raw, 2) if vix_raw else 0.0, "zscore": round(vix_z, 4) if vix_z is not None else 0.0, "norm": 0.0},
            "gold":    {"raw": round(gold_raw, 2) if gold_raw else 0.0, "zscore": round(gold_z, 4) if gold_z is not None else 0.0, "norm": 0.0},
            "copper":  {"zscore": round(copper_z, 4) if copper_z is not None else 0.0},
            "cpi":     {"norm": round(cpi_n, 4) if cpi_n is not None else 0.0, "zscore": round(cpi_z, 4) if cpi_z is not None else 0.0},
            "fed_bs":  {"zscore": round(fed_bs_z, 4) if fed_bs_z is not None else 0.0},
            "tlt":     {"zscore": round(tlt_z, 4) if tlt_z is not None else 0.0},
            "uso":     {"zscore": round(uso_z, 4) if uso_z is not None else 0.0},
            "stlfsi4": {"raw": round(stlfsi4_r, 4) if stlfsi4_r is not None else 0.0},
        },
        "detectors": {
            "energy_shock": bool(det_energy_shock),
            "stagflation": bool(det_stagflation),
            "inflationary_boom": bool(det_inflationary_boom),
            "simultaneous_selloff": bool(det_simultaneous_selloff),
            "interbank_stress": bool(det_interbank_stress),
        },
        "assessment": assessment_text or "",
        "what_to_watch": what_to_watch or "",
        "meta": {
            "next_scan": get_next_scan_time(),
            "series_fetched": total_series,
            "stale_series": [s[0].replace("fred_", "").replace("td_", "").replace("derived_", "") for s in stale_info],
            "missing_series": missing_series,
            "cache_used": cache_used,
            "framework": "Flow Theory v1.0",
            "voice_version": "FLOWSTATE_VOICE v1.0",
            "api_endpoint": "https://api.flowstate.io/v1",
        },
    }

    # Write latest JSON
    try:
        with open(JSON_LATEST, "w") as f:
            json.dump(latest_obj, f, indent=2)
    except Exception as e:
        print(f"  {RED}JSON latest write failed: {e}{RESET}")

    # Build history from CSV
    try:
        history = []
        if CSV_FILE.exists():
            csv_df = pd.read_csv(CSV_FILE)
            # All rows, newest first
            csv_df = csv_df.iloc[::-1]
            for _, row in csv_df.iterrows():
                entry = {
                    "date": str(row.get("date", "")),
                    "scan_number": int(row.get("scan_number", 1)) if "scan_number" in row and pd.notna(row.get("scan_number")) else 1,
                }
                # Extract z-scores from CSV columns
                col_map = {
                    "brent_zscore": "fred_brent_fred_zscore",
                    "vix_zscore": "fred_vix_zscore",
                    "cpi_norm": "fred_core_cpi_norm",
                    "fed_bs_zscore": "fred_fed_assets_zscore",
                    "tlt_zscore": "td_tlt_zscore",
                    "stlfsi4_raw": "fred_stlfsi4_raw",
                    "copper_zscore": "td_copper_zscore",
                    "spy_zscore": "td_spy_zscore",
                    "gold_zscore": "td_gold_zscore",
                    "uso_zscore": "td_usoil_zscore",
                }
                for out_key, csv_col in col_map.items():
                    val = row.get(csv_col)
                    entry[out_key] = round(float(val), 4) if pd.notna(val) and val != "" else 0.0

                # Reconstruct conditions
                bz = entry.get("brent_zscore", 0.0)
                vz = entry.get("vix_zscore", 0.0)
                cn = entry.get("cpi_norm", 0.0)
                fz = entry.get("fed_bs_zscore", 0.0)
                tz = entry.get("tlt_zscore", 0.0)
                sr = entry.get("stlfsi4_raw", 0.0)

                h_cond_energy = bz > 1.5
                h_cond_fear = vz > 1.5
                h_cond_inflation = cn > 8.0
                h_cond_buffer = fz < -0.5
                h_cond_bonds = tz < -1.0
                h_cond_banks = sr > 0.5

                h_score = sum([h_cond_energy, h_cond_fear, h_cond_inflation,
                               h_cond_buffer, h_cond_bonds, h_cond_banks])

                if h_score <= 1:
                    h_state = "STABLE"
                elif h_score <= 3:
                    h_state = "STRESSED"
                elif h_score <= 5:
                    h_state = "CRITICAL"
                else:
                    h_state = "CASCADE"

                entry["system_state"] = h_state
                entry["score"] = h_score
                entry["max_score"] = 6
                entry["conditions"] = {
                    "energy_shock": h_cond_energy,
                    "fear_elevated": h_cond_fear,
                    "inflation_ceiling": h_cond_inflation,
                    "buffer_thin": h_cond_buffer,
                    "bonds_selling": h_cond_bonds,
                    "banks_stressed": h_cond_banks,
                }

                history.append(entry)

        # Prepend current scan
        history.insert(0, latest_obj)

        # Dedup by date + scan_number, keep first (newest) occurrence
        seen = set()
        deduped = []
        for entry in history:
            key = (entry.get("date", ""), entry.get("scan_number", 1))
            if key not in seen:
                seen.add(key)
                deduped.append(entry)
        history = deduped

        with open(JSON_HISTORY, "w") as f:
            json.dump(history, f, indent=2)
    except Exception as e:
        print(f"  {RED}JSON history write failed: {e}{RESET}")

# ── Terminal report ──────────────────────────────────────────────────────────
def fmt_line(label, vals, stale_info):
    raw = vals["raw"]
    norm = vals["norm"]
    z = vals["zscore"]
    raw_min = vals["raw_min"]
    raw_max = vals["raw_max"]

    if raw is None:
        return f"  {label:30s} {RED}NO DATA{RESET}"

    if abs(raw) >= 100000:
        raw_str = f"{raw:>14,.0f}"
    elif abs(raw) >= 1000:
        raw_str = f"{raw:>14,.2f}"
    elif abs(raw) >= 10:
        raw_str = f"{raw:>14.2f}"
    else:
        raw_str = f"{raw:>14.4f}"

    if norm is not None:
        if norm < 4:
            nc = GREEN
        elif norm < 7:
            nc = YELLOW
        else:
            nc = RED
        norm_str = f"{nc}{norm:5.2f}{RESET}"
    else:
        norm_str = f"{DIM}  N/A{RESET}"

    z_str = f"{z:+6.2f}" if z is not None else "   N/A"

    if raw_min is not None and raw_max is not None:
        if abs(raw_max) >= 100000:
            range_str = f"[{raw_min:,.0f} — {raw_max:,.0f}]"
        elif abs(raw_max) >= 1000:
            range_str = f"[{raw_min:,.2f} — {raw_max:,.2f}]"
        elif abs(raw_max) >= 10:
            range_str = f"[{raw_min:.2f} — {raw_max:.2f}]"
        else:
            range_str = f"[{raw_min:.4f} — {raw_max:.4f}]"
    else:
        range_str = ""

    key = vals.get("_key", "")
    stale_flag = ""
    for col_name, freq, last_date in stale_info:
        clean = col_name.replace("fred_", "").replace("td_", "").replace("derived_", "")
        if key == clean or key in col_name or label.lower() in clean.lower():
            if freq == "monthly":
                stale_flag = f" {YELLOW}[MONTHLY — last: {last_date}]{RESET}"
            elif freq == "weekly":
                stale_flag = f" {YELLOW}[WEEKLY — last: {last_date}]{RESET}"
            else:
                stale_flag = f" {RED}[STALE — last: {last_date}]{RESET}"
            break

    return f"  {label:30s}{raw_str}  norm:{norm_str}  z:{z_str}  {range_str}{stale_flag}"


def print_report(date_str, scan_number, latest, stale_info, missing_series,
                 total_series, cache_used, df, z_df, norm_df):
    bar = "━" * 45
    print(f"\n{BOLD}{bar}")
    print(f"FLOWSTATE — ASSESSMENT ENGINE")
    print(f"Date: {date_str}  |  Scan: {scan_number}/4")
    print(f"{bar}{RESET}\n")

    sections = [
        ("trust_components",         "TRUST COMPONENTS"),
        ("capacity_components",      "CAPACITY COMPONENTS"),
        ("pressure_components",      "PRESSURE COMPONENTS"),
        ("flow_components",          "FLOW COMPONENTS"),
        ("energy_shock_components",  "ENERGY AND SHOCK COMPONENTS"),
        ("derived_series",           "DERIVED SERIES"),
    ]

    for group_name, group_label in sections:
        if group_name not in latest:
            continue
        print(f"{BOLD}{group_label}:{RESET}")
        for key, vals in latest[group_name].items():
            vals["_key"] = key
            print(fmt_line(vals["label"], vals, stale_info))
        print()

    # ── System snapshot ──────────────────────────────────────────────────
    print(f"{BOLD}SYSTEM SNAPSHOT:{RESET}")
    all_z = []
    for group_name, series_map in latest.items():
        for key, vals in series_map.items():
            if vals["zscore"] is not None and vals["raw"] is not None:
                all_z.append((vals["label"], vals["zscore"]))

    if all_z:
        seen = set()
        unique_z = []
        for label, zs in all_z:
            if label not in seen:
                seen.add(label)
                unique_z.append((label, zs))

        highest = max(unique_z, key=lambda x: x[1])
        lowest = min(unique_z, key=lambda x: x[1])
        most_anomalous = max(unique_z, key=lambda x: abs(x[1]))

        print(f"  Highest z-score today:      {highest[0]} at {highest[1]:+.2f}")
        print(f"  Lowest z-score today:       {lowest[0]} at {lowest[1]:+.2f}")
        print(f"  Most anomalous component:   {most_anomalous[0]} "
              f"({most_anomalous[1]:+.2f} standard deviations from its 2yr mean)")
    else:
        print(f"  {DIM}Insufficient data for snapshot{RESET}")
    print()

    # ── Energy shock status ──────────────────────────────────────────────
    brent_z = None
    energy_group = latest.get("energy_shock_components", {})
    brent_vals = energy_group.get("brent_crude", {})
    if brent_vals and brent_vals.get("zscore") is not None:
        brent_z = brent_vals["zscore"]

    print(f"  {BOLD}Energy shock status:{RESET}")
    if brent_z is not None:
        if brent_z > 1.5:
            print(f"  {RED}⚠ ENERGY SHOCK ACTIVE — Brent {brent_z:+.2f} "
                  f"standard deviations above 2yr mean{RESET}")
        else:
            print(f"  {GREEN}✓ Energy markets normal{RESET}")
    else:
        print(f"  {DIM}No Brent data available for energy shock check{RESET}")
    print()

    # ── Stagflation detector ──────────────────────────────────────────────
    copper_z = None
    flow_group = latest.get("flow_components", {})
    copper_vals = flow_group.get("copper", {})
    if copper_vals and copper_vals.get("zscore") is not None:
        copper_z = copper_vals["zscore"]

    print(f"  {BOLD}Stagflation detector:{RESET}")
    if brent_z is not None and copper_z is not None:
        if brent_z > 1.5 and copper_z < -0.5:
            print(f"  {RED}🔴 STAGFLATION SIGNATURE — energy shock with "
                  f"growth collapse detected{RESET}")
            print(f"     Brent z: {brent_z:+.2f}  Copper z: {copper_z:+.2f}")
        elif brent_z > 1.5 and copper_z > 0.5:
            print(f"  {YELLOW}🟡 INFLATIONARY BOOM — energy and growth both "
                  f"elevated, watch Fed response{RESET}")
            print(f"     Brent z: {brent_z:+.2f}  Copper z: {copper_z:+.2f}")
        else:
            print(f"  {GREEN}✓ No stagflation signature detected{RESET}")
            print(f"     Brent z: {brent_z:+.2f}  Copper z: {copper_z:+.2f}")
    else:
        print(f"  {DIM}Brent/Copper data unavailable for stagflation check{RESET}")
    print()

    # ── Simultaneous selloff detector ────────────────────────────────────
    print(f"  {BOLD}Simultaneous sell-off detector:{RESET}")
    spy_z_val = None
    tlt_z_val = None
    if "td_spy_zscore" in z_df.columns:
        valid = z_df["td_spy_zscore"].dropna()
        if len(valid) > 0:
            spy_z_val = float(valid.iloc[-1])
    if "td_tlt_zscore" in z_df.columns:
        valid = z_df["td_tlt_zscore"].dropna()
        if len(valid) > 0:
            tlt_z_val = float(valid.iloc[-1])

    if spy_z_val is not None and tlt_z_val is not None:
        if spy_z_val < -1.0 and tlt_z_val < -1.0:
            print(f"  {RED}🔴 SIMULTANEOUS STOCK/BOND SELLOFF — "
                  f"dash for cash signature detected{RESET}")
            print(f"     SPY z: {spy_z_val:+.2f}  TLT z: {tlt_z_val:+.2f}")
        else:
            print(f"  {GREEN}✓ No simultaneous selloff detected{RESET}")
            print(f"     SPY z: {spy_z_val:+.2f}  TLT z: {tlt_z_val:+.2f}")
    else:
        print(f"  {DIM}SPY/TLT data unavailable for selloff check{RESET}")
    print()

    # ── Interbank / financial stress detector (STLFSI4) ────────────────
    stlfsi4_raw = None
    stlfsi4_vals = energy_group.get("stress_index", {})
    if stlfsi4_vals and stlfsi4_vals.get("raw") is not None:
        stlfsi4_raw = stlfsi4_vals["raw"]

    print(f"  {BOLD}Financial stress detector (STLFSI4):{RESET}")
    if stlfsi4_raw is not None:
        if stlfsi4_raw > 1.0:
            print(f"  {RED}⚠ FINANCIAL SYSTEM STRESS — St Louis Fed stress index "
                  f"above critical threshold (raw: {stlfsi4_raw:+.4f}){RESET}")
        elif stlfsi4_raw > 0.0:
            print(f"  {YELLOW}🟡 FINANCIAL STRESS ELEVATED — above normal "
                  f"baseline (raw: {stlfsi4_raw:+.4f}){RESET}")
        else:
            print(f"  {GREEN}✓ Financial system stress normal "
                  f"(raw: {stlfsi4_raw:+.4f}){RESET}")
    else:
        print(f"  {DIM}STLFSI4 data unavailable{RESET}")
    print()

    # ── Flow Theory System State ──────────────────────────────────────────
    print(f"{BOLD}{bar}")
    print(f"FLOWSTATE SYSTEM STATE:")
    print(f"{bar}{RESET}")

    vix_z = None
    vix_vals = latest.get("pressure_components", {}).get("vix", {})
    if vix_vals and vix_vals.get("zscore") is not None:
        vix_z = vix_vals["zscore"]

    cpi_norm = None
    cpi_vals = latest.get("energy_shock_components", {}).get("core_cpi", {})
    if cpi_vals and cpi_vals.get("norm") is not None:
        cpi_norm = cpi_vals["norm"]

    fed_bs_z = None
    fed_vals = latest.get("capacity_components", {}).get("fed_assets", {})
    if fed_vals and fed_vals.get("zscore") is not None:
        fed_bs_z = fed_vals["zscore"]

    cond_a = brent_z is not None and brent_z > 1.5
    cond_b = vix_z is not None and vix_z > 1.5
    cond_c = cpi_norm is not None and cpi_norm > 8.0
    cond_d = fed_bs_z is not None and fed_bs_z < -0.5
    cond_e = tlt_z_val is not None and tlt_z_val < -1.0
    cond_f = stlfsi4_raw is not None and stlfsi4_raw > 0.5

    conditions = [
        ("A", "Brent zscore > 1.5",              "energy shock",       cond_a),
        ("B", "VIX zscore > 1.5",                "fear elevated",      cond_b),
        ("C", "CPI norm > 8.0",                  "inflation ceiling",  cond_c),
        ("D", "Fed Balance Sheet zscore < -0.5",  "thin buffer",        cond_d),
        ("E", "TLT zscore < -1.0",               "bonds selling",      cond_e),
        ("F", "STLFSI4 raw > 0.5",               "financial stress",   cond_f),
    ]

    active_count = sum(1 for _, _, _, v in conditions if v)

    for letter, desc, short, active in conditions:
        mark = f"{RED}✓{RESET}" if active else f"{GREEN}✗{RESET}"
        status = f"{RED}{short}{RESET}" if active else f"{DIM}{short}{RESET}"
        print(f"  {mark}  {letter}. {desc:36s} ({status})")

    print()
    if active_count <= 1:
        print(f"  {GREEN}{BOLD}🟢 SYSTEM STABLE{RESET}")
    elif active_count <= 3:
        print(f"  {YELLOW}{BOLD}🟡 SYSTEM STRESSED — "
              f"multiple pressure channels active ({active_count}/6){RESET}")
    elif active_count <= 5:
        print(f"  {RED}{BOLD}🔴 SYSTEM CRITICAL — "
              f"pre-cascade conditions present ({active_count}/6){RESET}")
    else:
        print(f"  {RED}{BOLD}💀 SYSTEM CASCADE — "
              f"all channels firing simultaneously ({active_count}/6){RESET}")
    print()

    # ── Data quality ─────────────────────────────────────────────────────
    print(f"{BOLD}DATA QUALITY:{RESET}")
    print(f"  Total series fetched:       {total_series}")
    stale_display = ", ".join(
        f"{s[0].replace('fred_','').replace('td_','').replace('derived_','')} "
        f"({s[1]}, last: {s[2]})" for s in stale_info
    ) if stale_info else "None"
    missing_display = ", ".join(missing_series) if missing_series else "None"
    cache_str = f"{GREEN}used{RESET}" if cache_used else f"{YELLOW}fresh{RESET}"
    print(f"  Stale series:               {stale_display}")
    print(f"  Missing series:             {missing_display}")
    print(f"  Cache status:               {cache_str}")
    print(f"{BOLD}{bar}{RESET}\n")

# ── Email body builder ───────────────────────────────────────────────────────
def build_email_body(scan_record, assessment_text):
    lines = []
    lines.append(f"FLOWSTATE SCAN REPORT")
    lines.append(f"Date: {scan_record['scan_date']}  Time: {scan_record['scan_time']} UTC")
    lines.append(f"Scan: {scan_record['scan_number']}/4")
    lines.append(f"")
    lines.append(f"SYSTEM STATE: {scan_record['system_state']} ({scan_record['score']}/6)")
    lines.append(f"")
    lines.append(f"CONDITIONS:")
    lines.append(f"  Energy Shock:      {'ACTIVE' if scan_record.get('cond_energy') else 'inactive'}")
    lines.append(f"  Fear Elevated:     {'ACTIVE' if scan_record.get('cond_fear') else 'inactive'}")
    lines.append(f"  Inflation Ceiling: {'ACTIVE' if scan_record.get('cond_inflation') else 'inactive'}")
    lines.append(f"  Buffer Thin:       {'ACTIVE' if scan_record.get('cond_buffer') else 'inactive'}")
    lines.append(f"  Bonds Selling:     {'ACTIVE' if scan_record.get('cond_bonds') else 'inactive'}")
    lines.append(f"  Banks Stressed:    {'ACTIVE' if scan_record.get('cond_banks') else 'inactive'}")
    lines.append(f"")
    lines.append(f"KEY READINGS:")
    lines.append(f"  Brent z-score:  {scan_record.get('brent_zscore', 'N/A')}")
    lines.append(f"  VIX z-score:    {scan_record.get('vix_zscore', 'N/A')}")
    lines.append(f"  CPI norm:       {scan_record.get('cpi_norm', 'N/A')}")
    lines.append(f"  Fed BS z-score: {scan_record.get('fed_bs_zscore', 'N/A')}")
    lines.append(f"  TLT z-score:    {scan_record.get('tlt_zscore', 'N/A')}")
    lines.append(f"  STLFSI4 raw:    {scan_record.get('stlfsi4_raw', 'N/A')}")
    lines.append(f"")
    lines.append(f"ASSESSMENT:")
    lines.append(assessment_text or "N/A")
    lines.append(f"")
    lines.append(f"---")
    lines.append(f"Flowstate Assessment Engine")
    return "\n".join(lines)

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    scan_number = get_scan_number()

    print(f"\n{BOLD}Fetching data...{RESET}")
    td_data, cache_used = fetch_twelvedata()
    fred_data, fred_failed = fetch_fred()

    print(f"\n{BOLD}Building aligned dataset...{RESET}")
    df = build_dataframe(td_data, fred_data)
    df = add_derived(df)

    # Frequency-aware staleness
    stale_info, missing_cols = detect_stale(df)

    missing_series = list(fred_failed)
    for key, records in td_data.items():
        if not records:
            missing_series.append(key)
    for col in missing_cols:
        clean = col.replace("fred_", "").replace("td_", "").replace("derived_", "")
        if clean not in missing_series:
            missing_series.append(clean)

    # Build normalized and z-score DataFrames
    norm_frames = {}
    z_frames = {}
    relevant_cols = set()
    for group in FRAMEWORK_MAP.values():
        for info in group.values():
            relevant_cols.add(info["col"])
    for extra in ["td_spy", "td_tlt"]:
        relevant_cols.add(extra)

    for col in relevant_cols:
        if col in df.columns:
            norm_frames[f"{col}_norm"] = normalize_0_10(df[col])
            z_frames[f"{col}_zscore"] = calc_zscore(df[col])

    norm_df = pd.DataFrame(norm_frames, index=df.index)
    z_df = pd.DataFrame(z_frames, index=df.index)

    today = df.index[-1]
    today_str = today.strftime("%Y-%m-%d")

    latest = get_latest_values(df, norm_df, z_df)

    total_series = sum(1 for col in relevant_cols if col in df.columns
                       and df[col].dropna().shape[0] > 0)

    # Print existing terminal report
    print_report(today_str, scan_number, latest, stale_info, missing_series,
                 total_series, cache_used, df, z_df, norm_df)

    # Append CSV (always — resilience rule 8)
    append_csv(today_str, scan_number, df, norm_df, z_df)
    print(f"  CSV appended to {CSV_FILE.name}")

    # ── Extract values for conditions ────────────────────────────────────
    energy_group = latest.get("energy_shock_components", {})
    brent_vals = energy_group.get("brent_crude", {})
    brent_z = brent_vals.get("zscore") if brent_vals else None
    brent_raw = brent_vals.get("raw") if brent_vals else None

    vix_vals = latest.get("pressure_components", {}).get("vix", {})
    vix_z = vix_vals.get("zscore") if vix_vals else None
    vix_raw = vix_vals.get("raw") if vix_vals else None

    cpi_vals = energy_group.get("core_cpi", {})
    cpi_n = cpi_vals.get("norm") if cpi_vals else None
    cpi_z = cpi_vals.get("zscore") if cpi_vals else None

    fed_vals = latest.get("capacity_components", {}).get("fed_assets", {})
    fed_bs_z = fed_vals.get("zscore") if fed_vals else None

    # TLT z-score from z_df
    tlt_z = None
    if "td_tlt_zscore" in z_df.columns:
        valid = z_df["td_tlt_zscore"].dropna()
        if len(valid) > 0:
            tlt_z = float(valid.iloc[-1])

    # STLFSI4 raw
    stlfsi4_vals = energy_group.get("stress_index", {})
    stlfsi4_r = stlfsi4_vals.get("raw") if stlfsi4_vals else None

    # SPY z-score
    spy_z = None
    if "td_spy_zscore" in z_df.columns:
        valid = z_df["td_spy_zscore"].dropna()
        if len(valid) > 0:
            spy_z = float(valid.iloc[-1])

    # Copper z-score
    copper_vals = latest.get("flow_components", {}).get("copper", {})
    copper_z = copper_vals.get("zscore") if copper_vals else None

    # Gold
    gold_vals = latest.get("flow_components", {}).get("gold", {})
    gold_z = gold_vals.get("zscore") if gold_vals else None
    gold_raw = gold_vals.get("raw") if gold_vals else None

    # USO z-score
    uso_z = None
    if "td_usoil_zscore" in z_df.columns:
        valid = z_df["td_usoil_zscore"].dropna()
        if len(valid) > 0:
            uso_z = float(valid.iloc[-1])

    # ── Calculate conditions and state ───────────────────────────────────
    cond_energy    = brent_z is not None and brent_z > 1.5
    cond_fear      = vix_z is not None and vix_z > 1.5
    cond_inflation = cpi_n is not None and cpi_n > 8.0
    cond_buffer    = fed_bs_z is not None and fed_bs_z < -0.5
    cond_bonds     = tlt_z is not None and tlt_z < -1.0
    cond_banks     = stlfsi4_r is not None and stlfsi4_r > 0.5

    score = sum([cond_energy, cond_fear, cond_inflation,
                 cond_buffer, cond_bonds, cond_banks])

    if score <= 1:
        system_state = "STABLE"
    elif score <= 3:
        system_state = "STRESSED"
    elif score <= 5:
        system_state = "CRITICAL"
    else:
        system_state = "CASCADE"

    # Five detectors
    det_energy_shock      = brent_z is not None and brent_z > 1.5
    det_stagflation       = (brent_z is not None and brent_z > 1.5 and
                             tlt_z is not None and tlt_z < -0.5)
    det_inflationary_boom = (brent_z is not None and brent_z > 1.5 and
                             copper_z is not None and copper_z > 0.5)
    det_simultaneous_selloff = (spy_z is not None and spy_z < -1.0 and
                                tlt_z is not None and tlt_z < -1.0)
    det_interbank_stress  = stlfsi4_r is not None and stlfsi4_r > 1.0

    # ── Generate assessment ──────────────────────────────────────────────
    scan_summary = {
        "date": today_str,
        "scan_number": scan_number,
        "system_state": system_state,
        "score": score,
        "brent_zscore": brent_z,
        "vix_zscore": vix_z,
        "cpi_norm": cpi_n,
        "fed_bs_zscore": fed_bs_z,
        "tlt_zscore": tlt_z,
        "stlfsi4_raw": stlfsi4_r,
        "spy_zscore": spy_z,
        "copper_zscore": copper_z,
        "gold_zscore": gold_z,
        "conditions": {
            "energy_shock": cond_energy,
            "fear_elevated": cond_fear,
            "inflation_ceiling": cond_inflation,
            "buffer_thin": cond_buffer,
            "bonds_selling": cond_bonds,
            "banks_stressed": cond_banks,
        },
        "detectors": {
            "energy_shock": det_energy_shock,
            "stagflation": det_stagflation,
            "inflationary_boom": det_inflationary_boom,
            "simultaneous_selloff": det_simultaneous_selloff,
            "interbank_stress": det_interbank_stress,
        },
    }

    assessment_text = generate_assessment(scan_summary, system_state)

    # Split assessment into assessment and what_to_watch
    what_to_watch = ""
    if "WATCH:" in assessment_text:
        parts = assessment_text.split("WATCH:", 1)
        assessment_only = parts[0].replace("ASSESSMENT:", "").strip()
        what_to_watch = parts[1].strip()
    else:
        assessment_only = assessment_text

    # ── Print assessment ─────────────────────────────────────────────────
    bar = "━" * 45
    print(f"\n{BOLD}{bar}")
    print(f"ASSESSMENT:")
    print(f"{bar}{RESET}")
    for line in textwrap.wrap(assessment_only, width=60):
        print(f"  {line}")
    if what_to_watch:
        print(f"\n{BOLD}WATCH:{RESET}")
        for line in what_to_watch.split("\n"):
            print(f"  {line.strip()}")
    print()

    # ── Build scan record for Supabase ───────────────────────────────────
    scan_record = {
        'scan_date': today_str,
        'scan_time': datetime.utcnow().strftime('%H:%M'),
        'scan_number': scan_number,
        'system_state': system_state,
        'score': score,
        'cond_energy': cond_energy,
        'cond_fear': cond_fear,
        'cond_inflation': cond_inflation,
        'cond_buffer': cond_buffer,
        'cond_bonds': cond_bonds,
        'cond_banks': cond_banks,
        'brent_raw': brent_raw,
        'brent_zscore': round(brent_z, 4) if brent_z is not None else None,
        'vix_raw': vix_raw,
        'vix_zscore': round(vix_z, 4) if vix_z is not None else None,
        'gold_raw': gold_raw,
        'gold_zscore': round(gold_z, 4) if gold_z is not None else None,
        'copper_zscore': round(copper_z, 4) if copper_z is not None else None,
        'cpi_norm': round(cpi_n, 4) if cpi_n is not None else None,
        'cpi_zscore': round(cpi_z, 4) if cpi_z is not None else None,
        'fed_bs_zscore': round(fed_bs_z, 4) if fed_bs_z is not None else None,
        'tlt_zscore': round(tlt_z, 4) if tlt_z is not None else None,
        'uso_zscore': round(uso_z, 4) if uso_z is not None else None,
        'stlfsi4_raw': round(stlfsi4_r, 4) if stlfsi4_r is not None else None,
        'det_energy_shock': det_energy_shock,
        'det_stagflation': det_stagflation,
        'det_inflationary_boom': det_inflationary_boom,
        'det_simultaneous_selloff': det_simultaneous_selloff,
        'det_interbank_stress': det_interbank_stress,
        'assessment': assessment_only,
        'what_to_watch': what_to_watch,
        'full_data': json.dumps(scan_summary),
        'data_quality': json.dumps({
            'stale_series': [s[0] for s in stale_info],
            'missing_series': missing_series,
            'cache_used': cache_used,
        }),
    }

    # ── Save to Supabase ─────────────────────────────────────────────────
    db_ok = save_to_supabase(scan_record)

    # ── Write JSON files ─────────────────────────────────────────────────
    json_ok = True
    try:
        write_json_files(
            scan_number, system_state, score,
            cond_energy, cond_fear, cond_inflation,
            cond_buffer, cond_bonds, cond_banks,
            brent_z, vix_z, cpi_n, fed_bs_z, tlt_z, stlfsi4_r,
            brent_raw, vix_raw, gold_raw, gold_z,
            copper_z, cpi_z, uso_z, spy_z,
            det_energy_shock, det_stagflation,
            det_inflationary_boom, det_simultaneous_selloff,
            det_interbank_stress,
            assessment_only, what_to_watch,
            stale_info, missing_series, cache_used,
            total_series
        )
    except Exception as e:
        print(f"  {RED}JSON write failed: {e}{RESET}")
        json_ok = False

    # ── Send email ───────────────────────────────────────────────────────
    email_ok = send_email(
        f"Flowstate {today_str} — {system_state} ({score}/6)",
        build_email_body(scan_record, assessment_only)
    )

    # ── Print final status ───────────────────────────────────────────────
    print(f"{BOLD}STATUS:{RESET}")
    print(f"  Supabase: {'ok' if db_ok else 'failed'}")
    print(f"  JSON:     {'ok' if json_ok else 'failed'}")
    print(f"  Email:    {'ok' if email_ok else 'failed'}")
    print()

if __name__ == "__main__":
    main()
