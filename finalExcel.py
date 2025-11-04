import math
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import re
import logging

# Config / easy variables
DATA_DIR = Path("data")
LOCAL_COMBINED = DATA_DIR / "nifty_options_2023_12.parquet"
ENTRY_FILE = Path("entrypoints.xlsx")
OUTPUT_FILE = Path("finalExceloutput.xlsx")

# TARGET / STOP (change these numbers to adjust strategy)
TARGET_POINTS = 20
STOP_POINTS = 20

# Helpers to compute strikes (50 step)
def round_down(n, step=50):
    return int(math.floor(n / step) * step)

def round_up(n, step=50):
    return int(math.ceil(n / step) * step)

def strikes_from_entry_row(type_val: str, close_price: float):
    tl = str(type_val).lower()
    strikes = []
    if "buy" in tl and "ce" in tl:
        start = round_down(close_price, 50)
        for i in range(3):
            strikes.append((start - i * 50, "CE"))
    elif "buy" in tl and "pe" in tl:
        start = round_up(close_price, 50)
        for i in range(3):
            strikes.append((start + i * 50, "PE"))
    return strikes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ...existing code...

def _ensure_datetime_index(df):
    """Ensure df has a DatetimeIndex and return new DataFrame."""
    if isinstance(df.index, pd.DatetimeIndex):
        return df.sort_index()
    for c in ("Datetime", "datetime", "timestamp", "time", "Date", "date"):
        if c in df.columns:
            df = df.copy()
            df[c] = pd.to_datetime(df[c])
            df = df.set_index(c).sort_index()
            return df
    # try to infer index name
    try:
        idx = pd.DatetimeIndex(df.index)
        return df.sort_index()
    except Exception:
        return None

def _match_symbol_like(colval, strike, opt_type):
    """Return True if column value likely refers to strike+type (handles '21400CE', '21400 CE', etc)."""
    if pd.isna(colval):
        return False
    s = str(colval).upper().replace("-", " ").replace("_", " ")
    # look for patterns "21400CE" or "21400 CE" or "21400 CE.NFO" etc
    patterns = [
        rf"\b{strike}\s*{opt_type}\b",
        rf"\b{strike}{opt_type}\b",
        rf"\b{strike}\b.*\b{opt_type}\b",
        rf"\b{strike}\b"
    ]
    for p in patterns:
        if re.search(p, s):
            return True
    return False

def _filter_combined_for_strike(df, strike, opt_type, expiry_date):
    """
    Try several heuristics to filter the combined parquet for the specific strike/type/expiry.
    Returns subset DataFrame or None.
    """
    df2 = _ensure_datetime_index(df)
    if df2 is None:
        logger.debug("Combined DF has no datetime index/column")
        return None

    cols = {c.lower(): c for c in df2.columns}
    logger.info("Combined file columns: %s", list(df2.columns)[:50])

    # 1) Symbol-like columns
    for candidate in ["symbol", "ticker", "instrument", "name"]:
        if candidate in cols:
            col = cols[candidate]
            mask = df2[col].astype(str).apply(lambda v: _match_symbol_like(v, strike, opt_type))
            sub = df2[mask]
            if not sub.empty:
                # try to filter expiry if expiry column present
                if "expiry" in cols:
                    try:
                        exp_col = cols["expiry"]
                        sub = sub[pd.to_datetime(sub[exp_col]).dt.date == pd.to_datetime(expiry_date).date()]
                    except Exception:
                        pass
                logger.info("Matched via %s column, rows=%d", col, len(sub))
                return sub

    # 2) Explicit strike / option type columns
    strike_col = None
    optcol = None
    for c_lower, c in cols.items():
        if "strike" in c_lower:
            strike_col = c
        if any(x in c_lower for x in ("optiontype", "optype", "type", "opt_type")):
            optcol = c
    if strike_col:
        mask = (pd.to_numeric(df2[strike_col], errors="coerce") == strike)
        if optcol:
            mask = mask & df2[optcol].astype(str).str.upper().str.contains(opt_type.upper())
        if "expiry" in cols:
            try:
                exp_col = cols["expiry"]
                mask = mask & (pd.to_datetime(df2[exp_col]).dt.date == pd.to_datetime(expiry_date).date())
            except Exception:
                pass
        sub = df2[mask]
        if not sub.empty:
            logger.info("Matched via strike/option columns, rows=%d", len(sub))
            return sub

    # 3) If none matched, attempt to parse symbol-like values from all string columns (last resort)
    string_cols = [c for c in df2.columns if df2[c].dtype == object]
    for col in string_cols:
        mask = df2[col].astype(str).apply(lambda v: _match_symbol_like(v, strike, opt_type))
        sub = df2[mask]
        if not sub.empty:
            logger.info("Matched via generic string column %s, rows=%d", col, len(sub))
            # try expiry filter
            if "expiry" in cols:
                try:
                    exp_col = cols["expiry"]
                    sub = sub[pd.to_datetime(sub[exp_col]).dt.date == pd.to_datetime(expiry_date).date()]
                except Exception:
                    pass
            return sub

    logger.info("No matching rows found in combined file for %s%s expiry %s", strike, opt_type, expiry_date)
    return None

# S3 storage options (used when local file not present)
S3_STORAGE_OPTIONS = {
    "anon": False,
    "key": "5c8ea9c516abfc78987bc98c70d2868a",
    "secret": "0cf64f9f0b64f6008cf5efe1529c6772daa7d7d0822f5db42a7c6a1e41b3cadf",
    "client_kwargs": {"endpoint_url": "https://cbabd13f6c54798a9ec05df5b8070a6e.r2.cloudflarestorage.com", "region_name": "auto"},
}
S3_PREFIX = "desiquant/data/candles/NIFTY"

def _read_parquet_s3(path):
    """Read parquet from the R2 S3 bucket path (path without s3://)."""
    uri = path if path.startswith("s3://") else f"s3://{path}"
    try:
        df = pd.read_parquet(uri, storage_options=S3_STORAGE_OPTIONS)
        return df
    except Exception as e:
        logger.debug("Failed to read s3 path %s : %s", uri, e)
        return None

def load_strike_data_local(strike: int, opt_type: str, expiry_date="2023-12-28"):
    """
    Load data for a single strike/type.
    Order:
      1) try local per-strike parquet under data/
      2) try combined LOCAL_COMBINED
      3) try S3 per-strike parquet under the provided R2 bucket
    """
    # 1) local per-file candidates (unchanged)
    filename_candidates = [
        DATA_DIR / "desiquant" / "data" / "candles" / "NIFTY" / expiry_date / f"{strike}{opt_type}.parquet",
        DATA_DIR / "desiquant" / "data" / "candles" / "NIFTY" / expiry_date / f"{strike}{opt_type}.parquet.gz",
        DATA_DIR / f"{expiry_date}_{strike}{opt_type}.parquet",
        DATA_DIR / f"{strike}{opt_type}.parquet",
    ]
    for p in filename_candidates:
        if p.exists():
            try:
                df = pd.read_parquet(p)
                df = _ensure_datetime_index(df)
                if df is not None:
                    logger.info("Loaded per-strike local file %s rows=%d", p, len(df))
                    return df
            except Exception as e:
                logger.debug("Failed to read local %s : %s", p, e)

    # 2) try combined local file
    if LOCAL_COMBINED.exists():
        try:
            df_all = pd.read_parquet(LOCAL_COMBINED)
            logger.info("Loaded combined parquet, rows=%d", len(df_all))
            sub = _filter_combined_for_strike(df_all, strike, opt_type, expiry_date)
            if sub is not None and not sub.empty:
                return _ensure_datetime_index(sub)
        except Exception as e:
            logger.exception("Error reading combined parquet: %s", e)

    # 3) Try reading directly from S3 R2 per-strike path
    s3_candidates = [
        f"{S3_PREFIX}/{expiry_date}/{strike}{opt_type}.parquet.gz",
        f"{S3_PREFIX}/{expiry_date}/{strike}{opt_type}.parquet",
        # sometimes filenames include a space or suffix
        f"{S3_PREFIX}/{expiry_date}/{strike} {opt_type}.parquet.gz",
        f"{S3_PREFIX}/{expiry_date}/{strike} {opt_type}.parquet",
    ]
    for s3p in s3_candidates:
        df = _read_parquet_s3(s3p)
        if df is not None:
            df = _ensure_datetime_index(df)
            if df is not None:
                logger.info("Loaded from S3 %s rows=%d", s3p, len(df))
                return df

    # 4) As a last resort, try scanning the expiry folder on S3 and match by symbol-like values
    try:
        fs = s3fs.S3FileSystem(key=S3_STORAGE_OPTIONS["key"], secret=S3_STORAGE_OPTIONS["secret"],
                               client_kwargs={"endpoint_url": S3_STORAGE_OPTIONS["client_kwargs"]["endpoint_url"]})
        prefix = f"{S3_PREFIX}/{expiry_date}/"
        try:
            files = fs.ls(prefix)
        except Exception:
            files = []
        for f in files:
            if not (f.endswith(".parquet") or f.endswith(".parquet.gz")):
                continue
            # quick heuristic: filename contains strike
            if str(strike) in f and opt_type.upper() in f.upper():
                df = _read_parquet_s3(f)
                if df is not None:
                    df = _ensure_datetime_index(df)
                    if df is not None:
                        logger.info("Loaded candidate from S3 %s", f)
                        return df
    except Exception as e:
        logger.debug("S3 folder scan failed: %s", e)

    logger.warning("Data not found for %s%s (expiry %s) locally or on S3", strike, opt_type, expiry_date)
    return None

def resample_1m_to_5m(df_1m: pd.DataFrame):
    # Expect df_1m indexed by Datetime; ensure numeric OHLC names
    df = df_1m.copy()
    # normalize column names if needed
    colmap = {}
    for c in df.columns:
        lc = c.lower()
        if lc in ("open", "o"):
            colmap[c] = "Open"
        if lc in ("high", "h"):
            colmap[c] = "High"
        if lc in ("low", "l"):
            colmap[c] = "Low"
        if lc in ("close", "c", "last", "lastprice"):
            colmap[c] = "Close"
    df = df.rename(columns=colmap)
    if not {"Open", "High", "Low", "Close"}.issubset(df.columns):
        # cannot resample without OHLC
        return None
    res = df.resample("5T").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"}).dropna()
    return res

def simulate_trade_on_series(df_5m: pd.DataFrame, buy_time: pd.Timestamp, target_pts=TARGET_POINTS, stop_pts=STOP_POINTS):
    """
    buy_time is the timestamp in original entry (e.g. 2023-12-26 10:30:00).
    As per spec: buying candle = buy_time - 1 candle (5 minutes) => e.g. 10:25.
    Buy price = HIGH of that candle.
    Then scan that candle and subsequent candles until first hit of target or stop.
    Returns dict with details or None if data missing.
    """
    if df_5m is None or df_5m.empty:
        return None
    # align times (ensure tz naive)
    buy_time = pd.to_datetime(buy_time)
    # buying candle timestamp = buy_time - 5 minutes (rounded to 5min floor)
    buy_candle_ts = (buy_time - pd.Timedelta(minutes=5)).floor("5T")
    if buy_candle_ts not in df_5m.index:
        # If exact timestamp missing, try nearest prior index
        prior_idx = df_5m.index[df_5m.index <= buy_candle_ts]
        if prior_idx.empty:
            return None
        buy_candle_ts = prior_idx[-1]

    buy_row = df_5m.loc[buy_candle_ts]
    buy_price = float(buy_row["High"])

    target_price = buy_price + target_pts
    stop_price = buy_price - stop_pts

    # scan from buy_candle_ts onwards
    scan = df_5m.loc[buy_candle_ts:]
    outcome = "none"
    hit_time = None
    for t, row in scan.iterrows():
        h = float(row["High"])
        l = float(row["Low"])
        # check target first (if both happen same candle, prioritize target)
        if h >= target_price:
            outcome = "target"
            hit_time = t
            break
        if l <= stop_price:
            outcome = "stoploss"
            hit_time = t
            break

    return {
        "buy_candle_ts": buy_candle_ts,
        "buy_price": buy_price,
        "target_price": target_price,
        "stop_price": stop_price,
        "outcome": outcome,
        "hit_time": hit_time,
    }

def build_strike_list_from_entrypoints():
    if not ENTRY_FILE.exists():
        raise FileNotFoundError(f"{ENTRY_FILE} not found")
    df = pd.read_excel(ENTRY_FILE)
    if "Type" not in df.columns or "ClosePrice" not in df.columns or "Time" not in df.columns:
        # user specified Time column exists in excel
        # allow "time" column or try to infer last column as time
        raise ValueError("entrypoints.xlsx must contain 'Type', 'ClosePrice' and 'Time' columns")
    strikes = []
    # also keep rows details so we can map back entry time per row
    rows = []
    for _, r in df.iterrows():
        type_val = r["Type"]
        cp = r["ClosePrice"]
        time_val = r["Time"]
        try:
            close_price = float(cp)
        except Exception:
            continue
        # time string may be like '2023-12-26 10:30:00'
        time_ts = pd.to_datetime(time_val)
        s = strikes_from_entry_row(type_val, close_price)
        for st, tp in s:
            strikes.append((st, tp))
            rows.append({"strike": st, "type": tp, "entry_time": time_ts})
    # deduplicate by strike+type but preserve one entry_time per original row by using rows list
    # return rows list (unique by strike+type+entry_time) and a deduped strike list
    unique_pairs = {}
    for r in rows:
        key = (r["strike"], r["type"], r["entry_time"])
        unique_pairs[key] = r
    rows_unique = list(unique_pairs.values())
    dedup_strikes = sorted({(r["strike"], r["type"]) for r in rows_unique}, key=lambda x: (x[0], x[1]))
    return rows_unique, dedup_strikes

def main_process(expiry_date="2023-12-28", target_pts=TARGET_POINTS, stop_pts=STOP_POINTS, day_needed="2023-12-26"):
    # Build list from entrypoints
    rows_unique, dedup_strikes = build_strike_list_from_entrypoints()

    results = []
    counts = {"target": 0, "stoploss": 0, "none": 0}

    # For each row (strike+type+entry_time), load instrument minute data, restrict to day_needed, resample 5m and simulate
    for r in rows_unique:
        strike = r["strike"]
        opt_type = r["type"]
        entry_time = r["entry_time"]

        df_min = load_strike_data_local(strike, opt_type, expiry_date=expiry_date)
        if df_min is None:
            print(f"⚠️ Data not found for {strike}{opt_type}, expiry {expiry_date}. Skipping.")
            continue

        # filter to day_needed only
        df_min = df_min.loc[pd.to_datetime(day_needed) : pd.to_datetime(day_needed) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)]
        if df_min.empty:
            print(f"⚠️ No data on {day_needed} for {strike}{opt_type}. Skipping.")
            continue

        # resample 5m
        df_5m = resample_1m_to_5m(df_min)
        if df_5m is None or df_5m.empty:
            print(f"⚠️ Cannot resample for {strike}{opt_type}. Skipping.")
            continue

        sim = simulate_trade_on_series(df_5m, entry_time, target_pts=target_pts, stop_pts=stop_pts)
        if sim is None:
            print(f"⚠️ Simulation failed for {strike}{opt_type}.")
            continue

        outcome = sim["outcome"]
        counts[outcome] = counts.get(outcome, 0) + 1

        results.append({
            "strike": strike,
            "type": opt_type,
            "expiry": expiry_date,
            "day": day_needed,
            "buy_candle_ts": sim["buy_candle_ts"],
            "buy_price": sim["buy_price"],
            "target_price": sim["target_price"],
            "stop_price": sim["stop_price"],
            "outcome": sim["outcome"],
            "hit_time": sim["hit_time"],
        })

    # write results to excel
    if results:
        out_df = pd.DataFrame(results)
        # format timestamps
        out_df["buy_candle_ts"] = pd.to_datetime(out_df["buy_candle_ts"])
        out_df["hit_time"] = pd.to_datetime(out_df["hit_time"])
        # save
        out_df.to_excel(OUTPUT_FILE, index=False)
        # append summary sheet (if using ExcelWriter)
        # simple print summary
        print(f"Saved results to {OUTPUT_FILE}. Summary: targets={counts['target']}, stoplosses={counts['stoploss']}, none={counts['none']}")
    else:
        print("No results to save.")

    return results, counts

if __name__ == "__main__":
    main_process()