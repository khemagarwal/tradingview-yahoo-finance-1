import s3fs
import pandas as pd
import numpy as np
from datetime import datetime
import logging

# ...existing code...

# S3 credentials (as provided)
S3_PARAMS = {
    "endpoint_url": "https://cbabd13f6c54798a9ec05df5b8070a6e.r2.cloudflarestorage.com",
    "key": "5c8ea9c516abfc78987bc98c70d2868a",
    "secret": "0cf64f9f0b64f6008cf5efe1529c6772daa7d7d0822f5db42a7c6a1e41b3cadf",
    "client_kwargs": {"region_name": "auto"},
}

S3_PREFIX = "desiquant/data/candles/NIFTY/2023-12-"  # match all December 2023 folders
LOCAL_CACHE = "data/nifty_options_2023_12.parquet"  # optional local cache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _build_s3_fs():
    return s3fs.S3FileSystem(anon=False, key=S3_PARAMS["key"], secret=S3_PARAMS["secret"],
                             client_kwargs=S3_PARAMS.get("client_kwargs", {}),
                             endpoint_url=S3_PARAMS.get("endpoint_url"))


def _find_december_files(fs):
    # glob supports patterns like prefix*/*.parquet*
    pattern = S3_PREFIX + "*/*.parquet*"
    try:
        files = fs.glob(pattern)
    except Exception:
        # fallback to listing prefix then globs
        files = []
        try:
            prefixes = fs.ls("desiquant/data/candles/NIFTY", detail=False)
            for p in prefixes:
                if p.startswith("desiquant/data/candles/NIFTY/2023-12-"):
                    files += fs.ls(p, detail=False)
        except Exception:
            pass
    # normalize to s3 keys (no leading s3://)
    return [f for f in files if f.endswith(".parquet") or f.endswith(".parquet.gz")]


def _read_parquet_s3(path, storage_options):
    uri = f"s3://{path}" if not path.startswith("s3://") else path
    return pd.read_parquet(uri, storage_options=storage_options)


def _detect_datetime_column(df):
    # find a datetime-like column
    for c in df.columns:
        lc = c.lower()
        if lc in ("datetime", "date", "timestamp", "time") or "date" in lc:
            return c
    # fallback to index if datetime-like
    if isinstance(df.index, pd.DatetimeIndex):
        return None
    raise ValueError("No datetime-like column found")


def load_december_2023(reload=False, limit_files=None):
    """
    Load and concatenate all December 2023 NIFTY option parquet files from S3.
    Returns a DataFrame indexed by Datetime (tz-naive, UTC assumed if present).
    If reload=False and LOCAL_CACHE exists, loads from local cache.
    """
    try:
        if not reload:
            try:
                df_cached = pd.read_parquet(LOCAL_CACHE)
                logger.info("Loaded local cache %s", LOCAL_CACHE)
                return df_cached
            except Exception:
                pass

        fs = _build_s3_fs()
        files = _find_december_files(fs)
        if limit_files:
            files = files[:limit_files]
        logger.info("Found %d parquet files for December 2023", len(files))

        dfs = []
        for f in files:
            try:
                df = _read_parquet_s3(f, storage_options={
                    "anon": False,
                    "key": S3_PARAMS["key"],
                    "secret": S3_PARAMS["secret"],
                    "client_kwargs": S3_PARAMS.get("client_kwargs", {}),
                    "endpoint_url": S3_PARAMS.get("endpoint_url"),
                })
                # detect datetime
                dt_col = _detect_datetime_column(df)
                if dt_col:
                    df[dt_col] = pd.to_datetime(df[dt_col])
                    df = df.rename(columns={dt_col: "Datetime"})
                elif isinstance(df.index, pd.DatetimeIndex):
                    df = df.reset_index().rename(columns={df.index.name or "index": "Datetime"})
                    df["Datetime"] = pd.to_datetime(df["Datetime"])
                else:
                    logger.warning("Skipping file (no datetime): %s", f)
                    continue

                df = df.set_index("Datetime").sort_index()
                # keep only OHLC columns if present, normalized names
                cols = {c: c for c in df.columns}
                # attempt to normalize: common names Open/High/Low/Close
                for cand in ["open", "OPEN", "Open"]:
                    if cand in df.columns:
                        cols[cand] = "Open"
                for cand in ["high", "HIGH", "High"]:
                    if cand in df.columns:
                        cols[cand] = "High"
                for cand in ["low", "LOW", "Low"]:
                    if cand in df.columns:
                        cols[cand] = "Low"
                for cand in ["close", "CLOSE", "Close", "lastPrice", "last"]:
                    if cand in df.columns and "Close" not in df.columns:
                        cols[cand] = "Close"
                df = df.rename(columns=cols)
                # ensure OHLC exist
                if not {"Open", "High", "Low", "Close"}.issubset(df.columns):
                    logger.warning("File missing OHLC, skipping: %s", f)
                    continue

                dfs.append(df[["Open", "High", "Low", "Close"]])
            except Exception as e:
                logger.exception("Error reading %s: %s", f, e)
                continue

        if not dfs:
            raise RuntimeError("No valid parquet files found for December 2023")

        full = pd.concat(dfs, axis=0)
        full = full[~full.index.duplicated(keep="first")].sort_index()
        # optional: store local cache
        try:
            full.to_parquet(LOCAL_CACHE, index=True)
        except Exception:
            pass
        logger.info("Concatenated dataframe rows: %d", len(full))
        return full
    except Exception as e:
        logger.exception("Failed to load December data: %s", e)
        raise


# --- Indicators ---
def sma(series, length):
    return series.rolling(window=length, min_periods=1).mean()


def rsi(series, length=9):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ma_up = up.ewm(alpha=1 / length, adjust=False).mean()
    ma_down = down.ewm(alpha=1 / length, adjust=False).mean()
    rs = ma_up / ma_down
    rsi_series = 100 - (100 / (1 + rs))
    return rsi_series


INTERVAL_MAP = {
    "1m": "1min", "3m": "3min", "5m": "5min", "10m": "10min",
    "15m": "15min", "30m": "30min", "1h": "1H", "2h": "2H",
    "4h": "4H", "1d": "1D"
}


def resample_and_format(df, interval="1m", limit=1000, before_ts=None, rsi_period=9, rsi_avg=3):
    """
    Resample the consolidated December dataframe to the requested interval,
    compute SMA_5, SMA_20, RSI and return frontend-ready dict:
    { "candles": [...], "sma5": [...], "sma20": [...], "rsi_base": [...], "rsi_avg": [...] }
    time values are epoch seconds (int).
    """
    if df is None or df.empty:
        return {"candles": [], "sma5": [], "sma20": [], "rsi_base": [], "rsi_avg": []}

    freq = INTERVAL_MAP.get(interval, "1min")
    res = df.resample(freq).agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"}).dropna()
    if res.empty:
        return {"candles": [], "sma5": [], "sma20": [], "rsi_base": [], "rsi_avg": []}

    # indicators
    res["SMA_5"] = sma(res["Close"], 5)
    res["SMA_20"] = sma(res["Close"], 20)
    res["RSI_Base"] = rsi(res["Close"], length=rsi_period)
    res["RSI_Avg"] = sma(res["RSI_Base"], rsi_avg)

    # filter by before_ts if provided
    if before_ts:
        cutoff = pd.to_datetime(int(before_ts), unit="s")
        res = res[res.index < cutoff]

    res = res.tail(limit)

    candles = [
        {"time": int(ts.timestamp()), "open": float(r.Open), "high": float(r.High), "low": float(r.Low), "close": float(r.Close)}
        for ts, r in res.iterrows()
    ]
    sma5 = [{"time": int(ts.timestamp()), "value": float(r.SMA_5)} for ts, r in res.iterrows() if not pd.isna(r.SMA_5)]
    sma20 = [{"time": int(ts.timestamp()), "value": float(r.SMA_20)} for ts, r in res.iterrows() if not pd.isna(r.SMA_20)]
    rsi_base = [{"time": int(ts.timestamp()), "value": float(r.RSI_Base) if not pd.isna(r.RSI_Base) else 0} for ts, r in res.iterrows()]
    rsi_avg_line = [{"time": int(ts.timestamp()), "value": float(r.RSI_Avg) if not pd.isna(r.RSI_Avg) else 0} for ts, r in res.iterrows()]

    return {
        "candles": candles,
        "sma5": sma5,
        "sma20": sma20,
        "rsi_base": rsi_base,
        "rsi_avg": rsi_avg_line,
        "rows": len(res),
        "from": res.index.min().isoformat(),
        "to": res.index.max().isoformat(),
    }


if __name__ == "__main__":
    # quick run to build cache and print summary
    df_all = load_december_2023(reload=False)
    out = resample_and_format(df_all, interval="1m", limit=20)
    print("Sample candles:", len(out["candles"]))
    print("Range:", out.get("from"), "→", out.get("to"))
    # show first rows
    if out["candles"]:
        print(out["candles"][:3])