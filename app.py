from flask import Flask, render_template, jsonify, request
import pandas as pd
import pandas_ta as ta
import os
from pathlib import Path
import time
from threading import Thread

app = Flask(__name__)

# -------------------- Local Data Config --------------------
DATA_DIR = Path("data")  # Folder containing monthly NIFTY .txt files
CACHE_FILE = Path("data/nifty_combined.parquet")


def read_all_nifty_txt_files():
    """Reads and merges all monthly NIFTY .txt files into a single DataFrame."""
    print("📂 Reading all NIFTY monthly txt files...")
    all_files = sorted(DATA_DIR.glob("*.txt"))
    if not all_files:
        raise FileNotFoundError("No .txt files found in /data folder")

    dfs = []
    for file in all_files:
        try:
            df = pd.read_csv(
                file,
                header=None,
                names=["Symbol", "Date", "Time", "Open", "High", "Low", "Close", "X1", "X2"],
            )
            df["Datetime"] = pd.to_datetime(df["Date"].astype(str) + " " + df["Time"])
            df = df[["Datetime", "Open", "High", "Low", "Close"]].sort_values("Datetime")
            dfs.append(df)
        except Exception as e:
            print(f"⚠️ Error reading {file.name}: {e}")

    full_df = pd.concat(dfs, ignore_index=True).drop_duplicates(subset="Datetime")
    full_df.to_parquet(CACHE_FILE, index=False)
    print(f"✅ Combined {len(dfs)} files → {len(full_df)} rows saved to cache.")
    return full_df


def load_cached_or_fresh_data():
    """Load data from cache or rebuild from txt files if needed."""
    if CACHE_FILE.exists():
        file_age = time.time() - CACHE_FILE.stat().st_mtime
        if file_age < 3600:  # 1 hour cache
            print(f"⚡ Using cached parquet ({file_age/60:.1f} min old)")
            return pd.read_parquet(CACHE_FILE)
        else:
            print("♻️ Cache old (>1h), rebuilding from txt files...")
            return read_all_nifty_txt_files()
    else:
        return read_all_nifty_txt_files()


def prepare_chart_data(limit=1000, before_ts=None, interval="1m"):
    """Return data formatted for chart display with resampling based on interval."""
    df = load_cached_or_fresh_data()
    df = df.set_index("Datetime").sort_index()

    # Supported intervals
    interval_map = {
        "1m": "1min",
        "3m": "3min",
        "5m": "5min",
        "10m": "10min",
        "15m": "15min",
        "30m": "30min",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "1d": "1D"
    }
    freq = interval_map.get(interval, "1min")

    # Resample all candles
    df = df.resample(freq).agg({
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last"
    }).dropna()

    # ✅ compute indicators BEFORE slicing
    df["SMA_5"] = ta.sma(df["Close"], length=5)
    df["SMA_20"] = ta.sma(df["Close"], length=20)
    df["RSI"] = ta.rsi(df["Close"], length=14)

    # Handle infinite scroll
    if before_ts:
        cutoff = pd.to_datetime(int(before_ts), unit="s")
        df = df[df.index < cutoff]

    # ✅ slice only after indicators are calculated
    df = df.tail(limit)

    # Convert to chart-compatible lists
    candles = [
        {"time": int(ts.timestamp()), "open": r.Open, "high": r.High, "low": r.Low, "close": r.Close}
        for ts, r in df.iterrows()
    ]
    sma5 = [{"time": int(ts.timestamp()), "value": r.SMA_5} for ts, r in df.iterrows() if not pd.isna(r.SMA_5)]
    sma20 = [{"time": int(ts.timestamp()), "value": r.SMA_20} for ts, r in df.iterrows() if not pd.isna(r.SMA_20)]
    rsi = [{"time": int(ts.timestamp()), "value": r.RSI if not pd.isna(r.RSI) else 0} for ts, r in df.iterrows()]



    return candles, sma5, sma20, rsi



# -------------------- Background refresher --------------------
def refresh_cache_periodically():
    """Rebuild combined cache every hour."""
    while True:
        try:
            read_all_nifty_txt_files()
            print("🔄 Cache refreshed successfully.")
        except Exception as e:
            print(f"⚠️ Cache refresh failed: {e}")
        time.sleep(3600)


# -------------------- Routes --------------------
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/data/nifty')
def get_nifty_data():
    limit = int(request.args.get("limit", 1000))
    before_ts = request.args.get("before")
    interval = request.args.get("interval", "1m")
    candles, sma5, sma20, rsi = prepare_chart_data(limit, before_ts, interval)
    return jsonify({
        "candlestick": candles,
        "sma5": sma5,
        "sma20": sma20,
        "rsi": rsi
    })


# -------------------- App entry --------------------
if __name__ == '__main__':
    Thread(target=refresh_cache_periodically, daemon=True).start()
    print("🚀 Starting Flask server at http://127.0.0.1:5000")
    app.run(debug=True)
