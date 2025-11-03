from flask import Flask, render_template, jsonify, request
import pandas as pd
import pandas_ta as ta
import s3fs
import os
from pathlib import Path
from models import db, Symbol
import time
from threading import Thread

app = Flask(__name__)

# -------------------- Database config --------------------
basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'symbols.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

# -------------------- R2 + Cache setup --------------------
R2_BUCKET_PATH = "desiquant/data/candles/NIFTY 50/"
LOCAL_CACHE_PATH = Path("data/nifty_latest.parquet")

def download_latest_r2_file(local_path):
    """Download the latest NIFTY 50 parquet file from Cloudflare R2."""
    print("🔍 Checking R2 for latest NIFTY file...")
    fs = s3fs.S3FileSystem(
        anon=False,
        key="5c8ea9c516abfc78987bc98c70d2868a",
        secret="0cf64f9f0b64f6008cf5efe1529c6772daa7d7d0822f5db42a7c6a1e41b3cadf",
        client_kwargs={"endpoint_url": "https://cbabd13f6c54798a9ec05df5b8070a6e.r2.cloudflarestorage.com"},
    )

    try:
        files = fs.ls(R2_BUCKET_PATH, detail=True)
        if not files:
            raise FileNotFoundError("No files found in R2 bucket path.")
    except Exception as e:
        print(f"❌ Error listing R2 files: {e}")
        raise

    latest = sorted(files, key=lambda x: x["LastModified"], reverse=True)[0]
    print(f"⬇️ Downloading latest parquet: {latest['Key']}")

    start = time.time()
    df = pd.read_parquet(f"s3://{latest['Key']}", filesystem=fs)
    df.to_parquet(local_path, index=False)
    print(f"✅ Downloaded and cached locally in {time.time() - start:.2f}s ({len(df)} rows)")
    return df


def fetch_nifty_r2_data():
    """Fetch NIFTY 50 data with local caching."""
    print("🔹 Starting fetch_nifty_r2_data()")

    local_file = LOCAL_CACHE_PATH
    local_file.parent.mkdir(exist_ok=True)

    # use cache if less than 1 hour old
    if local_file.exists():
        file_age = time.time() - local_file.stat().st_mtime
        if file_age < 3600:
            print(f"⚡ Using cached local file ({file_age/60:.1f} min old)")
            df = pd.read_parquet(local_file)
        else:
            print("♻️ Cached file old (>1h), refreshing from R2...")
            df = download_latest_r2_file(local_file)
    else:
        print("⬇️ No local cache found, downloading fresh from R2...")
        df = download_latest_r2_file(local_file)

    # process dataframe
    date_col = [c for c in df.columns if c.lower() in ["date", "datetime", "timestamp"]][0]
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.set_index(date_col).sort_index()
    df.rename(columns=lambda x: x.capitalize(), inplace=True)

    # compute indicators
    df["SMA_5"] = ta.sma(df["Close"], length=5)
    df["SMA_20"] = ta.sma(df["Close"], length=20)
    df["RSI"] = ta.rsi(df["Close"], length=14)

    # reduce to latest 1000 points for performance
    df = df.tail(1000)

    # prepare output
    candles = [{"time": int(ts.timestamp()), "open": r.Open, "high": r.High, "low": r.Low, "close": r.Close}
               for ts, r in df.iterrows()]
    sma5 = [{"time": int(ts.timestamp()), "value": r.SMA_5} for ts, r in df.iterrows() if not pd.isna(r.SMA_5)]
    sma20 = [{"time": int(ts.timestamp()), "value": r.SMA_20} for ts, r in df.iterrows() if not pd.isna(r.SMA_20)]
    rsi = [{"time": int(ts.timestamp()), "value": r.RSI if not pd.isna(r.RSI) else 0} for ts, r in df.iterrows()]

    print(f"✅ Data ready: {len(candles)} candles")
    return candles, sma5, sma20, rsi, []

def fetch_nifty_r2_data_dynamic(limit=1000, before_ts=None):
    """Load from local cache and slice data for infinite scroll."""
    local_file = Path("data/nifty_latest.parquet")
    if not local_file.exists():
        download_latest_r2_file(local_file)

    df = pd.read_parquet(local_file)
    date_col = [c for c in df.columns if c.lower() in ["date", "datetime", "timestamp"]][0]
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.set_index(date_col).sort_index()

    # if "before" is provided, filter older data
    if before_ts:
        cutoff = pd.to_datetime(int(before_ts), unit="s")
        df = df[df.index < cutoff]

    # take last N rows before the cutoff (or recent N)
    df = df.tail(limit)

    # compute indicators quickly
    df.rename(columns=lambda x: x.capitalize(), inplace=True)
    df["SMA_5"] = ta.sma(df["Close"], length=5)
    df["SMA_20"] = ta.sma(df["Close"], length=20)
    df["RSI"] = ta.rsi(df["Close"], length=14)

    candles = [{"time": int(ts.timestamp()), "open": r.Open, "high": r.High, "low": r.Low, "close": r.Close}
               for ts, r in df.iterrows()]
    sma5 = [{"time": int(ts.timestamp()), "value": r.SMA_5} for ts, r in df.iterrows() if not pd.isna(r.SMA_5)]
    sma20 = [{"time": int(ts.timestamp()), "value": r.SMA_20} for ts, r in df.iterrows() if not pd.isna(r.SMA_20)]
    rsi = [{"time": int(ts.timestamp()), "value": r.RSI if not pd.isna(r.RSI) else 0} for ts, r in df.iterrows()]
    return candles, sma5, sma20, rsi, []

# -------------------- Optional background cache refresher --------------------
def refresh_cache_periodically():
    """Background thread to refresh local cache every hour."""
    while True:
        try:
            download_latest_r2_file(LOCAL_CACHE_PATH)
            print("🔄 Cache refreshed successfully.")
        except Exception as e:
            print(f"⚠️ Cache refresh failed: {e}")
        time.sleep(3600)  # refresh every 1 hour

# -------------------- Routes --------------------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data/nifty')
def get_nifty_data():
    """Return recent or historical NIFTY candles depending on query params."""
    limit = int(request.args.get("limit", 1000))
    before_ts = request.args.get("before")  # optional UNIX timestamp

    candles, sma5, sma20, rsi, signals = fetch_nifty_r2_data_dynamic(limit, before_ts)
    return jsonify({
        "candlestick": candles,
        "sma5": sma5,
        "sma20": sma20,
        "rsi": rsi,
        "signals": signals
    })


@app.route('/api/symbols')
def get_symbols():
    # only show one symbol — NIFTY 50
    return jsonify([{"symbol": "NIFTY", "name": "NIFTY 50", "price": 0, "change": 0}])

# -------------------- App entry --------------------
if __name__ == '__main__':
    # start background refresher thread
    Thread(target=refresh_cache_periodically, daemon=True).start()

    print("🚀 Starting Flask server at http://127.0.0.1:5000")
    app.run(debug=True)
