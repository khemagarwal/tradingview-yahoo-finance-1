from flask import Flask, render_template, jsonify, request
import yfinance as yf
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
import json
import os
from models import db, Symbol
import requests


app = Flask(__name__)

# Configure SQLAlchemy with SQLite
basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'symbols.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize database
db.init_app(app)

def fetch_yahoo_data_chunked(ticker, interval, start_date, end_date, rsi_period=14):
    """
    Fetches Yahoo Finance data in chunks (useful for intraday intervals like 5m).
    Returns combined dataframe for the full date range.
    """
    import math

    max_chunk_days = 59  # Yahoo allows ~60 days for intraday
    all_data = []
    current_start = start_date

    while current_start < end_date:
        current_end = min(current_start + timedelta(days=max_chunk_days), end_date)
        print(f"Fetching chunk: {current_start.date()} → {current_end.date()}")

        try:
            data_chunk = yf.download(
                ticker,
                start=current_start,
                end=current_end,
                interval=interval,
                progress=False
            )

            # --- Flatten multi-index columns if Yahoo returned them (e.g., ('Close','^NSEI')) ---
            if isinstance(data_chunk.columns, pd.MultiIndex):
                data_chunk.columns = [col[0] for col in data_chunk.columns]

            if not data_chunk.empty:
                all_data.append(data_chunk)
            else:
                print(f"No data returned for {ticker} in chunk {current_start.date()} → {current_end.date()}")

        except Exception as e:
            print(f"Error fetching chunk {current_start.date()}: {e}")


        # Move to next chunk
        current_start = current_end + timedelta(days=1)

    if not all_data:
        return pd.DataFrame()

    data = pd.concat(all_data)

    # If somehow combined DataFrame is empty, return empty DataFrame
    if data.empty:
        print("Combined data is empty after concatenation.")
        return pd.DataFrame()

    # If columns are still multi-index (safety), flatten them
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [col[0] for col in data.columns]

    # If 'Close' not present after flattening, nothing to compute
    if 'Close' not in data.columns:
        print("No 'Close' column found after flattening — returning empty DataFrame.")
        return pd.DataFrame()


    data = data[~data.index.duplicated(keep='first')]
    data.sort_index(inplace=True)

    # Add SMA/RSI columns
    data['SMA_5'] = ta.sma(data['Close'], length=5)
    data['SMA_20'] = ta.sma(data['Close'], length=20)
    data['RSI'] = ta.rsi(data['Close'], length=rsi_period)
    return data


def fetch_yahoo_data(ticker, interval, rsi_period=14):
    # ticker = yf.Ticker(ticker)
    symbol = ticker

    end_date = datetime.now()
    if interval in ['1m', '5m']:
        start_date = end_date - timedelta(days=7)
    elif interval in ['15m', '60m']:
        # start_date = end_date - timedelta(days=59)
        start_date = datetime(2020, 1, 1)
    elif interval == '1d':
        # start_date = end_date - timedelta(days=365*5)
        start_date = datetime(2020, 1, 1)
    elif interval == '1wk':
        start_date = end_date - timedelta(weeks=365*5)
    elif interval == '1mo':
        start_date = end_date - timedelta(days=365*5)

    # data = ticker.history(start=start_date, end=end_date, interval=interval)

    if interval in ['1m', '2m', '5m', '15m', '30m', '60m']:
        data = fetch_yahoo_data_chunked(ticker, interval, start_date, end_date, rsi_period)
    else:
        data = yf.Ticker(symbol).history(start=start_date, end=end_date, interval=interval)

    # --- Safety guards: flatten multi-index columns if present and guard empty data ---
    if isinstance(data, pd.DataFrame) and not data.empty:
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [col[0] for col in data.columns]
        if 'Close' not in data.columns:
            print(f"No 'Close' column for {symbol} with interval {interval} — returning empty result.")
            return [], [], [], [], []
    else:
        # No data returned
        print(f"No data returned for {symbol} with interval {interval}")
        return [], [], [], [], []


    data['SMA_5'] = ta.sma(data['Close'], length=5)
    data['SMA_20'] = ta.sma(data['Close'], length=20)
    data['RSI'] = ta.rsi(data['Close'], length=rsi_period)

        # ---- Detect SMA crossovers ----
    data['Signal'] = None
    for i in range(1, len(data)):
        prev5, prev20 = data['SMA_5'].iloc[i - 1], data['SMA_20'].iloc[i - 1]
        curr5, curr20 = data['SMA_5'].iloc[i], data['SMA_20'].iloc[i]

        if pd.notna(prev5) and pd.notna(prev20):
            if prev5 < prev20 and curr5 > curr20:
                data.iloc[i, data.columns.get_loc('Signal')] = 'BUY'
            elif prev5 > prev20 and curr5 < curr20:
                data.iloc[i, data.columns.get_loc('Signal')] = 'SELL'

    candlestick_data = [
        {
            'time': int(row.Index.timestamp()),
            'open': row.Open,
            'high': row.High,
            'low': row.Low,
            'close': row.Close
        }
        for row in data.itertuples()
    ]

    sma5_data = [
        {
            'time': int(row.Index.timestamp()),
            'value': row.SMA_5
        }
        for row in data.itertuples() if not pd.isna(row.SMA_5)
    ]

    sma20_data = [
        {
            'time': int(row.Index.timestamp()),
            'value': row.SMA_20
        }
        for row in data.itertuples() if not pd.isna(row.SMA_20)
    ]


    rsi_data = [
        {
            'time': int(row.Index.timestamp()),
            'value': row.RSI if not pd.isna(row.RSI) else 0  # Convert NaN to zero
        }
        for row in data.itertuples()
    ]

    
    # ---- Markers for BUY/SELL ----
    signal_markers = []
    for row in data.itertuples():
        if row.Signal == 'BUY':
            signal_markers.append({
                'time': int(row.Index.timestamp()),
                'position': 'belowBar',
                'color': 'green',
                'shape': 'arrowUp',
                'text': 'BUY'
            })
        elif row.Signal == 'SELL':
            signal_markers.append({
                'time': int(row.Index.timestamp()),
                'position': 'aboveBar',
                'color': 'red',
                'shape': 'arrowDown',
                'text': 'SELL'
            })

    return candlestick_data, sma5_data, sma20_data, rsi_data, signal_markers


    url = f'https://www.nseindia.com/api/option-chain-indices?symbol={symbol.upper()}'
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': f'https://www.nseindia.com/option-chain?symbol={symbol.upper()}'
    }
    session = requests.Session()
    session.headers.update(headers)
    try:
        response = session.get(url, timeout=10)
        data = response.json()
        records = data['records']['data']
        
        calls, puts = [], []
        for rec in records:
            if 'CE' in rec:
                calls.append({
                    'strikePrice': rec['strikePrice'],
                    'lastPrice': rec['CE'].get('lastPrice'),
                    'change': rec['CE'].get('change'),
                    'openInterest': rec['CE'].get('openInterest'),
                    'volume': rec['CE'].get('totalTradedVolume'),
                    'expiryDate': rec['expiryDate']
                })
            if 'PE' in rec:
                puts.append({
                    'strikePrice': rec['strikePrice'],
                    'lastPrice': rec['PE'].get('lastPrice'),
                    'change': rec['PE'].get('change'),
                    'openInterest': rec['PE'].get('openInterest'),
                    'volume': rec['PE'].get('totalTradedVolume'),
                    'expiryDate': rec['expiryDate']
                })
        return {'calls': calls, 'puts': puts}
    except Exception as e:
        return {'error': str(e)}


def fetch_nse_option_chart(symbol):
    """
    Fetch basic price data for a single NIFTY option contract.
    Example symbol: 'NIFTY 26000 NOV 4 CE'
    """
    try:
        base_url = 'https://www.nseindia.com'
        url = f'{base_url}/api/option-chain-indices?symbol=NIFTY'
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': f'{base_url}/option-chain?symbol=NIFTY'
        }

        session = requests.Session()
        session.headers.update(headers)
        response = session.get(url, timeout=10)
        data = response.json()

        # Extract target option
        target_strike, target_expiry, target_type = symbol.split()[1:4]
        target_strike = float(target_strike)
        target_type = target_type.upper()

        records = data.get('records', {}).get('data', [])
        option_points = []
        now = datetime.now()

        for rec in records:
            expiry = rec.get('expiryDate')
            strike = rec.get('strikePrice')
            if expiry and strike == target_strike:
                opt = rec.get('CE' if target_type == 'CE' else 'PE')
                if opt and expiry.upper().startswith(target_expiry.upper()):
                    last_price = opt.get('lastPrice', 0)
                    option_points.append({
                        'time': int(now.timestamp()),
                        'open': last_price,
                        'high': last_price * 1.01,
                        'low': last_price * 0.99,
                        'close': last_price,
                    })
                    break

        # Simple flat SMA/RSI placeholders
        sma_data = [{'time': p['time'], 'value': p['close']} for p in option_points]
        rsi_data = [{'time': p['time'], 'value': 50} for p in option_points]

        return option_points, sma_data, sma_data, rsi_data, []
    except Exception as e:
        print(f"Error fetching NSE option chart: {e}")
        return [], [], [], [], []


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data/<ticker>/<interval>/<int:rsi_period>')
def get_data(ticker, interval, rsi_period):
    candlestick_data, sma5_data, sma20_data, rsi_data, signal_markers = fetch_yahoo_data(ticker, interval, rsi_period=rsi_period)
    return jsonify({
        'candlestick': candlestick_data,
        'sma5': sma5_data,
        'sma20': sma20_data,
        'rsi': rsi_data,
        'signals': signal_markers
    })

@app.route('/api/nifty_option')
def get_nifty_option():
    symbol = 'NIFTY 26000 NOV 4 CE'
    candles, sma5, sma20, rsi, signals = fetch_nse_option_chart(symbol)
    return jsonify({
        'candlestick': candles,
        'sma5': sma5,
        'sma20': sma20,
        'rsi': rsi,
        'signals': signals
    })



# Create database tables on startup if they don't exist
with app.app_context():
    db.create_all()
    
    # Add default symbols if the database is empty
    if Symbol.query.count() == 0:
        default_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'NFLX']
        for ticker in default_symbols:
            if not Symbol.query.filter_by(ticker=ticker).first():
                symbol = Symbol(ticker=ticker)
                db.session.add(symbol)
        db.session.commit()
        print(f'Added {len(default_symbols)} default symbols')

@app.route('/api/symbols')
def get_symbols():
    # Get symbols from database
    db_symbols = Symbol.query.all()
    symbol_list = [symbol.ticker for symbol in db_symbols]
    
    # Get real quotes for symbols
    try:
        if not symbol_list:
            return jsonify([])
            
        symbols_str = ' '.join(symbol_list)
        tickers = yf.Tickers(symbols_str)
        
        symbols_data = []
        for symbol in db_symbols:
            try:
                ticker_info = tickers.tickers[symbol.ticker].info
                quote_data = {
                    'id': symbol.id,
                    'symbol': symbol.ticker,
                    'price': ticker_info.get('currentPrice', 0),
                    'change': ticker_info.get('regularMarketChangePercent', 0),
                    'name': ticker_info.get('shortName', symbol.ticker),
                }
                symbols_data.append(quote_data)
            except Exception as e:
                # Fallback data if we can't get info for a particular symbol
                symbols_data.append({
                    'id': symbol.id,
                    'symbol': symbol.ticker,
                    'price': 0,
                    'change': 0,
                    'name': symbol.ticker,
                })
                print(f"Error getting data for {symbol.ticker}: {e}")
        
        return jsonify(symbols_data)
    
    except Exception as e:
        print(f"Error fetching quotes: {e}")
        # Fallback to just returning the symbols without data
        return jsonify([{'id': s.id, 'symbol': s.ticker, 'price': 0, 'change': 0, 'name': s.ticker} for s in db_symbols])

@app.route('/api/symbols', methods=['POST'])
def add_symbol():
    data = request.json
    if not data or 'symbol' not in data:
        return jsonify({'error': 'Symbol is required'}), 400
    
    ticker = data['symbol'].strip().upper()
    if not ticker:
        return jsonify({'error': 'Symbol cannot be empty'}), 400
    
    # Check if symbol already exists
    existing = Symbol.query.filter_by(ticker=ticker).first()
    if existing:
        return jsonify({'error': 'Symbol already exists', 'symbol': existing.to_dict()}), 409
    
    # Validate symbol with yfinance
    try:
        info = yf.Ticker(ticker).info
        if 'regularMarketPrice' not in info and 'currentPrice' not in info:
            return jsonify({'error': 'Invalid symbol'}), 400
            
        # Add symbol to database
        symbol = Symbol(ticker=ticker, name=info.get('shortName', ticker))
        db.session.add(symbol)
        db.session.commit()
        
        return jsonify({'message': 'Symbol added successfully', 'symbol': symbol.to_dict()}), 201
    except Exception as e:
        return jsonify({'error': f'Error adding symbol: {str(e)}'}), 400

@app.route('/api/symbols/<int:symbol_id>', methods=['DELETE'])
def delete_symbol(symbol_id):
    symbol = Symbol.query.get_or_404(symbol_id)
    db.session.delete(symbol)
    db.session.commit()
    return jsonify({'message': 'Symbol deleted successfully'}), 200

if __name__ == '__main__':
    app.run(debug=True)