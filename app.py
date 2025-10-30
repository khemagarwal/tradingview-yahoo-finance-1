from flask import Flask, render_template, jsonify, request
import yfinance as yf
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
import json
import os
from models import db, Symbol

app = Flask(__name__)

# Configure SQLAlchemy with SQLite
basedir = os.path.abspath(os.path.dirname(__file__))
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'symbols.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize database
db.init_app(app)

def fetch_yahoo_data(ticker, interval, rsi_period=14):
    ticker = yf.Ticker(ticker)

    end_date = datetime.now()
    if interval in ['1m', '5m']:
        start_date = end_date - timedelta(days=7)
    elif interval in ['15m', '60m']:
        start_date = end_date - timedelta(days=59)
    elif interval == '1d':
        start_date = end_date - timedelta(days=365*5)
    elif interval == '1wk':
        start_date = end_date - timedelta(weeks=365*5)
    elif interval == '1mo':
        start_date = end_date - timedelta(days=365*5)

    data = ticker.history(start=start_date, end=end_date, interval=interval)
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