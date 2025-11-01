import yfinance as yf
df = yf.download("^NSEI", interval="5m", start="2024-01-01", end="2024-03-01")
print(df.head(), df.tail(), len(df))
