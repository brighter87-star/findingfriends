import os

from dotenv import load_dotenv

load_dotenv()
POLYGON_APIKEY = os.getenv("POLYGON_APIKEY")

# File PATHs
TICKERLIST_PATH = os.path.join("src", "data", "cached", "tickers")
OHLC_PATH = os.path.join("src", "data", "cached", "OHLC")
TICKER_OVERVIEW_PATH = os.path.join("src", "data", "cached", "ticker_overview")
ETF_HOLDINGS_PATH = os.path.join("src", "data", "cached", "ETF_Holdings")

# URLs
POLYGON_OHLC_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks"
POLYGON_TICKER_OVERVIEW_URL = "https://api.polygon.io/v3/reference/tickers"
NASDAQURL = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt"
OTHERURL = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt"

if __name__ == "__main__":
    print(POLYGON_APIKEY)
