from dotenv import load_dotenv
import os

load_dotenv()
POLYGON_APIKEY=os.getenv("POLYGON_APIKEY")

# File PATHs
TICKERLIST_PATH=os.path.join("src", "data", "cached", "tickers")
OHLC_PATH=os.path.join("src", "data", "cached", "OHLC")

# URLs
POLYGON_OHLC_URL="https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks"
NASDAQURL="ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt"
OTHERURL="ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt"

# Analysis 최소 기준
DollarVolume=100000

if __name__ == "__main__" :
    print(POLYGON_APIKEY)
