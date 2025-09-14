import pandas as pd
import requests, time, os
from src.utils.calc import get_date_list
from src.utils.config import POLYGON_OHLC_URL, POLYGON_APIKEY, TICKERLIST_PATH, OHLC_PATH

def get_ticker_all() -> list:
    with open(os.path.join(TICKERLIST_PATH, "tickerlist.txt"), "r") as f:
       tickers = [line.strip() for line in f if line.strip()] 
       return tickers

def get_OHLC_all(tickers, interval="day", date="2025-09-11") -> pd.DataFrame:
    response=requests.get(url=f"{POLYGON_OHLC_URL}/{date}?adjusted=true&apikey={POLYGON_APIKEY}")
    results_df=pd.DataFrame(response.json()['results'])
    filtered_df=results_df[results_df["T"].isin(tickers)]
    return filtered_df

def save_OHLC_for_days(tickers, last_date, days=30, interval="day"):
    date_list = get_date_list(days=days)
    os.makedirs(OHLC_PATH, exist_ok=True)

    for idx, date in enumerate(date_list):
        file_path=os.path.join(OHLC_PATH, f"ohlc_{date}.parquet")
        if os.path.exists(file_path):
            print(f"{file_path}는 이미 존재하므로 건너뛰고 진행합니다.\n")
            continue
        print(f"{date}의 데이터를 얻어옵니다.{idx+1}/{len(date_list)}")
        df = get_OHLC_all(tickers, date=date)
        df["date"] = pd.to_datetime(date)
        df.to_parquet(file_path, index=False)

        if (idx+1) % 5 == 0:
            time.sleep(60)
            print("\nAPI 제한 때문에 조금 쉬는 중이에요...")

if __name__ == "__main__" :
    tickers=get_ticker_all()
    save_OHLC_for_days(tickers=tickers, last_date="2025-09-12", days=1)
