import logging
import os
import time

import pandas as pd
import requests

from src.utils.calc import get_date_list
from src.utils.config import (
    OHLC_PATH,
    POLYGON_APIKEY,
    POLYGON_OHLC_URL,
    TICKERLIST_PATH,
)


def get_ticker_all() -> list:
    with open(os.path.join(TICKERLIST_PATH, "tickerlist.txt")) as f:
        tickers = [line.strip() for line in f if line.strip()]
        return tickers


def get_ohlc_all(tickers, interval="day", date="2025-09-11") -> pd.DataFrame:
    response = requests.get(
        url=f"{POLYGON_OHLC_URL}/{date}?adjusted=true&apikey={POLYGON_APIKEY}",
    )
    if response.status_code != 200:
        logging.error("API 요청 실패")
        return None

    results_df = pd.DataFrame(response.json()["results"])
    filtered_df = results_df[results_df["T"].isin(tickers)]
    return filtered_df


def save_ohlc_for_days(tickers, last_date, days=30, interval="day"):
    date_list = get_date_list(days=days)
    os.makedirs(OHLC_PATH, exist_ok=True)
    api_call_cnt = 0

    for idx, date in enumerate(date_list):
        try:
            file_path = os.path.join(OHLC_PATH, f"ohlc_{date}.parquet")
            if os.path.exists(file_path):
                print(f"{file_path}는 이미 존재하므로 건너뛰고 진행합니다.\n")
                continue
        except Exception:
            raise Exception("파일 존재 여부를 확인하는 과정에서 문제가 생겼어요.")

        try:
            print(f"{date}의 데이터를 얻어옵니다.{idx+1}/{len(date_list)}")
            api_call_cnt += 1
            df = get_ohlc_all(tickers, date=date)
        except Exception:
            raise Exception("API call의 과정에서 문제가 생겼어요.")

        if df is not None:
            df["date"] = pd.to_datetime(date)
            df.to_parquet(file_path, index=False)
        else:
            logging.warning("데이터 없음")

        if api_call_cnt % 5 == 0:
            time.sleep(60)
            print("\nAPI 제한 때문에 조금 쉬는 중이에요...")


if __name__ == "__main__":
    tickers = get_ticker_all()
    save_ohlc_for_days(tickers=tickers, last_date="2025-09-12", days=400)
