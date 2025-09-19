import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests

from src.utils.calc import get_date_list
from src.utils.config import (
    OHLC_PATH,
    POLYGON_APIKEY,
    POLYGON_OHLC_URL,
    POLYGON_TICKER_OVERVIEW_URL,
    TICKER_OVERVIEW_PATH,
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


def _get_ticker_overview(ticker):
    df = pd.DataFrame()
    try:
        response = requests.get(
            url=f"{POLYGON_TICKER_OVERVIEW_URL}/{ticker}?apiKey={POLYGON_APIKEY}",
        )
        df = pd.DataFrame([response.json()["results"]])
    except Exception as e:
        logging.exception(f"{e}\n{ticker}를 가져오는데 실패함.")

    return df


def fetch_ticker_overview_all():
    ticker_list = get_ticker_all()
    df_all = pd.DataFrame([])
    for idx, ticker in enumerate(ticker_list):
        if (idx + 1) % 6 == 0:
            print("API call limit으로 쉬는 중...\n")
            time.sleep(60)
            continue
        print(f"{idx+1}/{len(ticker_list)}번째 종목({ticker}) 가져오는 중...")
        df_all = pd.concat([df_all, _get_ticker_overview(ticker)], ignore_index=True)

    print(df_all)

    _to_parquet_ticker_overview(df_all)


def _to_parquet_ticker_overview(df):
    os.makedirs(TICKER_OVERVIEW_PATH, exist_ok=True)
    today = pd.Timestamp.today().strftime("%Y-%m-%d")
    file_path = os.path.join(
        TICKER_OVERVIEW_PATH,
        f"ticker_overview_all-{today}.parquet",
    )
    try:
        df.to_parquet(file_path, index=False)
    except Exception as e:
        logging.exception(e)

    print("성공적으로 parquet으로 저장함!")


def from_parquet_to_df_ticker_overview(date):
    """date: %Y-%m-%d"""
    file_path = Path(TICKER_OVERVIEW_PATH) / f"ticker_overview_all-{date}.parquet"

    try:
        df = pd.read_parquet(file_path)
        return df

    except Exception:
        logging.exception(
            f"{file_path}에서 종목별 기본 정보를 가져오는데 실패했습니다.",
        )
        return None


if __name__ == "__main__":
    tickers = get_ticker_all()
    # save_ohlc_for_days(tickers, last_date="2025-09-18", days=30)
    df = from_parquet_to_df_ticker_overview("2025-09-18")
