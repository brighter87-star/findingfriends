import os

import pandas as pd
from src.utils.config import NASDAQURL, OTHERURL, TICKERLIST_PATH


def _fetch_ticker_all(url) -> pd.DataFrame:
    df = pd.read_csv(url, sep="|")
    df.rename(columns={"Symbol": "Ticker", "ACT Symbol": "Ticker"}, inplace=True)
    return df


def _save_ticker(df, market):
    pass


def _filter_common_stocks(df):
    exclude_keywords = [
        "ETF",
        "ETN",
        "Fund",
        "Trust",
        "Preferred",
        "Warrant",
        "Right",
        "Unit",
        "Note",
        "Bond",
        "Debenture",
        "Depositary",
    ]
    filtered_df = df[
        df["Security Name"].notna()
        & ~df["Security Name"].str.contains(
            "|".join(exclude_keywords),
            case=False,
            na=False,
        )
    ]
    return filtered_df


def generate_tickers_only_list():
    N_df = _fetch_ticker_all(NASDAQURL)
    O_df = _fetch_ticker_all(OTHERURL)
    N_filtered_df = _filter_common_stocks(N_df)
    O_filtered_df = _filter_common_stocks(O_df)
    tickers_only = [
        str(t).strip()
        for t in list(set(N_filtered_df["Ticker"]) | set(O_filtered_df["Ticker"]))
        if pd.notna(t)
    ]
    with open(os.path.join(TICKERLIST_PATH, "tickerlist.txt"), "w") as f:
        f.writelines(ticker + "\n" for ticker in tickers_only)


if __name__ == "__main__":
    generate_tickers_only_list()
