import os
from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.utils.calc import get_date_list
from src.utils.config import OHLC_PATH


@dataclass(frozen=True)
class FilterCtx:
    dollar_volume: int = 1000000
    min_days: int = 10


def _make_pct_change_col(df) -> pd.DataFrame:
    return df


def _load_and_merge_OHLC_all():
    date_list = get_date_list(days=100)
    dfs = []
    for date in date_list:
        try:
            df = pd.read_parquet(
                os.path.join(OHLC_PATH, f"ohlc_{date}.parquet"),
                engine="pyarrow",
            )
            dfs.append(df)
        except Exception as e:
            print(e)
            continue
    return pd.concat(dfs, ignore_index=True)


def _from_long_to_wide(df, col="c"):
    wide_df = df.pivot(index="date", columns="T", values=col)
    return wide_df


def _add_pct_change(df):
    return df.pct_change(fill_method=None)


def _drop_low_liquidity(df, ctx):
    cond = (
        np.isfinite(df["c"])
        & np.isfinite(df["v"])
        & (df["c"] * df["v"] < ctx.dollar_volume)
    )
    bad_mask = cond.groupby(df["T"]).transform("any")
    return df[~bad_mask]


def _drop_few_trading_days(df, ctx):
    trading_days = df.groupby("T")["date"].nunique()
    bad_tickers = set(trading_days[trading_days < ctx.min_days].index)
    return df[~df["T"].isin(bad_tickers)]


def preprocessing(target_col="c"):
    df = _load_and_merge_OHLC_all()
    ctx = FilterCtx(dollar_volume=1000000, min_days=15)

    for step in (_drop_few_trading_days, _drop_low_liquidity):
        df = step(df, ctx)

    wide_all = _from_long_to_wide(df, col=target_col)
    return _add_pct_change(wide_all)


if __name__ == "__main__":
    target_df = preprocessing(target_col="c")
