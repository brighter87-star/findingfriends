import pandas as pd
import os
from src.utils.config import OHLC_PATH, DollarVolume
from src.utils.calc import get_date_list

def _make_pct_change_col(df) -> pd.DataFrame:
    return df

def _load_and_merge_OHLC_all():
    date_list = get_date_list(days=100)
    dfs = []
    for date in date_list:
        df = pd.read_parquet(os.path.join(OHLC_PATH, f"ohlc_{date}.parquet"), engine="pyarrow")
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def _from_long_to_wide(df, col="c"):
    wide_df=df.pivot(index="date", columns="T", values=col)
    return wide_df

def _add_pct_change(df):
    return df.pct_change(fill_method=None)

def _filter_low_volume(df) -> list:
    low_df=df[df["cxv"] < DollarVolume]['T']
    return low_df.unique()

def preprocessing(target_col="c"):
    df_all=_load_and_merge_OHLC_all()
    df_all["cxv"]=df_all["c"]*df_all["v"]
    lowvtickers = _filter_low_volume(df_all)
    wide_all=_from_long_to_wide(df_all, col=target_col)
    return _add_pct_change(wide_all), lowvtickers
    

if __name__ == "__main__" :
    target_df, lowvtickers=preprocessing(target_col="c") 
