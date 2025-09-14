import numpy as np
from functools import singledispatchmethod
from src.analysis.preprocessing import preprocessing

def run_corr_analysis(df, ticker, days, excluded_tickers):
    if not ticker in "".join(df.columns):
        raise ValueError(f"\n분석 대상 DataFrame에 {ticker}가 존재하지 않습니다.")
    sliced_df=df.iloc[-days:]

    corr_series=sliced_df.corrwith(sliced_df[ticker], 
                                   method="pearson").drop(labels=np.append(excluded_tickers, ticker),
                                   errors="ignore")
    
    print(corr_series.sort_values(ascending=False).head(10))

if __name__ == "__main__":
    df, lowvtickers= preprocessing(target_col="c")
    run_corr_analysis(df, ticker="RGTI", days=35, excluded_tickers=lowvtickers)
