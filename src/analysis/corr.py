import logging

from src.analysis.preprocessing import preprocessing


def run_corr_analysis(df, ticker, days, top=10):
    if ticker not in "".join(df.columns):
        logging.error(f"\n분석 대상 DataFrame에 {ticker}가 존재하지 않습니다.")
    sliced_df = df.iloc[-days:]

    corr_series = sliced_df.corrwith(sliced_df[ticker], method="pearson").drop(
        labels=ticker,
        errors="ignore",
    )

    return corr_series.sort_values(ascending=False).head(top)


if __name__ == "__main__":
    df = preprocessing(target_col="c")
    run_corr_analysis(df, ticker="RGTI", days=35)
