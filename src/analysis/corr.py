import logging

from src.data.get import from_parquet_to_df_etf_holdings


def run_corr_analysis(df, ticker, days, top=10):
    if ticker not in "".join(df.columns):
        logging.error(f"\n분석 대상 DataFrame에 {ticker}가 존재하지 않습니다.")
    sliced_df = df.iloc[-days:]

    corr_series = sliced_df.corrwith(sliced_df[ticker], method="pearson").drop(
        labels=ticker,
        errors="ignore",
    )

    return corr_series.sort_values(ascending=False).head(top)


def post_corr_mapping():
    holdings_df = from_parquet_to_df_etf_holdings("2025-09-21")
    themes_by_ticker = (
        holdings_df.groupby("Ticker")["Theme"]
        .apply(lambda s: set(s.dropna()))
        .to_dict()
    )
    print(len(themes_by_ticker.keys()))


if __name__ == "__main__":
    # df = filter_volume_and_min_days(target_col="c")
    # run_corr_analysis(df, ticker="RGTI", days=35)
    post_corr_mapping()
