import pandas_market_calendars as mcal
import pandas as pd

def get_date_list(days=0, today=pd.Timestamp.today(tz='UTC')) -> pd.Timestamp:
    nyse=mcal.get_calendar('NYSE')
    day_range=nyse.schedule(start_date=today-pd.Timedelta(days=days*2), end_date=today)
    date_list = [_to_string_from_time(date) for date in day_range["market_open"]]

    return date_list[-days:]

def _to_string_from_time(date: pd.Timestamp):
    return date.strftime("%Y-%m-%d")

if __name__ == "__main__":
    print(get_date_list(days=5))


