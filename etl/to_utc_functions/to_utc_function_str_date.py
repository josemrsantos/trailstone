from etl_exceptions import NotCorrectDateType
from datetime import datetime
import pytz


def to_utc_function_str_date(value):
    if type(value) != str:
        raise NotCorrectDateType('Date type is not an str')
    try:
        date = datetime.strptime(value, "%Y-%m-%d %H:%M:%S+00:00")
    except ValueError as e:
        raise NotCorrectDateType(f'"{value}" does not match the format "%Y-%m-%d %H:%M:%S+00:00"')
    utc = pytz.timezone('UTC')
    date = utc.localize(date)
    return date
