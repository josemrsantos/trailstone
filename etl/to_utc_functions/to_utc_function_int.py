from etl_exceptions import NotCorrectDateType
import datetime
import pytz


def to_utc_function_int(value):
    if type(value) != int:
        raise NotCorrectDateType('Date type is not an int')
    date = datetime.datetime.fromtimestamp(value / 1e3)
    utc = pytz.timezone('UTC')
    date = utc.localize(date)
    return date
