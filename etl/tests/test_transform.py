import transform
import pytest
from etl_exceptions import NotCorrectDateType
from to_utc_functions.to_utc_function_int import to_utc_function_int
from to_utc_functions.to_utc_function_str_date import to_utc_function_str_date


def test_empty_tz_columns():
    test_transformer = transform.APITransformer()
    assert test_transformer.tz_columns == []


def test_load_functions_from_to_utc_functions():
    data = [[1,11],[2,22],[3,33],]
    header=[1,2]
    test_transformer = transform.APITransformer(header=header, data=data)
    result = [function.__name__ for function in test_transformer.functions_list]
    assert 'to_utc_function_int' in result
    assert 'to_utc_function_str_date' in result


def test_convert_int_ts_ok():
    data = [[1,11],[2,22],[3,33],]
    header=[1,2]
    tz_columns = [0]
    test_transformer = transform.APITransformer(header=header, data=data, tz_columns=tz_columns)
    expected = to_utc_function_int(1)
    assert test_transformer.data[0][0] == expected


def test_convert_str_ts_ok():
    data = [[1,'2023-09-28 23:15:00+00:00'],[2,'2023-09-28 23:20:00+00:00'],[3,'2023-09-28 23:25:00+00:00'],]
    header=[1,2]
    tz_columns = [1]
    test_transformer = transform.APITransformer(header=header, data=data, tz_columns=tz_columns)
    expected = to_utc_function_str_date('2023-09-28 23:15:00+00:00')
    assert test_transformer.data[0][1] == expected


def test_convert_nonsense_ts_raises_exception():
    data = [[1,None],[2,None],[3,None],]
    header=[1,2]
    tz_columns = [1]
    with pytest.raises(NotCorrectDateType):
        transform.APITransformer(header=header, data=data, tz_columns=tz_columns)
