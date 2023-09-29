import extract
import pytest
from to_tabular_functions.to_tabular_csv import to_tabular_csv
from etl_exceptions import NotCorrectType


def test_to_tabular_xml_ok():
    raw_text = 'a,b\n1,11\n2,22\n3,33'
    header, data = to_tabular_csv(raw_text)
    expected_header = ['a', 'b']
    expected_data = [['1','11'], ['2','22'], ['3','33']]
    assert header == expected_header
    assert data == expected_data


def test_to_tabular_wrong_json_throw_exception():
    raw_data = '[{"a": "1", "b": "11"}, {"a": "2", "b": 22}, {"a": "3", "b": "33"}]'
    with pytest.raises(NotCorrectType) as e_info:
        to_tabular_csv(raw_data)




