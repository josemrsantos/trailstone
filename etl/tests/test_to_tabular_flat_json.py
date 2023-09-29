from to_tabular_functions.to_tabular_flat_json import to_tabular_flat_json
import pytest
from etl_exceptions import NotCorrectType


def test_to_tabular_json_ok():
    raw_text = '[{"a": "1", "b": "11"}, {"a": "2", "b": "22"}, {"a": "3", "b": "33"}]'
    header, data = to_tabular_flat_json(raw_text)
    expected_header = ['a', 'b']
    expected_data = [['1','11'], ['2','22'], ['3','33']]
    assert header == expected_header
    assert data == expected_data


def test_to_tabular_wrong_json_throw_exception():
    raw_data = "a,b\n1,11\n2,22\n3,33\n"
    with pytest.raises(NotCorrectType) as e_info:
        to_tabular_flat_json(raw_data)


def test_to_tabular_syntax_wrong_json_throw_exception():
    raw_data = '[{"a": "1", "b": "11"}, {"a": "2", "b": 22, {"a": "3", "b": "33"}]'
    with pytest.raises(NotCorrectType) as e_info:
        to_tabular_flat_json(raw_data)


def test_to_tabular_column_names_wrong_json_throw_exception():
    raw_data = '[{"a": "1", "b": "11"}, {"a": "2", "b": "22"}, {"a": "3", "c": "33"}]'
    with pytest.raises(NotCorrectType) as e_info:
        to_tabular_flat_json(raw_data)
