from to_tabular_functions.to_tabular_flat_json import to_tabular_flat_json
import pytest
from etl_exceptions import NotCorrectType


class MockJSONOk:
    def __init__(self):
        self.header = []
        self.raw_data = '[{"a": "1", "b": "11"}, {"a": "2", "b": "22"}, {"a": "3", "b": "33"}]'
        self.data = []


class MockJSONIsCSV:
    def __init__(self):
        self.header = None
        self.raw_data = "a,b\n1,11\n2,22\n3,33\n"
        self.data = []


class MockJSONSyntaxError:
    def __init__(self):
        self.header = None
        self.raw_data = '[{"a": "1", "b": "11"}, {"a": "2", "b": 22, {"a": "3", "b": "33"}]'
        self.data = []


class MockJSONDifferentColumnName:
    def __init__(self):
        self.header = []
        self.raw_data = '[{"a": "1", "b": "11"}, {"a": "2", "b": "22"}, {"a": "3", "c": "33"}]'
        self.data = []


def test_to_tabular_json_ok():
    json_extractor = MockJSONOk()
    to_tabular_flat_json(json_extractor)
    expected_header = ['a', 'b']
    expected_data = [['1','11'], ['2','22'], ['3','33']]
    assert json_extractor.header == expected_header
    assert json_extractor.data == expected_data


def test_to_tabular_wrong_json_throw_exception():
    json_extractor = MockJSONIsCSV()
    with pytest.raises(NotCorrectType) as e_info:
        to_tabular_flat_json(json_extractor)


def test_to_tabular_syntax_wrong_json_throw_exception():
    json_extractor = MockJSONSyntaxError()
    with pytest.raises(NotCorrectType) as e_info:
        to_tabular_flat_json(json_extractor)


def test_to_tabular_column_names_wrong_json_throw_exception():
    json_extractor = MockJSONDifferentColumnName()
    with pytest.raises(NotCorrectType) as e_info:
        to_tabular_flat_json(json_extractor)
