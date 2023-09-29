import extract
import pytest


def test_to_tabular_xml_ok():
    # TODO
    pass


def test_to_tabular_json_ok():
    # TODO
    pass


def test_to_tabular_wrong_json_throw_exception():
    # TODO
    pass


def test_clean_header_spaces_uppercase_ok():
    test_extractor = extract.APIExtractor()
    test_extractor.header = [' leading', 'tailing ', 'in between', ' Uppercase AND spaces ']
    test_extractor.clean_header()
    expected = ['leading', 'tailing', 'in_between', 'uppercase_and_spaces']
    assert test_extractor.header == expected

