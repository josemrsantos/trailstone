import extract
import pytest
import mock
import requests
from requests import Session
from unittest.mock import patch


def test_key_parsed_correctly_spaces_http():
    test_extractor = extract.APIExtractor(key=' 123 abc')
    expected = '%20123%20abc'
    assert test_extractor.key == expected


def test_key_parsed_correctly_special_chars_http():
    test_extractor = extract.APIExtractor(key=' .\\!+=£$%^&*()"|<>,?')
    expected = '%20.%5C%21%2B%3D%C2%A3%24%25%5E%26%2A%28%29%22%7C%3C%3E%2C%3F'
    assert test_extractor.key == expected


def test_create_correct_request_url():
    test_extractor = extract.APIExtractor(key='123')
    test_extractor.url = 'http://test/'
    test_extractor.endpoint = 'parameter/endpoint/file.ext'
    test_extractor.create_request_url_api_key()
    expected = 'http://test/parameter/endpoint/file.ext?api_key=123'
    assert test_extractor.request_url == expected


@patch.object(Session, 'get', autospec=True)
def test_get_data_xml_ok(mock_session):
    mock_session.return_value.content = '1,2,3,4\n11,22,33,44'.encode('utf-8')
    mock_session.return_value.status_code = 200
    test_extractor = extract.APIExtractor(key='123')
    test_extractor.url = 'http://test/'
    test_extractor.endpoint = 'parameter/endpoint/file.ext'
    test_extractor.get_data()
    result = test_extractor.raw_data
    expected = '1,2,3,4\n11,22,33,44'
    assert result == expected


@patch.object(Session, 'get', autospec=True)
def test_get_data_json_ok(mock_session):
    mock_session.return_value.content = '[{1:1, 2:2,3:3}, {1:11, 2:22,3:33}]'.encode('utf-8')
    mock_session.return_value.status_code = 200
    test_extractor = extract.APIExtractor(key='123')
    test_extractor.url = 'http://test/'
    test_extractor.endpoint = 'parameter/endpoint/file.ext'
    test_extractor.get_data()
    result = test_extractor.raw_data
    expected = '[{1:1, 2:2,3:3}, {1:11, 2:22,3:33}]'
    assert result == expected


@patch.object(Session, 'get', autospec=True)
def test_get_data_all_fails_throw_exception(mock_session):
    mock_session.return_value.content = 'Some random output'.encode('utf-8')
    mock_session.return_value.text = 'Some random output'
    mock_session.return_value.status_code = 429
    test_extractor = extract.APIExtractor(key='123')
    test_extractor.url = 'http://test/'
    test_extractor.endpoint = 'parameter/endpoint/file.ext'
    with pytest.raises(Exception):
        test_extractor.get_data()


def test_clean_header_spaces_uppercase_ok():
    test_extractor = extract.APIExtractor()
    test_extractor.header = [' leading', 'tailing ', 'in between', ' Uppercase AND spaces ']
    test_extractor.clean_header()
    expected = ['leading', 'tailing', 'in_between', 'uppercase_and_spaces']
    assert test_extractor.header == expected

