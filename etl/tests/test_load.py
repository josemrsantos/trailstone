import load
import pytest
from etl_exceptions import CannotCreateFilePath
import os
from unittest.mock import patch
import pyarrow.parquet



@patch('os.makedirs')
def test_create_full_path_ok(mock_makedirs):
    loader = load.APILoader(data=None,
                            header=None,
                            path='./output/wind',
                            partition='partition1/partition2')
    result = loader.create_full_path('file.txt')
    expected = './output/wind/partition1/partition2/file.txt'
    assert result == expected
    mock_makedirs.assert_called()


def test_create_full_path_no_path_throws_exception():
    loader = load.APILoader(data=None,
                            header=None,
                            path=None,
                            partition='partition1/partition2')
    with pytest.raises(CannotCreateFilePath):
        loader.create_full_path('file.txt')


@patch('os.makedirs')
@patch('pyarrow.parquet.write_table')
def test_create_file_ok(mock_makedirs, mock_parquet):
    loader = load.APILoader(data=[[1,2], [3,4]],
                            header=[1,2],
                            path='./test/1',
                            partition='partition1/partition2')
    mock_parquet.assert_called()
    mock_makedirs.assert_called()


@patch('os.makedirs')
def test_create_file_wrong_header_throws_exception(mock_makedirs):
    with pytest.raises(ValueError):
        loader = load.APILoader(data=[[1,2], [3,4]],
                                header=[2], # Note that the header should have 2 columns
                                path='./test/1',
                                partition='partition1/partition2')

