import logging
import pandas as pd
import pyarrow
import pyarrow.parquet
from etl_base import ETL
import os
from etl_exceptions import CannotCreateFilePath


logger = logging.getLogger('load')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class APILoader(ETL):
    """
    Class for doing the last stage: the Load. In this case it was decided to output the final data into Parquet files
    since it is an optimal format to load the data into (partitioned) hive tables that can be queried using Athena,
    and can be efficiently loaded into DataWarehouse Data Base engines such as Redshift or Snowflake.
    """
    def __init__(self, data=None, header=None, name=None, path='./output', partition=None):
        self.data = data
        self.header = header
        self.name = name
        self.path = path
        self.partition = partition
        self.functions_list = []
        if data and header:
            self.export_data()

    def create_full_path(self, file_name='data.parquet'):
        """
        Creates all the directories for the full path to data file. Returns the full path to the file (including the
        file name).
        :param file_name: The name of the file name. The default value is 'data.parquet'.
        :return: The full path to the file
        """
        if self.path is None or self.partition is None:
            raise CannotCreateFilePath('To create the file, the path and the partition need to be defined')
        # Make sure all directories exist
        os.makedirs(name=f'{self.path}/{self.partition}/', exist_ok=True)
        # Create the full path
        full_path = f'{self.path}/{self.partition}/{file_name}'
        return full_path

    def export_data(self):
        """
        Exports the data in this object into a file, according to the defined path and partition(s)
        :return:
        """
        # For the sake of simplicity, create a "pandas" Data Frame
        df = pd.DataFrame(self.data, columns=self.header)
        # Create a PyArrow Table from the pandas Data Frame
        table = pyarrow.Table.from_pandas(df)
        # create the full_path, including the partition (should be the date, but can also be something else)
        export_path = self.create_full_path()
        # Output the data into a parquet file
        pyarrow.parquet.write_table(table, export_path)
        logger.info(f'Data Loaded into the parquet file {export_path}')


def main():
    logger.info('This is a Python module and is not supposed to be called directly')


if __name__ == "__main__":
    main()