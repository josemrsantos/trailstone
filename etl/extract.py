import requests
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
import urllib.parse
import pkgutil
from inspect import getmembers, isfunction
from etl_exceptions import NotCorrectType
import logging

# Configure logging
logger = logging.getLogger('extract')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class APIExtractor:

    def __init__(self, url=None, endpoint=None, key=None, name=None):
        self.url = url
        self.endpoint = endpoint
        self.key = urllib.parse.quote(key) if key is not None else None
        self.name = name
        # self.functions_list = ['to_tabular_flat_json', 'to_tabular_csv'] if functions_list is None else functions_list
        self.functions_list = []
        # self.request will hold the requests.models.Response object if we need to create new to_tabular functions
        self.request = None
        self.raw_data = ''
        self.request_url = ''
        self.header = []
        self.data = []
        # Set the retry strategy
        self.retry_strategy = Retry(total=4, backoff_factor=2, status_forcelist=[429])
        # Only run the extraction (and cleaning) code if an url and an endpoint were provided
        if self.url is not None and self.endpoint is not None:
            self.load_all_to_tabular_functions()
            self.create_request_url_api_key()
            self.get_data()
            self.to_tabular()
            self.clean_header()

    def load_all_to_tabular_functions(self):
        self.functions_list = []
        all_modules = [name for _, name, _ in pkgutil.iter_modules(['to_tabular_functions'])]
        for module in all_modules:
            exec(f'from to_tabular_functions import {module}')
            module_functions = getmembers(eval(module), isfunction)
            for function_tuple in module_functions:
                function_name, function = function_tuple
                # Add this function as a module
                # setattr(self, function_name, function)
                #                 setattr(self, function_name, eval(f'{module}.{function_name}'))
                # Add function_name to self.functions_list
                self.functions_list.append(function)

    def create_request_url_api_key(self):
        """
        Method assumes that an API key has been provided.
        Please create other functions for other types of auth.
        """
        self.request_url = f'{self.url}{self.endpoint}?api_key={self.key}'

    def get_data(self):
        adapter = HTTPAdapter(max_retries=self.retry_strategy)
        session = requests.Session()
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        self.request = session.get(self.request_url)
        if self.request.status_code != 200:
            raise Exception(self.request.text)
        self.raw_data = self.request.content.decode('utf-8')

    def to_tabular(self):
        for function in self.functions_list:
            try:
                self.header, self.data = function(self.raw_data)
                return
            except NotCorrectType:
                pass
        error_msg = (f'Not able to convert to a tabular format using any of the functions: '
                     f'{",".join(self.functions_list)}')
        logger.error(error_msg)
        raise NotCorrectType(error_msg)

    def clean_header(self):
        new_header = []
        for column in self.header:
            # Remove trailing and leading spaces
            column = column.strip()
            # lowercase
            column = column.lower()
            # replace spaces with underscores
            column = column.replace(' ', '_')
            new_header.append(column)
        self.header = new_header

    def dump(self):
        logger.info(f'--------------------------------------------------------------------')
        logger.info(self.name)
        logger.info(f'--------------------------------------------------------------------')
        logger.info(f"HEADER={','.join(self.header)}")
        logger.info('DATA:')
        for row in self.data:
            logger.info(f"{','.join([str(i) for i in row])}")


def main():
    logger.info('This is a Python module and is not supposed to be called directly')
    pass


if __name__ == "__main__":
    main()
