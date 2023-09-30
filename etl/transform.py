import pkgutil
from inspect import getmembers, isfunction
from etl_exceptions import NotCorrectDateType
import logging
from etl_base import ETL


# Configure logging
logger = logging.getLogger('extract')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class FunctionFound(Exception):
    """This Exception subclass is important only for the logic flow of the transform_timestamps method"""
    pass


class APITransformer(ETL):
    """
    Class for doing the Transform part
    """
    def __init__(self, data=None, header=None, tz_columns=None, name=None):
        self.header = header
        self.functions_list = []
        self.data = data
        self.tz_columns = tz_columns if tz_columns is not None else []
        self.name = name
        # Only run the extraction (and cleaning) code if an url and an endpoint were provided
        if self.data is not None and self.header is not None:
            self.load_all_to_utc_timestamp_functions()
            self.transform_timestamps()

    def load_all_to_utc_timestamp_functions(self):
        """
        Like the method load_all_to_tabular_functions in the extract module.
        Method that calls all functions inside the folder to_utc_functions into self.functions_list
        These functions should raise a NotCorrectDateType Exception, if they are not meant for that data type
        """
        self.functions_list = []
        all_modules = [name for _, name, _ in pkgutil.iter_modules(['to_utc_functions'])]
        for module in all_modules:
            exec(f'from to_utc_functions import {module}')
            module_functions = getmembers(eval(module),isfunction)
            for function_tuple in module_functions:
                function_name, function = function_tuple
                # Add function_name to self.functions_list (No need to add the function as a method of this class)
                self.functions_list.append(function)

    def transform_timestamps(self):
        """
        Like the method to_tabular in the extract module.
        Finds a function in self.functions_list that is able to convert that "date" into a "time zone aware UTC format"
        """
        for row in self.data:
            for column in self.tz_columns:
                try:
                    for function in self.functions_list:
                        try:
                            row[column] = function(row[column])
                            raise FunctionFound()
                        except NotCorrectDateType as e:
                            pass
                    raise NotCorrectDateType(f'No function in {self.functions_list} is able to convert ts to UTC')
                except FunctionFound:
                    pass


def main():
    logger.info('This is a Python module and is not supposed to be called directly')
    pass


if __name__ == "__main__":
    main()