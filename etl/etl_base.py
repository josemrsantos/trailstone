import logging

# # Configure logging
logger = logging.getLogger('etl_base')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class ETL:

    def __init__(self):
        """
        This constructor method should be re-defined by any subclass of API, but it should always define these 3
        object variables/attributes
        """
        self.name = ''
        self.header = []
        self.data = []
        pass

    def dump(self):
        """
        A method that might help debug issues on development. Outputs the name, header and data of the object.
        """
        logger.info(f'--------------------------------------------------------------------')
        logger.info(self.name)
        logger.info(f'--------------------------------------------------------------------')
        logger.info(f"{','.join(self.header)}")
        for row in self.data:
            logger.info(f"{','.join([str(i) for i in row])}")


def main():
    logger.info('This is a Python module and is not supposed to be called directly')


if __name__ == "__main__":
    main()