import extract
import transform
import load
from datetime import date, timedelta
import argparse
import logging

# # Configure logging (This should be the top level)
logger = logging.getLogger('etl')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class API:

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


def get_days(days_back, today):
    if not today:  # By default today will be the empty string and should be set for today
        today = date.today()
    else:
        year, month, day = today.split('-')
        today = date(year, month, day)
    all_days = []
    for i in range(days_back, 0, -1):
        day = today - timedelta(days=i)
        all_days.append(day)
    logger.debug(f'Will fetch data for the {days_back} previous days, '
                 f'from the day {all_days[0]} to the day {all_days[-1]}')
    return all_days


def main():
    logger.info('START of ETL')
    # Read from parameters how many days back
    parser = argparse.ArgumentParser(description='All parameters are optional. '
                                                 'By default it will fetch the last 7 days')
    parser.add_argument('--days_back',
                        required=False,
                        help='Hom many days to go back',
                        default=7,
                        type=int)
    parser.add_argument('--today',
                        required=False,
                        help='Input file name (or full path)',
                        default='',
                        type=str)

    args = parser.parse_args()
    # Create a list with all days that we need to get backwards
    all_days = get_days(days_back=args.days_back,today=args.today)
    for day in all_days:
        wind_name = f'Wind - {day}'
        solar_name = f'Solar - {day}'
        # Extract
        logger.info(f'Going to fetch data for the day {day}')
        wind_extractor = extract.APIExtractor(url='http://localhost:8000/',
                                              endpoint=f'{day}/renewables/windgen.csv',
                                              key='ADU8S67Ddy!d7f?',
                                              name=wind_name)
        solar_extractor = extract.APIExtractor(url='http://localhost:8000/',
                                               endpoint=f'{day}/renewables/solargen.json',
                                               key='ADU8S67Ddy!d7f?',
                                               name=solar_name)
        # Transform
        wind_transformed = transform.APITransformer(wind_extractor.data,
                                                    wind_extractor.header,
                                                    tz_columns=[0,3],
                                                    name=wind_name)
        solar_transformed = transform.APITransformer(solar_extractor.data,
                                                     solar_extractor.header,
                                                     tz_columns=[0, 3],
                                                     name=solar_name)
        # Load
        load.APILoader( data=wind_transformed.data,
                        header=wind_transformed.header,
                        name=wind_name,
                        path='./output/wind',
                        partition=day)
        load.APILoader(data=solar_transformed.data,
                       header=solar_transformed.header,
                       name=solar_name,
                       path='./output/solar',
                       partition=day)
    logger.info('END of ETL')


if __name__ == "__main__":
    main()