from extract import NotCorrectType
import json
import csv


def to_tabular_csv(raw_data):
    data = []
    try:
        json_content = json.loads(raw_data)
        raise NotCorrectType('to_tabular_csv method was not able to get data as it seems to be JSON')
    except json.decoder.JSONDecodeError as e:
        pass
    csv_iterator = csv.DictReader(raw_data.splitlines(), delimiter=',')
    header = csv_iterator.fieldnames
    for row in csv_iterator:
        new_row = [row[column] for column in header]
        data.append(new_row)
    return header, data
