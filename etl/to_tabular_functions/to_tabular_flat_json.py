from etl_exceptions import NotCorrectType
import json


def to_tabular_flat_json(raw_data):
    """
    This method assumes that the response was a json that has a list of dictionaries
    e.g.: [{'id1':1, 'id2':2},{'id1':11, 'id2':22},{'id1':1111, 'id2':2222}]
    All dictionaries should have the same keys. Dictionary values represent a new line
    """
    try:
        data = []
        json_content = json.loads(raw_data)
        first_row = json_content[0]
        header = [i for i in first_row]
        for row in json_content:
            new_row = []
            for column in header:
                try:
                    new_row.append(row[column]) # This expects all dictionaries to have identical keys
                except KeyError:
                    raise NotCorrectType(f'Column not found: {column} in JSON data: {row}')
            data.append(new_row)
        return header, data
    except json.decoder.JSONDecodeError as e:
        raise NotCorrectType('to_tabular_flat_json method was not able to get data as it dot seem to be JSON')
