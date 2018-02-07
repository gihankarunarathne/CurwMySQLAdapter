from enum import Enum


class Data(Enum):
    """
    Enum types for Data handling

    Data Table Enum:
    - data : Data will be stored in `data` table
    - processed_data : Data will be stored in `processed_data` table
    """
    data = 'data'
    processed_data = 'processed_data'


class TimeseriesGroupOperation(Enum):
    """
    Enum type for Timeseries Group Operations.
    """
    mysql_1min_sum = '1min_sum'
    mysql_1min_max = '1min_max'
    mysql_1min_avg = '1min_avg'
    mysql_5min_sum = '5min_sum'
    mysql_5min_max = '5min_max'
    mysql_5min_avg = '5min_avg'
