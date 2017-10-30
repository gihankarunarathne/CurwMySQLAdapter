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
