from datetime import datetime

from .Constants import  COMMON_DATETIME_FORMAT

def validate_common_datetime(date_text):
    try:
        datetime.strptime(date_text, COMMON_DATETIME_FORMAT)
        return True
    except ValueError:
        return False
