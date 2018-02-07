from datetime import datetime

from .data import TimeseriesGroupOperation
from .Constants import MYSQL_DATETIME_FORMAT
from .AdapterError import InvalidDataAdapterError

_MYSQL_5MIN_TIMESERIES = \
    "SELECT " \
        "REPLACE(" \
            "MAX(time), " \
            "SUBSTRING_INDEX(MAX(time), ' ', -1), " \
            "SEC_TO_TIME((TIME_TO_SEC(MAX(time)) DIV 300) * 300)) as datetime, " \
        "%s(value) as value " \
    "FROM curw_iot.data " \
    "WHERE " \
        "id = '%s' " \
        "and " \
        "time <= '%s' " \
        "and " \
        "time > '%s' " \
    "GROUP BY UNIX_TIMESTAMP(time) DIV 300 " \
    "ORDER BY datetime;"

_MYSQL_1MIN_TIMESERIES = \
    "SELECT " \
        "DATE_FORMAT(MAX(time), '%s') as datetime, " \
        "%s(value) as value " \
    "FROM curw_iot.data " \
    "WHERE " \
        "id = '%s' " \
        "and " \
        "time <= '%s' " \
        "and " \
        "time > '%s' " \
    "GROUP BY UNIX_TIMESTAMP(time) DIV 60 " \
    "ORDER BY datetime;"


def _1min_sum(event_id, start_date, end_date):
    """
    Returns mysql query for retrieving timeseries in between start_date and end_date
    for a given event_id in 1 min intervals.
    :param event_id: timeseries id
    :param start_date: starting datetime (early datetime) [exclusive], format: "%Y-%m-%d %H:%M:%S"
    :param end_date: ending datetime (late datetime) [inclusive], format: "%Y-%m-%d %H:%M:%S"
    :return: mysql query to retrieve timeseries in 1min interval
    """
    return _MYSQL_1MIN_TIMESERIES % (MYSQL_DATETIME_FORMAT, 'SUM', event_id, end_date, start_date)


def _1min_max(event_id, start_date, end_date):
    """
    Returns mysql query for retrieving timeseries in between start_date and end_date
    for a given event_id 1 min intervals.
    :param event_id: timeseries id
    :param start_date: starting datetime (early datetime) [exclusive], format: "%Y-%m-%d %H:%M:%S"
    :param end_date: ending datetime (late datetime) [inclusive], format: "%Y-%m-%d %H:%M:%S"
    :return: mysql query to retrieve timeseries in 1min interval
    """
    return _MYSQL_1MIN_TIMESERIES % (MYSQL_DATETIME_FORMAT, 'MAX', event_id, end_date, start_date)

def _1min_avg(event_id, start_date, end_date):
    """
    Returns mysql query for retrieving timeseries in between start_date and end_date
    for a given event_id 1 min intervals.
    :param event_id: timeseries id
    :param start_date: starting datetime (early datetime) [exclusive], format: "%Y-%m-%d %H:%M:%S"
    :param end_date: ending datetime (late datetime) [inclusive], format: "%Y-%m-%d %H:%M:%S"
    :return: mysql query to retrieve timeseries in 1min interval
    """
    return _MYSQL_1MIN_TIMESERIES % (MYSQL_DATETIME_FORMAT, 'AVG', event_id, end_date, start_date)


def _5min_sum(event_id, start_date, end_date):
    """
    Returns mysql query for retrieving timeseries in between start_date and end_date
    for a given event_id 5 min intervals.
    :param event_id: timeseries id
    :param start_date: starting datetime (early datetime) [exclusive], format: "%Y-%m-%d %H:%M:%S"
    :param end_date: ending datetime (late datetime) [inclusive], format: "%Y-%m-%d %H:%M:%S"
    :return: mysql query to retrieve timeseries in 5min interval
    """
    return _MYSQL_5MIN_TIMESERIES % ('SUM', event_id, end_date, start_date)

def _5min_max(event_id, start_date, end_date):
    """
    Returns mysql query for retrieving timeseries in between start_date and end_date
    for a given event_id 5 min intervals.
    :param event_id: timeseries id
    :param start_date: starting datetime (early datetime) [exclusive], format: "%Y-%m-%d %H:%M:%S"
    :param end_date: ending datetime (late datetime) [inclusive], format: "%Y-%m-%d %H:%M:%S"
    :return: mysql query to retrieve timeseries in 5min interval
    """
    return _MYSQL_5MIN_TIMESERIES % ('MAX', event_id, end_date, start_date)

def _5min_avg(event_id, start_date, end_date):
    """
    Returns mysql query for retrieving timeseries in between start_date and end_date
    for a given event_id 5 min intervals.
    :param event_id: timeseries id
    :param start_date: starting datetime (early datetime) [exclusive], format: "%Y-%m-%d %H:%M:%S"
    :param end_date: ending datetime (late datetime) [inclusive], format: "%Y-%m-%d %H:%M:%S"
    :return: mysql query to retrieve timeseries in 5min interval
    """
    return _MYSQL_5MIN_TIMESERIES % ('AVG', event_id, end_date, start_date)

def get_query(group_operation, event_id, start_date, end_date):
    """
    Returns the corresponding sql query for the given group_operation
    :param group_operation: should be of TimeseriesGroupOperation
    :param event_id: timeseries id
    :param start_date: starting datetime (early datetime) [exclusive], format: "%Y-%m-%d %H:%M:%S"
    :param end_date: ending datetime (late datetime) [inclusive], format: "%Y-%m-%d %H:%M:%S"
    :return: mysql query string
    """
    if group_operation is TimeseriesGroupOperation.mysql_1min_sum:
        return _1min_sum(event_id, start_date, end_date)
    elif group_operation is TimeseriesGroupOperation.mysql_1min_max:
        return _1min_max(event_id, start_date, end_date)
    elif group_operation is TimeseriesGroupOperation.mysql_1min_avg:
        return _1min_avg(event_id, start_date, end_date)
    elif group_operation is TimeseriesGroupOperation.mysql_5min_sum:
        return _5min_sum(event_id, start_date, end_date)
    elif group_operation is TimeseriesGroupOperation.mysql_5min_max:
        return _5min_max(event_id, start_date, end_date)
    elif group_operation is TimeseriesGroupOperation.mysql_5min_avg:
        return _5min_avg(event_id, start_date, end_date)
    else:
        raise InvalidDataAdapterError("Invalid group_operation type: %s" % group_operation)
