#!/usr/bin/python3

import hashlib
import json
import logging
import traceback

import pymysql
import pymysql.cursors
from .pymysqlpool import Pool

from .station import Station
from .data import Data, TimeseriesGroupOperation
from .Constants import COMMON_DATETIME_FORMAT, MYSQL_DATETIME_FORMAT
from .Utils import validate_common_datetime
from .SQLQueries import get_query
from .AdapterError import InvalidDataAdapterError, DatabaseConstrainAdapterError, DatabaseAdapterError


class MySQLAdapter:
    def __init__(self, host="localhost", user="root", password="", db="curw"):

        # Initialize db connection pool.
        def conn_creator():
            return pymysql.connect(host=host, user=user, password=password, db=db, autocommit=True)

        self.pool = Pool(create_instance=conn_creator)

        # Retrieve db version.
        conn = self.pool.get()
        with conn.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            data = cursor.fetchone()
            logging.info("Database version : %s " % data)
        if conn is not None:
            self.pool.put(conn)

        self.meta_struct = {
            'station': '',
            'variable': '',
            'unit': '',
            'type': '',
            'source': '',
            'name': ''
        }
        self.meta_struct_keys = sorted(self.meta_struct.keys())

        self.station_struct = {
            'id': '',
            'stationId': '',
            'name': '',
            'latitude': '',
            'longitude': '',
            'resolution': '',
            'description': ''
        }
        self.station_struct_keys = self.station_struct.keys()

        self.source_struct = {
            'id': '',
            'source': '',
            'parameters': ''
        }
        self.source_struct_keys = self.source_struct.keys()

    def get_connection(self):
        pass

    def get_meta_struct(self):
        """Get the Meta Data Structure of hash value
        NOTE: start_date and end_date is not using for hashing
        """
        return self.meta_struct

    def get_station_struct(self):
        """Get the Station Data Structure
        """
        return self.station_struct

    def get_event_id(self, meta_data):
        """Get the event id for given meta data
        NOTE: Only 'station', 'variable', 'unit', 'type', 'source', 'name' fields use for generate hash value

        :param dict meta_data: Dict of Meta Data that use to create the hash
        Meta Data should contains all required following keys s.t.
        {
            'station': 'Hanwella',
            'variable': 'Discharge',
            'unit': 'm3/s',
            'type': 'Forecast',
            'source': 'HEC-HMS',
            'name': 'Cloud Continuous'
        }

        :return str: sha256 hash value in hex format (length of 64 characters). If does not exists, return None.
        """
        event_id = None
        m = hashlib.sha256()

        hash_data = dict(self.meta_struct)
        for i, value in enumerate(self.meta_struct_keys):
            hash_data[value] = meta_data[value]
        logging.debug('hash Data:: %s', hash_data)

        m.update(json.dumps(hash_data, sort_keys=True).encode("ascii"))
        possible_id = m.hexdigest()
        connection = self.pool.get()
        try:
            with connection.cursor() as cursor:
                sql = "SELECT 1 FROM `run` WHERE `id`=%s"

                cursor.execute(sql, possible_id)
                is_exist = cursor.fetchone()
                if is_exist is not None:
                    event_id = possible_id
            return event_id
        except Exception as ex:
            error_message = 'Error in retrieving event_id for meta data: %s.' % meta_data
            #TODO logging and raising is considered as a cliche' and bad practice.
            logging.error(error_message)
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.put(connection)

    def create_event_id(self, meta_data):
        """
        Create a new event id for given meta data

        :param dict meta_data: Dict of Meta Data that use to create the hash
        Meta Data should contains all of following keys s.t.
        {
            'station': 'Hanwella',
            'variable': 'Precipitation',
            'unit': 'mm',
            'type': 'Forecast',
            'source': 'WRF',
            'name': 'WRF 1st'
        }
        If end_date   is not set, use default date as "01 Jan 2050 00:00:00 GMT"

        :return str: sha256 hash value in hex format (length of 64 characters)
        """
        hash_data = dict(self.meta_struct)
        for i, value in enumerate(self.meta_struct_keys):
            hash_data[value] = meta_data[value]

        m = hashlib.sha256()

        m.update(json.dumps(hash_data, sort_keys=True).encode("ascii"))
        event_id = m.hexdigest()
        connection = self.pool.get()
        try:
            sql = [
                "SELECT `id` as `station_id` FROM `station` WHERE `name`=%s",
                "SELECT `id` as `variable_id` FROM `variable` WHERE `variable`=%s",
                "SELECT `id` as `unit_id` FROM `unit` WHERE `unit`=%s",
                "SELECT `id` as `type_id` FROM `type` WHERE `type`=%s",
                "SELECT `id` as `source_id` FROM `source` WHERE `source`=%s"
            ]

            def check_foreign_key_reference(cursor_value, key_name, key_value):
                if cursor_value is not None:
                    return cursor_value[0]
                else:
                    raise DatabaseConstrainAdapterError("Could not find %s with value %s" % (key_name, key_value))

            station_id = None
            variable_id = None
            unit_id = None
            type_id = None
            source_id = None
            with connection.cursor() as cursor1:
                cursor1.execute(sql[0], (meta_data['station']))
                station_id = check_foreign_key_reference(cursor1.fetchone(), 'station', meta_data['station'])
            with connection.cursor() as cursor2:
                cursor2.execute(sql[1], (meta_data['variable']))
                variable_id = check_foreign_key_reference(cursor2.fetchone(), 'variable', meta_data['variable'])
            with connection.cursor() as cursor3:
                cursor3.execute(sql[2], (meta_data['unit']))
                unit_id = check_foreign_key_reference(cursor3.fetchone(), 'unit', meta_data['unit'])
            with connection.cursor() as cursor4:
                cursor4.execute(sql[3], (meta_data['type']))
                type_id = check_foreign_key_reference(cursor4.fetchone(), 'type', meta_data['type'])
            with connection.cursor() as cursor5:
                cursor5.execute(sql[4], (meta_data['source']))
                source_id = check_foreign_key_reference(cursor5.fetchone(), 'source', meta_data['source'])

            with connection.cursor() as cursor6:
                sql = "INSERT INTO `run` (`id`, `name`, `station`, `variable`, `unit`, `type`, `source`) VALUES (%s, %s, %s, %s, %s, %s, %s)"

                sql_values = (
                    event_id,
                    meta_data['name'],
                    station_id,
                    variable_id,
                    unit_id,
                    type_id,
                    source_id
                )
                cursor6.execute(sql, sql_values)
                return event_id

        except DatabaseConstrainAdapterError as ae:
            #TODO logging and raising is considered as a cliche' and bad practice.
            logging.error("Database Constraint Violation Error: %s" % ae.message)
            raise ae
        except Exception as ex:
            raise DatabaseAdapterError("Error while creating event_id for meta_data: %s" % meta_data, ex)
        finally:
            if connection is not None:
                self.pool.put(connection)

    def insert_timeseries(self, event_id, timeseries, upsert=False, mode=Data.data):
        """Insert timeseries into the db against given event_id

        :param string event_id: Hex Hash value that need to store timeseries against.

        :param list   timeseries: List of time series of time & value list ['2017-05-01 00:00:00', 1.08]
        E.g. [ ['2017-05-01 00:00:00', 1.08], ['2017-05-01 01:00:00', 2.04], ... ]

        :param boolean upsert: If True, upsert existing values ON DUPLICATE KEY. Default is False.
        Ref: 1). https://stackoverflow.com/a/14383794/1461060
             2). https://chartio.com/resources/tutorials/how-to-insert-if-row-does-not-exist-upsert-in-mysql/

        :param dict opts: Options dict for insert timeseries s.t.
        {
            mode: 'data', 'processed_data', # Default is 'data'
        }

        :return int: Affected row count.
        """
        if not isinstance(mode, Data):
            raise InvalidDataAdapterError("Provided Data type %s is invalid" % mode)

        row_count = 0
        connection = self.pool.get()
        try:
            with connection.cursor() as cursor1:
                sql_table = "INSERT INTO `%s`" % mode.value
                sql = sql_table + " (`id`, `time`, `value`) VALUES (%s, %s, %s)"

                if upsert:
                    sql = sql_table + \
                          " (`id`, `time`, `value`) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)"

                # Refer to performance in copy list : https://stackoverflow.com/a/2612990/1461060
                timeseries_copy = []
                for item in timeseries:
                    timeseries_copy.append(item[:])

                new_timeseries = []
                for t in [i for i in timeseries_copy]:
                    if len(t) > 1:
                        # Format value into 3 decimal palaces
                        t[1] = round(float(t[1]), 3)
                        # Insert EventId in font of timestamp, value list
                        t.insert(0, event_id)
                        new_timeseries.append(t)
                    else:
                        logging.warning('Invalid timeseries data:: %s', t)

                logging.debug(new_timeseries[:10])
                row_count = cursor1.executemany(sql, new_timeseries)

            with connection.cursor() as cursor2:
                sql = "UPDATE `run` SET `start_date`=(SELECT MIN(time) from `data` WHERE id=%s), " +\
                      "`end_date`=(SELECT MAX(time) from `data` WHERE id=%s) WHERE id=%s"
                cursor2.execute(sql, (event_id, event_id, event_id))

            return row_count

        except Exception as ex:
            error_message = 'Error in insterting timeseries: ' \
                            '(event_id: %s, upsert: %s, mode: %s)' % (event_id, upsert, mode)
            # TODO logging and raising is considered as a cliche' and bad practice.
            logging.error(error_message)
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.put(connection)

    def delete_timeseries(self, event_id):
        """Delete given timeseries from the database

        :param string event_id: Hex Hash value that need to delete timeseries against

        :return int: Affected row count.
        """
        row_count = 0
        connection = self.pool.get()
        try:
            with connection.cursor() as cursor:
                sql = [
                    "DELETE FROM `run` WHERE `id`=%s",
                ]
                ''' 
                "DELETE FROM `data` WHERE `id`=%s"
                NOTE: Since `data` table `id` foriegn key contain on `ON DELETE CASCADE`,
                when deleting entries on `run` table will automatically delete the records
                in `data` table
                '''
                row_count = cursor.execute(sql[0], event_id)
                return row_count
        except Exception as ex:
            error_message = 'Error in deleting timeseries of for event_id: %s.' % event_id
            # TODO logging and raising is considered as a cliche' and bad practice.
            logging.error(error_message)
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.put(connection)

    def get_event_ids(self, meta_query=None, opts=None):
        """Get event ids set according to given meta data

        :param dict meta_query: Dict of Meta Query that use to search the hash
        event ids. It may contain any of following keys s.t.
        {
            'station': 'Hanwella', // Or a list ['Hanwella', 'Colombo']
            'variable': 'Precipitation',
            'unit': 'mm',
            'type': 'Forecast',
            'source': 'WRF',
            'name': 'Daily Forecast',
            'start_date': '2017-05-01 00:00:00',
            'end_date': '2017-05-03 23:00:00'
        }

        :param dict opts: Dict of options for searching and handling data s.t.
        {
            'limit': 100,
            'skip': 0
        }

        :return list: Return list of event objects which matches the given scenario
        """
        if opts is None:
            opts = {}
        if meta_query is None:
            meta_query = {}
        connection = self.pool.get()
        try:
            if not opts.get('limit'):
                opts['limit'] = 100
            if not opts.get('skip'):
                opts['skip'] = 0

            with connection.cursor() as cursor:
                out_order = []
                sorted_keys = ['id'] + self.meta_struct_keys
                for key in sorted_keys:
                    out_order.append("`%s` as `%s`" % (key, key))
                out_order = ','.join(out_order)

                sql = "SELECT %s FROM `run_view` " % out_order
                if meta_query:
                    sql += "WHERE "
                    cnt = 0
                    for key in meta_query:
                        if cnt:
                            sql += "AND "

                        # TODO: Need to update start and end date of timeseries
                        if key is 'from':
                            # sql += "`%s`>=\"%s\" " % ('start_date', meta_query[key])
                            sql = sql[:-4]
                        elif key is 'to':
                            # sql += "`%s`<=\"%s\" " % ('start_date', meta_query[key])
                            sql = sql[:-4]
                        elif key is 'station' and isinstance(meta_query[key], list):
                            sql += "`%s` in (%s) " % (key, ','.join('\"%s\"' % x for x in meta_query[key]))
                        else:
                            sql += "`%s`=\"%s\" " % (key, meta_query[key])
                        cnt += 1

                logging.debug('sql (get_event_ids):: %s', sql)
                cursor.execute(sql)
                events = cursor.fetchmany(opts.get('limit'))
                logging.debug('Events (get_event_ids):: %s', events)
                response = []
                for event in events:
                    meta_struct = dict(self.meta_struct)
                    for i, value in enumerate(sorted_keys):
                        meta_struct[sorted_keys[i]] = event[i]
                    response.append(meta_struct)

                return response

        except Exception as ex:
            error_message = 'Error in retrieving event_ids for meta query: %s.' % meta_query
            # TODO logging and raising is considered as a cliche' and bad practice.
            logging.error(error_message)
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.put(connection)

    def retrieve_timeseries(self, meta_query=None, opts=None):
        """Get timeseries

        :param (dict | list) meta_query: If Meta Query is a Dict, then it'll use to search the hash
        event ids. It may contain any of following keys s.t.
        {
            'station': 'Hanwella',
            'variable': 'Precipitation',
            'unit': 'mm',
            'type': 'Forecast',
            'source': 'WRF',
            'name': 'Daily Forecast',
            'start_date': '2017-05-01 00:00:00',
            'end_date': '2017-05-03 23:00:00',
        }
        If the Meta Query is a List, then those will use to retrieve the timeseries.
        List may have following structure s.t.
            ['eventId1', 'eventId2', ...] // List of strings
        Or
            [{id: 'eventId1'}, {id: 'eventId2'}, ...] // List of Objects

        :param dict opts: Dict of options for searching and handling data s.t.
        {
            'limit': 100,
            'skip': 0,
            'from': '2017-05-01 00:00:00',
            'to': '2017-05-06 23:00:00',
            'mode': Data.data | Data.processed_data, # Default is `Data.data`
        }

        :return list: Return list of objects with the timeseries data for given matching events
        """
        if opts is None:
            opts = {}
        if meta_query is None:
            meta_query = []

        data_table = opts.get('mode', Data.data)
        if isinstance(data_table, Data):
            data_table = data_table.value
        else:
            raise InvalidDataAdapterError("Provided Data type %s is invalid" % data_table)

        connection = self.pool.get()
        try:
            if not opts.get('limit'):
                opts['limit'] = 100
            if not opts.get('skip'):
                opts['skip'] = 0

            with connection.cursor() as cursor:
                if isinstance(meta_query, dict):
                    event_ids = self.get_event_ids(meta_query)
                else:
                    event_ids = list(meta_query)

                logging.debug('event_ids :: %s', event_ids)
                response = []
                for event in event_ids:
                    if isinstance(event, dict):
                        event_id = event.get('id')
                    else:
                        event_id = event
                        event = {'id': event_id}

                    sql = "SELECT `time`,`value` FROM `%s` WHERE `id`=\"%s\" " % (data_table, event_id)

                    if opts.get('from'):
                        sql += "AND `%s`>=\"%s\" " % ('time', opts['from'])
                    if opts.get('to'):
                        sql += "AND `%s`<=\"%s\" " % ('time', opts['to'])

                    logging.debug('sql (retrieve_timeseries):: %s', sql)
                    cursor.execute(sql)
                    timeseries = cursor.fetchall()
                    event['timeseries'] = [[time, value] for time, value in timeseries]
                    response.append(event)

                return response
        except Exception as ex:
            error_message = 'Error in retrieving timeseries for meta query: %s.' % meta_query
            # TODO logging and raising is considered as a cliche' and bad practice.
            logging.error(error_message)
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.put(connection)

    def extract_grouped_time_series(self, event_id, start_date, end_date, group_operation):
        """
        Extract the grouped timeseries for the given event_id.
        :param event_id: timeseries id
        :param start_date: start datetime (early datetime) [exclusive]
        :param end_date: end datetime (late datetime) [inclusive]
        :param group_operation: aggregation time interval and value operation
        :return: timerseries, a list of list, [[datetime, value], [datetime, value], ...]
        """
        # Group operation should be of TimeseriesGroupOperation enum type.
        if not isinstance(group_operation, TimeseriesGroupOperation):
            raise InvalidDataAdapterError("Provided group_operation: %s is of not valid type" % group_operation)

        # Validate start and end dates.
        # Should be in the COMMON_DATETIME_FORMAT('%Y-%m-%d %H:%M:%S'). Should be in string format.
        if not validate_common_datetime(start_date) or not validate_common_datetime(end_date):
            raise InvalidDataAdapterError("Provided start_date: %s or end_date: %s is no in the '%s' format"
                                          % (start_date, end_date, COMMON_DATETIME_FORMAT))

        # Get the SQL Query.
        sql_query = get_query(group_operation, event_id, start_date, end_date)
        # Execute the SQL Query.
        connection = self.pool.get()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                timeseries = cursor.fetchall()
                # Prepare the output time series.
                # If the retrieved data set is empty return empty list.
                return [[time, value] for time, value in timeseries]
        except Exception as ex:
            error_message = 'Error in retrieving grouped_time_series for meta data: (%s, %s, %s, %s).' \
                            % (event_id, start_date, end_date, group_operation)
            # TODO logging and raising is considered as a cliche' and bad practice.
            logging.error(error_message)
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.put(connection)

    def create_station(self, station=None):
        """Insert stations into the database

         Station ids ranged as below;
        - 1 xx xxx - CUrW (stationId: curw_<SOMETHING>)
        - 2 xx xxx - Megapolis (stationId: megapolis_<SOMETHING>)
        - 3 xx xxx - Government (stationId: gov_<SOMETHING>. May follow as gov_irr_<SOMETHING>)
        - 4 xx xxx - Public (stationId: pub_<SOMETHING>)
        - 8 xx xxx - Satellite (stationId: sat_<SOMETHING>)

        Simulation models station ids ranged over 1’000’000 as below;
        - 1 1xx xxx - WRF (stationId: [;<prefix>_]wrf_<SOMETHING>)
        - 1 2xx xxx - FLO2D (stationId: [;<prefix>_]flo2d_<SOMETHING>)
        - 1 3xx xxx - MIKE (stationId: [;<prefix>_]mike_<SOMETHING>)

        :param list/tuple   station: Station details in the form of list s.t.
        [<Station.CUrW>, <STATION_ID>, <NAME>, <LATITUDE>, <LONGITUDE>, <RESOLUTION>, <DESCRIPTION>] Or
        (<ID>, <STATION_ID>, <NAME>, <LATITUDE>, <LONGITUDE>, <RESOLUTION>, <DESCRIPTION>)
        """
        if station is None:
            station = []
        row_count = 0
        connection = self.pool.get()
        try:
            with connection.cursor() as cursor1:
                if isinstance(station, tuple) and isinstance(station[0], Station):
                    sql = "SELECT max(id) FROM `station` WHERE %s <= id AND id < %s" \
                          % (station[0].value, station[0].value+Station.getRange(station[0]))
                    logging.debug(sql)
                    cursor1.execute(sql)
                    last_id = cursor1.fetchone()
                    station = list(station)
                    if last_id[0] is not None:
                        station[0] = last_id[0] + 1
                    else:
                        station[0] = station[0].value
                elif isinstance(station, list) and isinstance(station[0], Station):
                    sql = "SELECT max(id) FROM `station` WHERE %s <= id AND id < %s" % (station[0].value, station[0].value+Station.getRange(station[0]))
                    logging.debug(sql)
                    cursor1.execute(sql)
                    last_id = cursor1.fetchone()
                    if last_id[0] is not None:
                        station[0] = last_id[0] + 1
                    else:
                        station[0] = station[0].value

            with connection.cursor() as cursor2:
                sql = "INSERT INTO `station` (`id`, `stationId`, `name`, `latitude`, `longitude`, `resolution`, `description`) VALUES (%s, %s, %s, %s, %s, %s, %s)"

                logging.debug('Create Station: %s', station)
                row_count = cursor2.execute(sql, station)
                logging.debug('Created Station # %s', row_count)

            return row_count

        except Exception as ex:
            error_message = 'Error in creating station: %s' % station
            # TODO logging and raising is considered as a cliche' and bad practice.
            logging.error(error_message)
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.put(connection)

    def get_station(self, query={}):
        """
        Get matching station details for given query.

        :param query Dict: Query to find the station. It may contain any of following keys s.t.
        {
            id: 100001, // Integer
            stationId: 'curw_hanwella',
            name: 'Hanwella'
        }
        :return Object: Details of matching station. If not found empty Object will be return.
        """
        response = None
        connection = self.pool.get()
        try:
            with connection.cursor() as cursor:
                out_put_order = []
                for key in self.station_struct_keys:
                    out_put_order.append("`%s` as `%s`" % (key, key))
                out_put_order = ','.join(out_put_order)

                sql = "SELECT %s FROM `station` " % out_put_order
                if query:
                    sql += "WHERE "
                    cnt = 0
                    for key in query:
                        if cnt:
                            sql += "AND "

                        sql += "`%s`=\"%s\" " % (key, query[key])
                        cnt += 1

                logging.debug('sql (get_station):: %s', sql)
                cursor.execute(sql)
                station = cursor.fetchone()
                if station is not None:
                    response = {}
                    for i, value in enumerate(self.station_struct_keys):
                        response[value] = station[i]
                    logging.debug('station:: %s', response)
            return response

        except Exception as ex:
            error_message = 'Error in retrieving station details for query: %s' % query
            # TODO logging and raising is considered as a cliche' and bad practice.
            logging.error(error_message)
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.put(connection)

    def delete_station(self, id=0, station_id=''):
        """Delete given station from the database

        :param integer id: Station Id
        :param string station_id: Station Id

        :return int: Affected row count.
        """
        row_count = 0
        connection = self.pool.get()
        try:
            with connection.cursor() as cursor:
                if id > 0:
                    sql = "DELETE FROM `station` WHERE `id`=%s"
                    row_count = cursor.execute(sql, (id))
                elif station_id:
                    sql = "DELETE FROM `station` WHERE `stationId`=%s"

                    row_count = cursor.execute(sql, (station_id))
                else:
                    logging.warning('Unable to find station')
                return row_count
        except Exception as ex:
            error_message = 'Error in deleting station: id = %s, stationId = %s' % (id, station_id)
            # TODO logging and raising is considered as a cliche' and bad practice.
            logging.error(error_message)
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.put(connection)

    def get_stations(self, query={}):
        """
        Get matching stations details for given query.

        :param query:
        :return:
        """
        return []

    def get_stations_in_area(self, query={}):
        """Get stations

        :param dict query: Query for retrieve stations. It may contain any of following keys s.t.
        {
            latitude_lower: '',
            longitude_lower: '',
            latitude_upper: '',
            longitude_upper: '',
        }
        If the latitude and longitude of a particular area is provided,
        it'll look into all the stations which reside inside that area.

        :return list: Return list of objects with the stations data which resign in given area.
        """
        connection = self.pool.get()
        try:
            with connection.cursor() as cursor:
                out_order = []
                sorted_keys = sorted(self.station_struct.keys())
                for key in sorted_keys:
                    out_order.append("`%s` as `%s`" % (key, key))
                out_order = ','.join(out_order)

                sql = "SELECT %s FROM `station` " % (out_order)
                if query:
                    sql += "WHERE "
                    cnt = 0
                    for key in query:
                        if cnt:
                            sql += "AND "

                        if key is 'latitude_lower':
                            sql += "`%s`>=\"%s\" " % ('latitude', query[key])
                        elif key is 'longitude_lower':
                            sql += "`%s`>=\"%s\" " % ('longitude', query[key])
                        elif key is 'latitude_upper':
                            sql += "`%s`<=\"%s\" " % ('latitude', query[key])
                        elif key is 'longitude_upper':
                            sql += "`%s`<=\"%s\" " % ('longitude', query[key])
                        cnt += 1

                logging.debug('sql (get_stations):: %s', sql)
                cursor.execute(sql)
                stations = cursor.fetchall()
                response = []
                for station in stations:
                    station_struct = dict(self.station_struct)
                    for i, value in enumerate(sorted_keys):
                        station_struct[sorted_keys[i]] = station[i]
                    response.append(station_struct)

                return response
        except Exception as ex:
            error_message = 'Error in getting station in area of: %s' % query
            # TODO logging and raising is considered as a cliche' and bad practice.
            logging.error(error_message)
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.put(connection)

    def create_source(self, source=None):
        """
        Create Source with given details

        :param list/tuple/str source: Source details in the form of
        [<ID>, <SOURCE_NAME>, <PARAMETERS:json>]
        (<ID>, <SOURCE_NAME>, <PARAMETERS:json>) Or
        [<SOURCE_NAME>, <PARAMETERS:json>]
        (<SOURCE_NAME>, <PARAMETERS:json>) Or
        <SOURCE_NAME>

        :return: dict of
        {
            status: True/False,
            rowCount: 1, # Number of row affected
            source: [] # Created Sources
        }
        """
        if source is None:
            source = []
        if isinstance(source, str):
            source = [source]

        row_count = 0
        connection = self.pool.get()
        try:
            sql = "INSERT INTO `source` (`id`, `source`, `parameters`) VALUES (%s, %s, %s)"
            with connection.cursor() as cursor1:
                if len(source) < 3:
                    sql_source_id = "SELECT max(id) FROM `source`"
                    logging.debug(sql_source_id)
                    cursor1.execute(sql_source_id)
                    last_id = cursor1.fetchone()
                    if isinstance(source, tuple):
                        source = list(source)
                    if last_id[0] is not None:
                        source.insert(0, last_id[0] + 1)
                    else:
                        source.insert(0, 0)
                    # If parameters are still missing, append
                    if len(source) < 3:
                        source.append(None)
                    source = tuple(source)

            with connection.cursor() as cursor2:
                logging.debug('Create Source: %s', source)
                row_count = cursor2.execute(sql, source)
                logging.debug('Created Source # %s', row_count)

            return {
                'status': row_count > 0,
                'row_count': row_count,
                'source': source
            }

        except Exception as ex:
            error_message = 'Error in creating source: %s' % source
            # TODO logging and raising is considered as a cliche' and bad practice.
            logging.error(error_message)
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.put(connection)

    def get_source(self, source_id=0, name=''):
        """
        Get existing source

        :param integer source_id: ID of the source
        :param string name: Source Name
        :return: Dict of
        {
            id: '',
            source: '',
            parameters: ''
        }
        """
        response = {}
        connection = self.pool.get()
        try:
            with connection.cursor() as cursor:
                output_order = []
                for key in self.source_struct_keys:
                    output_order.append("`%s` as `%s`" % (key, key))
                output_order = ','.join(output_order)

                sql = "SELECT %s FROM `source` " % output_order
                if source_id > 0:
                    sql += "WHERE id=%s" % source_id
                elif name:
                    sql += "WHERE source=\"%s\"" % name
                else:
                    logging.warning('Unable to find source')

                logging.debug('sql (get_source):: %s', sql)
                cursor.execute(sql)
                source = cursor.fetchone()
                if source is not None:
                    for i, value in enumerate(self.source_struct_keys):
                        response[value] = source[i]
                    logging.debug('source:: %s', response)

        except Exception as ex:
            error_message = 'Error in retrieving source: (source id: %s, name: %s)' % (source_id, name)
            # TODO logging and raising is considered as a cliche' and bad practice.
            logging.error(error_message)
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.put(connection)

    def delete_source(self, id=0):
        """Delete given source from the database

        :param integer id: Source Id

        :return dict: Return with
        {
            status: True/False,
            rowCount: 1, # Number of row affected
        }
        """
        row_count = 0
        connection = self.pool.get()
        try:
            with connection.cursor() as cursor:
                if id > 0:
                    sql = "DELETE FROM `source` WHERE `id`=%s"
                    row_count = cursor.execute(sql, id)
                else:
                    logging.warning('Unable to find station')

                return {
                    'status': row_count > 0,
                    'row_count': row_count
                }

        except Exception as ex:
            error_message = 'Error in retrieving source: (source id: %s)' % id
            # TODO logging and raising is considered as a cliche' and bad practice.
            logging.error(error_message)
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.put(connection)

    def close(self):
        # disconnect from server
        # TODO since a connection pool is used. Need to think of an implementation for this method.
        pass
