#!/usr/bin/python3

import hashlib
import json
import logging
import traceback

import pymysql.cursors
from .station import Station
from .data import Data
from .AdapterError import InvalidDataAdapterError, DatabaseConstrainAdapterError


class MySQLAdapter:
    def __init__(self, host="localhost", user="root", password="", db="curw"):
        """Initialize Database Connection"""
        # Open database connection
        self.connection = pymysql.connect(host=host,
                                          user=user,
                                          password=password,
                                          db=db)

        # prepare a cursor object using cursor() method
        cursor = self.connection.cursor()

        # execute SQL query using execute() method.
        cursor.execute("SELECT VERSION()")

        # Fetch a single row using fetchone() method.
        data = cursor.fetchone()

        logging.info("Database version : %s " % data)

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
        try:
            with self.connection.cursor() as cursor:
                sql = "SELECT 1 FROM `run` WHERE `id`=%s"

                cursor.execute(sql, possible_id)
                is_exist = cursor.fetchone()
                if is_exist is not None:
                    event_id = possible_id
        except Exception as e:
            traceback.print_exc()
        finally:
            return event_id

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
        try:
            with self.connection.cursor() as cursor:
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

                cursor.execute(sql[0], (meta_data['station']))
                station_id = check_foreign_key_reference(cursor.fetchone(), 'station', meta_data['station'])
                cursor.execute(sql[1], (meta_data['variable']))
                variable_id = check_foreign_key_reference(cursor.fetchone(), 'variable', meta_data['variable'])
                cursor.execute(sql[2], (meta_data['unit']))
                unit_id = check_foreign_key_reference(cursor.fetchone(), 'unit', meta_data['unit'])
                cursor.execute(sql[3], (meta_data['type']))
                type_id = check_foreign_key_reference(cursor.fetchone(), 'type', meta_data['type'])
                cursor.execute(sql[4], (meta_data['source']))
                source_id = check_foreign_key_reference(cursor.fetchone(), 'source', meta_data['source'])

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
                cursor.execute(sql, sql_values)
                self.connection.commit()

        except DatabaseConstrainAdapterError as ae:
            logging.warning(ae.message)
            raise ae
        except Exception as e:
            traceback.print_exc()
            raise e

        return event_id

    def insert_timeseries(self, event_id, timeseries, upsert=False, opts=None):
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
        if opts is None:
            opts = {}
        data_table = opts.get('mode', Data.data)
        if isinstance(data_table, Data):
            data_table = data_table.value
        else:
            raise InvalidDataAdapterError("Provided Data type %s is invalid" % data_table)
        row_count = 0
        try:
            with self.connection.cursor() as cursor:
                sqlTable = "INSERT INTO `%s`" % data_table
                sql = sqlTable + " (`id`, `time`, `value`) VALUES (%s, %s, %s)"

                if upsert:
                    sql = sqlTable + " (`id`, `time`, `value`) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)"

                # Refer to performance in copy list : https://stackoverflow.com/a/2612990/1461060
                timeseries_copy = []
                for item in timeseries: timeseries_copy.append(item[:])

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
                row_count = cursor.executemany(sql, new_timeseries)
                self.connection.commit()

                sql = "UPDATE `run` SET `start_date`=(SELECT MIN(time) from `data` WHERE id=%s), `end_date`=(SELECT MAX(time) from `data` WHERE id=%s) WHERE id=%s"
                cursor.execute(sql, (event_id, event_id, event_id))
                self.connection.commit()

        except Exception as e:
            traceback.print_exc()
        finally:
            return row_count

    def delete_timeseries(self, event_id):
        """Delete given timeseries from the database

        :param string event_id: Hex Hash value that need to delete timeseries against

        :return int: Affected row count.
        """
        row_count = 0
        try:
            with self.connection.cursor() as cursor:
                sql = [
                    "DELETE FROM `run` WHERE `id`=%s",
                ]
                ''' "DELETE FROM `data` WHERE `id`=%s"
                NOTE: Since `data` table `id` foriegn key contain on `ON DELETE CASCADE`,
                when deleting entries on `run` table will automatically delete the records
                in `data` table
                '''

                row_count = cursor.execute(sql[0], event_id)
                self.connection.commit()

        except Exception as e:
            traceback.print_exc()
        finally:
            return row_count

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
        try:
            if not opts.get('limit'):
                opts['limit'] = 100
            if not opts.get('skip'):
                opts['skip'] = 0

            with self.connection.cursor() as cursor:
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
                            sql += "`%s` in (%s) " % (key, ','.join('\"%s\"' % (x) for x in meta_query[key]))
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

        except Exception as e:
            traceback.print_exc()

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
        List may have following struture s.t.
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

        try:
            if not opts.get('limit'):
                opts['limit'] = 100
            if not opts.get('skip'):
                opts['skip'] = 0

            with self.connection.cursor() as cursor:
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
        except Exception as e:
            traceback.print_exc()

    def create_station(self, station=[]):
        """Insert stations into the database

         Station ids ranged as below;
        - 1 xx xxx - CUrW (stationId: curw_<SOMETHING>)
        - 2 xx xxx - Megapolis (stationId: megapolis_<SOMETHING>)
        - 3 xx xxx - Government (stationId: gov_<SOMETHING>. May follow as gov_irr_<SOMETHING>)
        - 4 xx xxx - Public (stationId: pub_<SOMETHING>)
        - 8 xx xxx - Satellite (stationId: sat_<SOMETHING>)

        Simulation models station ids ranged over 1’000’000 as below;
        - 1 1xx xxx - WRF (stationId: wrf_<SOMETHING>)
        - 1 2xx xxx - FLO2D (stationId: flo2d_<SOMETHING>)

        :param list/tuple   station: Station details in the form of list s.t.
        [<Station.CUrW>, <STATION_ID>, <NAME>, <LATITUDE>, <LONGITUDE>, <RESOLUTION>, <DESCRIPTION>] Or
        (<ID>, <STATION_ID>, <NAME>, <LATITUDE>, <LONGITUDE>, <RESOLUTION>, <DESCRIPTION>)
        """
        row_count = 0
        try:
            with self.connection.cursor() as cursor:
                if isinstance(station, tuple) and isinstance(station[0], Station):
                    sql = "SELECT max(id) FROM `station` WHERE %s <= id AND id < %s" \
                          % (station[0].value, station[0].value+Station.getRange(station[0]))
                    logging.debug(sql)
                    cursor.execute(sql)
                    last_id = cursor.fetchone()
                    station = list(station)
                    if last_id[0] is not None:
                        station[0] = last_id[0] + 1
                    else:
                        station[0] = station[0].value
                if isinstance(station, list) and isinstance(station[0], Station):
                    sql = "SELECT max(id) FROM `station` WHERE %s <= id AND id < %s" % (station[0].value, station[0].value+Station.getRange(station[0]))
                    logging.debug(sql)
                    cursor.execute(sql)
                    last_id = cursor.fetchone()
                    if last_id[0] is not None:
                        station[0] = last_id[0] + 1
                    else:
                        station[0] = station[0].value

                sql = "INSERT INTO `station` (`id`, `stationId`, `name`, `latitude`, `longitude`, `resolution`, `description`) VALUES (%s, %s, %s, %s, %s, %s, %s)"

                logging.debug('Create Station: %s', station)
                row_count = cursor.execute(sql, station)
                self.connection.commit()
                logging.debug('Created Station # %s', row_count)

        except Exception as e:
            traceback.print_exc()
        finally:
            return row_count

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
        try:
            with self.connection.cursor() as cursor:
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

        except Exception as e:
            traceback.print_exc()
        finally:
            return response

    def delete_station(self, id=0, station_id=''):
        """Delete given station from the database

        :param integer id: Station Id
        :param string station_id: Station Id

        :return int: Affected row count.
        """
        row_count = 0
        try:
            with self.connection.cursor() as cursor:
                if id > 0:
                    sql = "DELETE FROM `station` WHERE `id`=%s"
                    row_count = cursor.execute(sql, (id))
                    self.connection.commit()
                elif station_id:
                    sql = "DELETE FROM `station` WHERE `stationId`=%s"

                    row_count = cursor.execute(sql, (station_id))
                    self.connection.commit()
                else:
                    logging.warning('Unable to find station')

        except Exception as e:
            traceback.print_exc()
        finally:
            return row_count

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
        try:
            with self.connection.cursor() as cursor:
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
        except Exception as e:
            traceback.print_exc()

    def create_source(self, source=[]):
        """
        Create Source with given details

        :param list/tuple source: Source details in the form of
        [<ID>, <SOURCE_NAME>, <PARAMETERS:json>]
        (<ID>, <SOURCE_NAME>, <PARAMETERS:json>) Or
        [<SOURCE_NAME>, <PARAMETERS:json>]
        (<SOURCE_NAME>, <PARAMETERS:json>)
        :return: dict of
        {
            status: True/False,
            rowCount: 1, # Number of row affected
            source: [] # Created Sources
        }
        """
        row_count = 0
        try:
            with self.connection.cursor() as cursor:
                if isinstance(source, tuple) and len(source) < 3:
                    sql = "SELECT max(id) FROM `source`"
                    logging.debug(sql)
                    cursor.execute(sql)
                    last_id = cursor.fetchone()
                    source = list(source)
                    if last_id[0] is not None:
                        source.insert(0, last_id[0] + 1)
                    else:
                        source.insert(0, 0)
                    source = tuple(source)
                if isinstance(source, list) and len(source) < 3:
                    sql = "SELECT max(id) FROM `station`"
                    logging.debug(sql)
                    cursor.execute(sql)
                    last_id = cursor.fetchone()
                    if last_id[0] is not None:
                        source.insert(0, last_id[0] + 1)
                    else:
                        source.insert(0, 0)

                sql = "INSERT INTO `source` (`id`, `source`, `parameters`) VALUES (%s, %s, %s)"

                logging.debug('Create Source: %s', source)
                row_count = cursor.execute(sql, source)
                self.connection.commit()
                logging.debug('Created Source # %s', row_count)

        except Exception as e:
            traceback.print_exc()
        finally:
            return {
                'status': row_count > 0,
                'row_count': row_count,
                'source': source
            }

    def get_source(self, id=0, name=''):
        """
        Get existing source

        :param integer id: ID of the source
        :param string name: Source Name
        :return: Dict of
        {
            id: '',
            source: '',
            param: ''
        }
        """
        response = {}
        try:
            with self.connection.cursor() as cursor:
                output_order = []
                for key in self.source_struct_keys:
                    output_order.append("`%s` as `%s`" % (key, key))
                output_order = ','.join(output_order)

                sql = "SELECT %s FROM `source` " % output_order
                if id > 0:
                    sql += "WHERE id=%s" % id
                elif name:
                    sql += "WHERE source=%s" % name
                else:
                    logging.warning('Unable to find source')

                logging.debug('sql (get_source):: %s', sql)
                cursor.execute(sql)
                source = cursor.fetchone()
                for i, value in enumerate(self.source_struct_keys):
                    response[value] = source[i]
                logging.debug('source:: %s', response)

        except Exception as e:
            traceback.print_exc()
        finally:
            return response

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
        try:
            with self.connection.cursor() as cursor:
                if id > 0:
                    sql = "DELETE FROM `source` WHERE `id`=%s"
                    row_count = cursor.execute(sql, id)
                    self.connection.commit()
                else:
                    logging.warning('Unable to find station')

        except Exception as e:
            traceback.print_exc()
        finally:
            return {
                'status': row_count > 0,
                'row_count': row_count
            }

    def close(self):
        # disconnect from server
        self.connection.close()
