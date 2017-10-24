#!/usr/bin/python3

import hashlib
import json
import logging
import traceback

import pymysql.cursors


class mysqladapter:
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

    def getMetaStruct(self):
        """Get the Meta Data Structure of hash value
        NOTE: start_date and end_date is not using for hashing
        """
        return self.meta_struct

    def getStationStruct(self):
        """Get the Station Data Structure
        """
        return self.station_struct

    def getEventId(self, metaData):
        """Get the event id for given meta data
        NOTE: Only 'station', 'variable', 'unit', 'type', 'source', 'name' fields use for generate hash value

        :param dict metaData: Dict of Meta Data that use to create the hash
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
        eventId = None
        m = hashlib.sha256()

        hashData = dict(self.meta_struct)
        for i, value in enumerate(self.meta_struct_keys):
            hashData[value] = metaData[value]
        logging.debug('hash Data:: %s', hashData)

        m.update(json.dumps(hashData, sort_keys=True).encode("ascii"))
        possibleId = m.hexdigest()
        try:
            with self.connection.cursor() as cursor:
                sql = "SELECT 1 FROM `run` WHERE `id`=%s"

                cursor.execute(sql, possibleId)
                isExist = cursor.fetchone()
                if isExist is not None:
                    eventId = possibleId
        except Exception as e:
            traceback.print_exc()
        finally:
            return eventId

    def createEventId(self, metaData):
        """
        Create a new event id for given meta data

        :param dict metaData: Dict of Meta Data that use to create the hash
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
        hashData = dict(self.meta_struct)
        for i, value in enumerate(self.meta_struct_keys):
            hashData[value] = metaData[value]

        m = hashlib.sha256()
        m.update(json.dumps(hashData, sort_keys=True).encode("ascii"))
        eventId = m.hexdigest()
        try:
            with self.connection.cursor() as cursor:
                sql = [
                    "SELECT `id` as `stationId` FROM `station` WHERE `name`=%s",
                    "SELECT `id` as `variableId` FROM `variable` WHERE `variable`=%s",
                    "SELECT `id` as `unitId` FROM `unit` WHERE `unit`=%s",
                    "SELECT `id` as `typeId` FROM `type` WHERE `type`=%s",
                    "SELECT `id` as `sourceId` FROM `source` WHERE `source`=%s"
                ]

                cursor.execute(sql[0], (metaData['station']))
                stationId = cursor.fetchone()[0]
                cursor.execute(sql[1], (metaData['variable']))
                variableId = cursor.fetchone()[0]
                cursor.execute(sql[2], (metaData['unit']))
                unitId = cursor.fetchone()[0]
                cursor.execute(sql[3], (metaData['type']))
                typeId = cursor.fetchone()[0]
                cursor.execute(sql[4], (metaData['source']))
                sourceId = cursor.fetchone()[0]

                sql = "INSERT INTO `run` (`id`, `name`, `station`, `variable`, `unit`, `type`, `source`) VALUES (%s, %s, %s, %s, %s, %s, %s)"

                sqlValues = (
                    eventId,
                    metaData['name'],
                    stationId,
                    variableId,
                    unitId,
                    typeId,
                    sourceId
                )
                cursor.execute(sql, sqlValues)
                self.connection.commit()

        except Exception as e:
            traceback.print_exc()

        return eventId

    def insertTimeseries(self, eventId, timeseries, upsert=False):
        """Insert timeseries into the db against given eventId

        :param string eventId: Hex Hash value that need to store timeseries against.

        :param list   timeseries: List of time series of time & value list ['2017-05-01 00:00:00', 1.08]
        E.g. [ ['2017-05-01 00:00:00', 1.08], ['2017-05-01 01:00:00', 2.04], ... ]

        :param boolean upsert: If True, upsert existing values ON DUPLICATE KEY. Default is False.
        Ref: 1). https://stackoverflow.com/a/14383794/1461060
             2). https://chartio.com/resources/tutorials/how-to-insert-if-row-does-not-exist-upsert-in-mysql/

        :return int: Affected row count.
        """
        rowCount = 0
        try:
            with self.connection.cursor() as cursor:
                sql = "INSERT INTO `data` (`id`, `time`, `value`) VALUES (%s, %s, %s)"

                if upsert:
                    sql = "INSERT INTO `data` (`id`, `time`, `value`) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)"

                # Refer to performance in copy list : https://stackoverflow.com/a/2612990/1461060
                timeseriesCopy = []
                for item in timeseries: timeseriesCopy.append(item[:])

                newTimeseries = []
                for t in [i for i in timeseriesCopy]:
                    if len(t) > 1:
                        # Format value into 3 decimal palaces
                        t[1] = round(float(t[1]), 3)
                        # Insert EventId in font of timestamp, value list
                        t.insert(0, eventId)
                        newTimeseries.append(t)
                    else:
                        logging.warning('Invalid timeseries data:: %s', t)

                logging.debug(newTimeseries[:10])
                rowCount = cursor.executemany(sql, (newTimeseries))
                self.connection.commit()

                sql = "UPDATE `run` SET `start_date`=(SELECT MIN(time) from `data` WHERE id=%s), `end_date`=(SELECT MAX(time) from `data` WHERE id=%s) WHERE id=%s"
                cursor.execute(sql, (eventId, eventId, eventId))
                self.connection.commit()

        except Exception as e:
            traceback.print_exc()
        finally:
            return rowCount

    def deleteTimeseries(self, eventId):
        """Delete given timeseries from the database

        :param string eventId: Hex Hash value that need to delete timeseries against

        :return int: Affected row count.
        """
        rowCount = 0
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

                rowCount = cursor.execute(sql[0], (eventId))
                self.connection.commit()

        except Exception as e:
            traceback.print_exc()
        finally:
            return rowCount

    def getEventIds(self, metaQuery={}, opts={}):
        """Get event ids set according to given meta data

        :param dict metaQuery: Dict of Meta Query that use to search the hash
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
        try:
            if not opts.get('limit'):
                opts['limit'] = 100
            if not opts.get('skip'):
                opts['skip'] = 0

            with self.connection.cursor() as cursor:
                outOrder = []
                sortedKeys = ['id'] + self.meta_struct_keys
                for key in sortedKeys:
                    outOrder.append("`%s` as `%s`" % (key, key))
                outOrder = ','.join(outOrder)

                sql = "SELECT %s FROM `run_view` " % (outOrder)
                if metaQuery:
                    sql += "WHERE "
                    cnt = 0
                    for key in metaQuery:
                        if cnt:
                            sql += "AND "

                        # TODO: Need to update start and end date of timeseries
                        if key is 'from':
                            # sql += "`%s`>=\"%s\" " % ('start_date', metaQuery[key])
                            sql = sql[:-4]
                        elif key is 'to':
                            # sql += "`%s`<=\"%s\" " % ('start_date', metaQuery[key])
                            sql = sql[:-4]
                        elif key is 'station' and isinstance(metaQuery[key], list):
                            sql += "`%s` in (%s) " % (key, ','.join('\"%s\"' % (x) for x in metaQuery[key]))
                        else:
                            sql += "`%s`=\"%s\" " % (key, metaQuery[key])
                        cnt += 1

                logging.debug('sql (getEventIds):: %s', sql)
                cursor.execute(sql)
                events = cursor.fetchmany(opts.get('limit'))
                logging.debug('Events (getEventIds):: %s', events)
                response = []
                for event in events:
                    meta_struct = dict(self.meta_struct)
                    for i, value in enumerate(sortedKeys):
                        meta_struct[sortedKeys[i]] = event[i]
                    response.append(meta_struct)

                return response

        except Exception as e:
            traceback.print_exc()

    def retrieveTimeseries(self, metaQuery=[], opts={}):
        """Get timeseries

        :param (dict | list) metaQuery: If Meta Query is a Dict, then it'll use to search the hash
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
            'to': '2017-05-06 23:00:00'
        }

        :return list: Return list of objects with the timeseries data for given matching events
        """
        try:
            if not opts.get('limit'):
                opts['limit'] = 100
            if not opts.get('skip'):
                opts['skip'] = 0

            with self.connection.cursor() as cursor:
                if isinstance(metaQuery, dict):
                    eventIds = self.getEventIds(metaQuery)
                else:
                    eventIds = list(metaQuery)

                logging.debug('eventIds :: %s', eventIds)
                response = []
                for event in eventIds:
                    if isinstance(event, dict):
                        eventId = event.get('id')
                    else:
                        eventId = event
                        event = {'id': eventId}

                    sql = "SELECT `time`,`value` FROM `data` WHERE `id`=\"%s\" " % (eventId)

                    if opts.get('from'):
                        sql += "AND `%s`>=\"%s\" " % ('time', opts['from'])
                    if opts.get('to'):
                        sql += "AND `%s`<=\"%s\" " % ('time', opts['to'])

                    logging.debug('sql (retrieveTimeseries):: %s', sql)
                    cursor.execute(sql)
                    timeseries = cursor.fetchall()
                    event['timeseries'] = [[time, value] for time, value in timeseries]
                    response.append(event)

                return response
        except Exception as e:
            traceback.print_exc()

    def createStation(self, station=[]):
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
        [<ID>, <STATION_ID>, <NAME>, <LATITUDE>, <LONGITUDE>, <RESOLUTION>, <DESCRIPTION>] Or
        (<ID>, <STATION_ID>, <NAME>, <LATITUDE>, <LONGITUDE>, <RESOLUTION>, <DESCRIPTION>)
        """
        rowCount = 0
        try:
            with self.connection.cursor() as cursor:
                # TODO: station Id validation and fill out default values
                sql = "INSERT INTO `station` (`id`, `stationId`, `name`, `latitude`, `longitude`, `resolution`, `description`) VALUES (%s, %s, %s, %s, %s, %s, %s)"

                logging.debug('Create Station: %s', station)
                rowCount = cursor.execute(sql, station)
                self.connection.commit()
                logging.debug('Created Station # %s', rowCount)

        except Exception as e:
            traceback.print_exc()
        finally:
            return rowCount

    def getStation(self, query={}):
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
        try:
            with self.connection.cursor() as cursor:
                outPutOrder = []
                for key in self.station_struct_keys:
                    outPutOrder.append("`%s` as `%s`" % (key, key))
                outPutOrder = ','.join(outPutOrder)

                sql = "SELECT %s FROM `station` " % (outPutOrder)
                if query:
                    sql += "WHERE "
                    cnt = 0
                    for key in query:
                        if cnt:
                            sql += "AND "

                        sql += "`%s`=\"%s\" " % (key, query[key])
                        cnt += 1

                logging.debug('sql (getStation):: %s', sql)
                cursor.execute(sql)
                station = cursor.fetchone()
                response = {}
                for i, value in enumerate(self.station_struct_keys):
                    response[value] = station[i]
                logging.debug('station:: %s', response)
                return response

        except Exception as e:
            traceback.print_exc()

    def deleteStation(self, id=0, stationId=''):
        """Delete given station from the database

        :param integer id: Station Id
        :param string stationId: Station Id

        :return int: Affected row count.
        """
        rowCount = 0
        try:
            with self.connection.cursor() as cursor:
                if id > 0:
                    sql = "DELETE FROM `station` WHERE `id`=%s"
                    rowCount = cursor.execute(sql, (id))
                    self.connection.commit()
                elif stationId:
                    sql = "DELETE FROM `station` WHERE `stationId`=%s"

                    rowCount = cursor.execute(sql, (stationId))
                    self.connection.commit()
                else:
                    logging.warning('Unable to find station')

        except Exception as e:
            traceback.print_exc()
        finally:
            return rowCount

    def getStations(self, query={}):
        """
        Get matching stations details for given query.

        :param query:
        :return:
        """
        return []

    def getStationsInArea(self, query={}):
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
                outOrder = []
                sortedKeys = sorted(self.station_struct.keys())
                for key in sortedKeys:
                    outOrder.append("`%s` as `%s`" % (key, key))
                outOrder = ','.join(outOrder)

                sql = "SELECT %s FROM `station` " % (outOrder)
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

                logging.debug('sql (getStations):: %s', sql)
                cursor.execute(sql)
                stations = cursor.fetchall()
                response = []
                for station in stations:
                    station_struct = dict(self.station_struct)
                    for i, value in enumerate(sortedKeys):
                        station_struct[sortedKeys[i]] = station[i]
                    response.append(station_struct)

                return response
        except Exception as e:
            traceback.print_exc()

    def close(self):
        # disconnect from server
        self.connection.close()
