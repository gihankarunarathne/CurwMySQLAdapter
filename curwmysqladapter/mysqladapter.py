#!/usr/bin/python3

import pymysql.cursors, hashlib, collections, json, traceback

class mysqladapter :
    def __init__(self, host="localhost", user="root", password="", db="curw") :
        '''Initialize Database Connection'''
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

        print ("Database version : %s " % data)

        self.metaStruct = {
            'station': '',
            'variable': '',
            'unit': '',
            'type': '',
            'source': '',
            'name': ''
        }
        self.metaStructKeys = sorted(self.metaStruct.keys())
        self.stationStruct = {
            'name': '',
            'latitude': '',
            'longitude': ''
        }

    def getMetaStruct(self) :
        '''Get the Meta Data Structure of hash value
        NOTE: start_date and end_date is not using for hashing
        '''
        return self.metaStruct

    def getStationStruct(self) :
        '''Get the Station Data Structure
        '''
        return self.stationStruct

    def getEventId(self, metaData) :
        '''Get the event id for given meta data
        NOTE: Only 'station', 'variable', 'unit', 'type', 'source', 'name' fields use for generate hash value

        :param dict metaData: Dict of Meta Data that use to create the hash
        Meta Data should contains all required following keys s.t.
        {
            'station': 'Hanwella',
            'variable': 'Discharge',
            'unit': 'm3/s',
            'type': 'Forecast',
            'source': 'HEC-HMS',
            'name': 'Cloud Continuous',
            'start_date': '2017-05-01 00:00:00', // Optional: Start Time of timeseries
            'end_date': '2017-05-03 23:00:00'    // Optional: End Time of timeseries
        }
        If start_date is not set, use default date as "01 Jan 1970 00:00:00 GMT"
        If end_date   is not set, use default date as "01 Jan 2050 00:00:00 GMT"

        :return str: sha256 hash value in hex format (length of 64 characters). If does not exists, return None.
        '''
        eventId = None
        m = hashlib.sha256()

        if 'start_date' not in metaData :
            metaData['start_date'] = '1970-01-01 00:00:00'
        if 'end_date' not in metaData :
            metaData['end_date'] = '2050-01-01 00:00:00'

        hashData = dict(self.metaStruct)
        for i, value in enumerate(self.metaStructKeys) :
            hashData[value] = metaData[value]
        print('hash Data ', hashData)

        m.update(json.dumps(hashData, sort_keys=True).encode("ascii"))
        possibleId = m.hexdigest()
        try:
            with self.connection.cursor() as cursor:
                sql = "SELECT 1 FROM `run` WHERE `id`=%s"

                cursor.execute(sql, possibleId)
                isExist = cursor.fetchone()
                if isExist is not None :
                    eventId = possibleId
        except Exception as e :
            traceback.print_exc()
        finally:
            return eventId


    def createEventId(self, metaData) :
        '''Create a new event id for given meta data

        :param dict metaData: Dict of Meta Data that use to create the hash
        Meta Data should contains all of following keys s.t.
        {
            'station': 'Hanwella',
            'variable': 'Precipitation',
            'unit': 'mm',
            'type': 'Forecast',
            'source': 'WRF',
            'name': 'WRF 1st',
            'start_date': '2017-05-01 00:00:00',
            'end_date': '2017-05-03 23:00:00'
        }
        If start_date is not set, use default date as "01 Jan 1970 00:00:00 GMT"
        If end_date   is not set, use default date as "01 Jan 2050 00:00:00 GMT"

        :return str: sha256 hash value in hex format (length of 64 characters)
        '''
        hashData = dict(self.metaStruct)
        for i, value in enumerate(self.metaStructKeys) :
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

                sql = "INSERT INTO `run` (`id`, `name`, `start_date`, `end_date`, `station`, `variable`, `unit`, `type`, `source`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                
                if 'start_date' not in metaData :
                    metaData['start_date'] = '1970-01-01 00:00:00'
                if 'end_date' not in metaData :
                    metaData['end_date'] = '2050-01-01 00:00:00'

                sqlValues = (
                    eventId,
                    metaData['name'],
                    metaData['start_date'],
                    metaData['end_date'],
                    stationId,
                    variableId,
                    unitId,
                    typeId,
                    sourceId
                )
                cursor.execute(sql, sqlValues)
                self.connection.commit()

        except Exception as e :
            traceback.print_exc()

        return eventId

    def insertTimeseries(self, eventId, timeseries, upsert=False) :
        '''Insert timeseries into the db against given eventId

        :param string eventId: Hex Hash value that need to store timeseries against.

        :param list   timeseries: List of time series of time & value list ['2017-05-01 00:00:00', 1.08]
        E.g. [ ['2017-05-01 00:00:00', 1.08], ['2017-05-01 01:00:00', 2.04], ... ]

        :param boolean upsert: If True, upsert existing values ON DUPLICATE KEY. Default is False.
        Ref: 1). https://stackoverflow.com/a/14383794/1461060 
             2). https://chartio.com/resources/tutorials/how-to-insert-if-row-does-not-exist-upsert-in-mysql/

        :return int: Affected row count.
        '''
        rowCount = 0
        try:
            with self.connection.cursor() as cursor:
                sql = "INSERT INTO `data` (`id`, `time`, `value`) VALUES (%s, %s, %s)"

                if upsert :
                    sql = "INSERT INTO `data` (`id`, `time`, `value`) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)"

                newTimeseries = []
                for t in timeseries :
                    if len(t) > 1 :
                        t[1] = round(float(t[1]), 3)
                        t.insert(0, eventId)
                        newTimeseries.append(t)
                    else :
                        print('Invalid timeseries data :', t)

                # print(newTimeseries[:10])
                rowCount = cursor.executemany(sql, (newTimeseries))
                self.connection.commit()

        except Exception as e :
            traceback.print_exc()
        finally:
            return rowCount

    def deleteTimeseries(self, eventId) :
        '''Delete given timeseries from the database

        :param string eventId: Hex Hash value that need to delete timeseries against

        :return int: Affected row count.
        '''
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

        except Exception as e :
            traceback.print_exc()
        finally:
            return rowCount

    def getEventIds(self, metaQuery={}, opts={}) :
        '''Get event ids set according to given meta data

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
            'end_date': '2017-05-03 23:00:00',

            'from': '2017-05-01 00:00:00',
            'to': '2017-05-06 23:00:00'
        }

        :return list: Return list of event objects which matches the given scenario
        '''
        try:
            if not opts.get('limit') :
                opts['limit'] = 100
            if not opts.get('skip') :
                opts['skip'] = 0

            with self.connection.cursor() as cursor:
                outOrder = []
                sortedKeys = ['id'] + sorted(self.metaStruct.keys())
                for key in sortedKeys :
                    outOrder.append("`%s` as `%s`" % (key, key))
                outOrder = ','.join(outOrder)

                sql = "SELECT %s FROM `run_view` " % (outOrder)
                if metaQuery :
                    sql += "WHERE "
                    cnt = 0
                    for key in metaQuery :
                        if cnt :
                            sql += "AND "

                        if key is 'from' :
                            sql += "`%s`>=\"%s\" " % ('start_date', metaQuery[key])
                        elif key is 'to' :
                            sql += "`%s`<=\"%s\" " % ('start_date', metaQuery[key])
                        elif key is 'station' and isinstance(metaQuery[key], list) :
                            sql += "`%s` in (%s) " % (key, ','.join('\"%s\"' % (x) for x in metaQuery[key]) )
                        else :
                            sql += "`%s`=\"%s\" " % (key, metaQuery[key])
                        cnt += 1

                print('sql::', sql)
                cursor.execute(sql)
                events = cursor.fetchmany(opts.get('limit'))
                response = []
                for event in events :
                    metaStruct = dict(self.metaStruct)
                    for i, value in enumerate(sortedKeys) :
                        metaStruct[sortedKeys[i]] = event[i]
                    response.append(metaStruct)

                return response

        except Exception as e :
            traceback.print_exc()

    def retrieveTimeseries(self, eventIds=[]) :
        '''Get timeseries

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

        :return list: Return list of objects with the timeseries data for given matching events
        '''
        try :
            with self.connection.cursor() as cursor:
                if isinstance(eventIds, dict) :
                    eventIds = self.getEventIds(eventIds)
                response = []
                for event in eventIds :
                    sql = "SELECT `time`,`value` FROM `data` WHERE `id`=%s"
                    if isinstance(event, dict) :
                        eventId = event.get('id')
                    else :
                        eventId = event
                        event = {'id': eventId}
                    cursor.execute(sql, (eventId))
                    timeseries = cursor.fetchall()
                    event['timeseries'] = [[time, value] for time, value in timeseries]
                    response.append(event)

                return response
        except Exception as e :
            traceback.print_exc()

    def getStations(self, query={}) :
        '''Get stations
        
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
        '''
        try :
            with self.connection.cursor() as cursor:
                outOrder = []
                sortedKeys = sorted(self.stationStruct.keys())
                for key in sortedKeys :
                    outOrder.append("`%s` as `%s`" % (key, key))
                outOrder = ','.join(outOrder)

                sql = "SELECT %s FROM `station` " % (outOrder)
                if query :
                    sql += "WHERE "
                    cnt = 0
                    for key in query :
                        if cnt :
                            sql += "AND "

                        if key is 'latitude_lower' :
                            sql += "`%s`>=\"%s\" " % ('latitude', query[key])
                        elif key is 'longitude_lower' :
                            sql += "`%s`>=\"%s\" " % ('longitude', query[key])
                        elif key is 'latitude_upper' :
                            sql += "`%s`<=\"%s\" " % ('latitude', query[key])
                        elif key is 'longitude_upper' :
                            sql += "`%s`<=\"%s\" " % ('longitude', query[key])
                        cnt += 1

                print('sql::', sql)
                cursor.execute(sql)
                stations = cursor.fetchall()
                response = []
                for station in stations :
                    stationStruct = dict(self.stationStruct)
                    for i, value in enumerate(sortedKeys) :
                        stationStruct[sortedKeys[i]] = station[i]
                    response.append(stationStruct)

                return response
        except Exception as e :
            traceback.print_exc()

    def close(self) :
        # disconnect from server
        self.connection.close()


