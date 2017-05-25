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


    def getEventId(self, metaData) :
        '''Get the event id for given meta data

        :param dict metaData: Dict of Meta Data that use to create the hash
        Meta Data should contains all of following keys s.t.
        {
            'station': 'Hanwella',
            'variable': 'Discharge',
            'unit': 'm3/s',
            'type': 'Forecast',
            'source': 'HEC-HMS',
            'name': 'HEC-HMS 1st',
            'start_date': '2017-05-01 00:00:00',
            'end_date': '2017-05-03 23:00:00'
        }
        If start_date is not set, use default date as "01 Jan 1970 00:00:00 GMT"
        If end_date   is not set, use default date as "01 Jan 2050 00:00:00 GMT"

        :return str: sha256 hash value in hex format (length of 64 characters)
        '''
        eventId = None
        m = hashlib.sha256()

        if 'start_date' in metaData :
            metaData['start_date'] = '1970-01-01 00:00:00'
        if 'end_date' in metaData :
            metaData['end_date'] = '2050-01-01 00:00:00'

        m.update(json.dumps(metaData, sort_keys=True).encode("ascii"))
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
        m = hashlib.sha256()
        m.update(json.dumps(metaData, sort_keys=True).encode("ascii"))
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
                
                if 'start_date' in metaData :
                    metaData['start_date'] = '1970-01-01 00:00:00'
                if 'end_date' in metaData :
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

    def insertTimeseries(self, eventId, timeseries) :
        '''Insert timeseries into the db against given eventId'''
        rowCount = 0
        try:
            with self.connection.cursor() as cursor:
                sql = "INSERT INTO `data` (`id`, `time`, `value`) VALUES (%s, %s, %s)"

                newTimeseries = []
                for t in timeseries :
                    t[1] = round(float(t[1]), 3)
                    t.insert(0, eventId)
                    newTimeseries.append(t)

                # print(newTimeseries[:10])
                rowCount = cursor.executemany(sql, (newTimeseries))
                self.connection.commit()

        except Exception as e :
            traceback.print_exc()
        finally:
            return rowCount

    def deleteTimeseries(self, eventId) :
        '''Delete given timeseries from the database'''
        rowCount = 0
        try:
            with self.connection.cursor() as cursor:
                sql = "DELETE FROM `data` WHERE `id`=%s"

                rowCount = cursor.execute(sql, (eventId))
                self.connection.commit()

        except Exception as e :
            traceback.print_exc()
        finally:
            return rowCount

    def getEventIds(self) :
        '''Get event ids set according to given meta data'''
        print('getEventIds not implemented.')

    def retrieveTimeseries(self) :
        '''Get timeseries'''
        print('retrieveTimeSeries not implemented.')

    def close(self) :
        # disconnect from server
        self.connection.close()

