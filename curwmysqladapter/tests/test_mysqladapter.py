import csv
import datetime
import json
import os
import logging, logging.config
import traceback
from glob import glob

import unittest2 as unittest

from curwmysqladapter import mysqladapter, Station

class MySQLAdapterTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        try:
            ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
            CONFIG = json.loads(open(ROOT_DIR + '/CONFIG.json').read())

            # Initialize Logger
            logging_config = json.loads(open(ROOT_DIR + '/LOGGING_CONFIG.json').read())
            logging.config.dictConfig(logging_config)
            self.logger = logging.getLogger('MySQLAdapterTest')
            self.logger.addHandler(logging.StreamHandler())
            self.logger.info('setUpClass')

            MYSQL_HOST = "localhost"
            MYSQL_USER = "root"
            MYSQL_DB = "curw"
            MYSQL_PASSWORD = ""

            DAY_INTERVAL = 24

            if 'MYSQL_HOST' in CONFIG:
                MYSQL_HOST = CONFIG['MYSQL_HOST']
            if 'MYSQL_USER' in CONFIG:
                MYSQL_USER = CONFIG['MYSQL_USER']
            if 'MYSQL_DB' in CONFIG:
                MYSQL_DB = CONFIG['MYSQL_DB']
            if 'MYSQL_PASSWORD' in CONFIG:
                MYSQL_PASSWORD = CONFIG['MYSQL_PASSWORD']

            self.adapter = mysqladapter(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
            self.eventIds = []

            # Store Rainfall Data
            stations = ['Colombo', 'Hanwella', 'Norwood']
            types = ['Forecast-0-d', 'Forecast-1-d-after', 'Forecast-2-d-after']
            metaData = {
                'station': 'Hanwella',
                'variable': 'Precipitation',
                'unit': 'mm',
                'type': 'Forecast-0-d',
                'source': 'WRF',
                'name': 'Forecast Test',
                'start_date': '2017-05-01 00:00:00',
                'end_date': '2017-05-03 23:00:00'
            }
            RAINFALL_DIR = os.path.join(ROOT_DIR, 'data', 'Rainfall')
            self.logger.debug(RAINFALL_DIR)
            for station in stations:
                self.logger.info('Inserting Rainfall at %s', station)
                for file in glob(os.path.join(RAINFALL_DIR, station + '*.csv')):
                    timeseries = csv.reader(open(file, 'r'), delimiter=' ', skipinitialspace=True)
                    timeseries = list(timeseries)

                    filename = os.path.basename(file)
                    dateArr = filename.split('.')[0].split('-')
                    self.logger.debug('-'.join(dateArr[1:]))
                    startDate = datetime.datetime.strptime('-'.join(dateArr[1:]), '%Y-%m-%d')
                    endDate = startDate + datetime.timedelta(hours=len(timeseries))
                    self.logger.debug('startDate: %s, endDate: %s', startDate, endDate)
                    stationMeta = dict(metaData)
                    stationMeta['station'] = station
                    stationMeta['start_date'] = startDate.strftime("%Y-%m-%d %H:%M:%S")
                    stationMeta['end_date'] = endDate.strftime("%Y-%m-%d %H:%M:%S")

                    for i in range(0, 3):
                        stationMeta['type'] = types[i]
                        eventId = self.adapter.getEventId(stationMeta)
                        if eventId is None:
                            eventId = self.adapter.createEventId(stationMeta)
                            self.logger.debug('HASH SHA256 created: %s', eventId)
                        else:
                            self.logger.debug('HASH SHA256 exists: %s', eventId)

                        # for l in timeseries[:3] + timeseries[-2:] :
                        #     print(l)
                        if eventId not in self.eventIds:
                            self.eventIds.append(eventId)
                        rowCount = self.adapter.insertTimeseries(eventId,
                                                                 timeseries[i * DAY_INTERVAL:(i + 1) * DAY_INTERVAL],
                                                                 True)
                        self.logger.debug('%s rows inserted.', rowCount)
            self.logger.info('Inserted Rainfall data.')

            # Store Discharge Data
            stations = ['Hanwella']
            types = [
                'Forecast-0-d',
                'Forecast-1-d-after',
                'Forecast-2-d-after',
                'Forecast-3-d-after',
                'Forecast-4-d-after',
                'Forecast-5-d-after'
            ]
            metaData = {
                'station': 'Hanwella',
                'variable': 'Discharge',
                'unit': 'm3/s',
                'type': 'Forecast-0-d',
                'source': 'HEC-HMS',
                'name': 'Forecast Test',
                'start_date': '2017-05-01 00:00:00',
                'end_date': '2017-05-03 23:00:00'
            }
            DISCHARGE_DIR = os.path.join(ROOT_DIR, 'data', 'Discharge')
            for station in stations:
                self.logger.info('Inserting Discharges at %s', station)
                for file in glob(os.path.join(DISCHARGE_DIR, station + '*.csv')):
                    timeseries = csv.reader(open(file, 'r'), delimiter=',', skipinitialspace=True)
                    timeseries = list(timeseries)

                    filename = os.path.basename(file)
                    dateArr = filename.split('.')[0].split('-')
                    self.logger.debug('-'.join(dateArr[1:]))
                    startDate = datetime.datetime.strptime('-'.join(dateArr[1:]), '%Y-%m-%d')
                    endDate = startDate + datetime.timedelta(hours=len(timeseries))
                    self.logger.debug('startDate: %s, endDate: %s', startDate, endDate)
                    stationMeta = dict(metaData)
                    stationMeta['station'] = station
                    stationMeta['start_date'] = startDate.strftime("%Y-%m-%d %H:%M:%S")
                    stationMeta['end_date'] = endDate.strftime("%Y-%m-%d %H:%M:%S")

                    for i in range(0, 6):
                        stationMeta['type'] = types[i]
                        eventId = self.adapter.getEventId(stationMeta)
                        if eventId is None:
                            eventId = self.adapter.createEventId(stationMeta)
                            self.logger.debug('HASH SHA256 created: %s', eventId)
                        else:
                            self.logger.debug('HASH SHA256 exists: %s', eventId)

                        # for l in timeseries[:3] + timeseries[-2:] :
                        #     print(l)
                        if eventId not in self.eventIds:
                            self.eventIds.append(eventId)
                        rowCount = self.adapter.insertTimeseries(eventId,
                                                                 timeseries[i * DAY_INTERVAL:(i + 1) * DAY_INTERVAL],
                                                                 True)
                        self.logger.debug('%s rows inserted.', rowCount)
            self.logger.info("Inserted Discharge data.")


        except Exception as e:
            traceback.print_exc()

    @classmethod
    def tearDownClass(self):
        print('tearDownClass')
        try:
            for id in self.eventIds:
                self.adapter.deleteTimeseries(id)
            self.adapter.close()
        except Exception as e:
            traceback.print_exc()

    def setUp(self):
        self.logger.info('setUp')

    def tearDown(self):
        self.logger.info('tearDown')

    def test_getEventIdExists(self):
        metaData = {
            'station': 'Hanwella',
            'variable': 'Discharge',
            'unit': 'm3/s',
            'type': 'Forecast-0-d',
            'source': 'HEC-HMS',
            'name': 'Forecast Test'
        }
        eventId = self.adapter.getEventId(metaData)
        self.assertTrue(isinstance(eventId, str))
        self.assertTrue(eventId.isalnum())

    def test_getEventIdNotExists(self):
        metaData = {
            'station': 'Hanwella',
            'variable': 'Discharge',
            'unit': 'm3/s',
            'type': 'Forecast-0-d',
            'source': 'HEC-HMS',
            'name': 'Forecast Test Not Exists'
        }
        eventId = self.adapter.getEventId(metaData)
        self.assertTrue(eventId is None)

    def test_getEventIdsWithEmptyQuery(self):
        response = self.adapter.getEventIds()
        self.assertEqual(len(response), 15)

    def test_getEventIdsForGivenStation(self):
        metaQuery = {
            'station': 'Hanwella',
            'variable': 'Precipitation',
            'type': 'Forecast-0-d',
        }
        response = self.adapter.getEventIds(metaQuery)
        self.assertEqual(len(response), 1)
        timeseries = self.adapter.retrieveTimeseries(response)
        self.assertEqual(len(timeseries[0]['timeseries']), 96)

    def test_getEventIdsForListOfStations(self):
        metaQuery = {
            'station': ['Hanwella', 'Colombo'],
            'variable': 'Precipitation',
            'type': 'Forecast-0-d',
        }
        response = self.adapter.getEventIds(metaQuery)
        self.assertEqual(len(response), 2)
        timeseries = self.adapter.retrieveTimeseries(response)
        self.assertEqual(len(timeseries[0]['timeseries']), 96)

    def test_retrieveTimeseriesFromToDate(self):
        metaQuery = {
            'station': 'Hanwella',
            'variable': 'Precipitation',
            'type': 'Forecast-0-d'
        }
        opts = {
            'from': '2017-05-31 00:00:00',
            'to': '2017-06-01 23:00:00'
        }
        timeseries = self.adapter.retrieveTimeseries(metaQuery, opts)
        self.assertEqual(len(timeseries[0]['timeseries']), 48)
        self.assertEqual(len(timeseries), 1)

    def test_createStation(self):
        station = (Station.CUrW, 'curw_test_station', 'Test Station', 7.111666667, 80.14983333, 0, "Testing Adapter")
        self.logger.info(station)
        rowCount = self.adapter.createStation(station)
        self.assertEqual(rowCount, 1)
        rowCount = self.adapter.deleteStation(stationId=station[1])
        self.assertEqual(rowCount, 1)

    def test_createStationWithPublic(self):
        station = (Station.Public, 'curw_test_station', 'Test Station', 7.111666667, 80.14983333, 0, "Testing Adapter")
        self.logger.info(station)
        rowCount = self.adapter.createStation(station)
        self.assertEqual(rowCount, 1)
        rowCount = self.adapter.deleteStation(stationId=station[1])
        self.assertEqual(rowCount, 1)

    def test_createStationWithList(self):
        station = [Station.Satellite, 'curw_test_station', 'Test Station', 7.111666667, 80.14983333, 0, "Testing Adapter"]

        rowCount = self.adapter.createStation(station)
        self.assertEqual(rowCount, 1)
        rowCount = self.adapter.deleteStation(stationId=station[1])
        self.assertEqual(rowCount, 1)

    def test_createStationWithGivenId(self):
        station = [110001, 'curw_test_station', 'Test Station', 7.111666667, 80.14983333, 0, "Testing Adapter"]

        rowCount = self.adapter.createStation(station)
        self.assertEqual(rowCount, 1)
        rowCount = self.adapter.deleteStation(station[0])
        self.assertEqual(rowCount, 1)

    def test_getStationByName(self):
        query = {
            'name': 'Hanwella'
        }
        station = self.adapter.getStation(query)
        self.assertEqual(len(station.keys()), 7)
        self.assertTrue(isinstance(station, dict))

    def test_getStationsInArea(self):
        query = {
            'latitude_lower': '6.83564',
            'longitude_lower': '80.0817',
            'latitude_upper': '7.18517',
            'longitude_upper': '80.6147'
        }
        stations = self.adapter.getStationsInArea(query)
        self.assertEqual(len(stations), 5)
        self.assertTrue('name' in stations[0])

    # Scenario: All observed rainfall data series within a geographic region
    # (lower-left and upper-right coords provided) from date1 to date2
    def test_retrieveAllTimeseriesInAreaForGivenTime(self):
        query = {
            'latitude_lower': '6.83564',
            'longitude_lower': '80.0817',
            'latitude_upper': '7.18517',
            'longitude_upper': '80.6147'
        }
        stations = self.adapter.getStationsInArea(query)
        self.assertEqual(len(stations), 5)
        self.assertTrue('name' in stations[0])
        stations = [{'name': 'Hanwella'}, {'name': 'Colombo'}]
        stationList = list(map((lambda x: x['name']), stations))

        metaQuery = {
            'station': stationList,
            'variable': 'Precipitation',
        }
        opts = {
            'from': '2017-05-31 00:00:00',
            'to': '2017-06-01 23:00:00'
        }
        timeseries = self.adapter.retrieveTimeseries(metaQuery, opts)
        self.assertEqual(len(timeseries[0]['timeseries']), 48)
        self.assertEqual(len(timeseries), 6)
