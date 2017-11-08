import csv
import datetime
import json
import os
import logging, logging.config
import traceback
from glob import glob

import unittest2 as unittest

from curwmysqladapter import MySQLAdapter, Station, Data, AdapterError


class MySQLAdapterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            root_dir = os.path.dirname(os.path.realpath(__file__))
            config = json.loads(open(root_dir + '/CONFIG.json').read())

            # Initialize Logger
            logging_config = json.loads(open(root_dir + '/LOGGING_CONFIG.json').read())
            logging.config.dictConfig(logging_config)
            cls.logger = logging.getLogger('MySQLAdapterTest')
            cls.logger.addHandler(logging.StreamHandler())
            cls.logger.info('setUpClass')

            MYSQL_HOST = "localhost"
            MYSQL_USER = "root"
            MYSQL_DB = "curw"
            MYSQL_PASSWORD = ""

            DAY_INTERVAL = 24

            if 'MYSQL_HOST' in config:
                MYSQL_HOST = config['MYSQL_HOST']
            if 'MYSQL_USER' in config:
                MYSQL_USER = config['MYSQL_USER']
            if 'MYSQL_DB' in config:
                MYSQL_DB = config['MYSQL_DB']
            if 'MYSQL_PASSWORD' in config:
                MYSQL_PASSWORD = config['MYSQL_PASSWORD']

            cls.adapter = MySQLAdapter(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
            cls.eventIds = []

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
            RAINFALL_DIR = os.path.join(root_dir, 'data', 'Rainfall')
            cls.logger.debug(RAINFALL_DIR)
            for station in stations:
                cls.logger.info('Inserting Rainfall at %s', station)
                for file in glob(os.path.join(RAINFALL_DIR, station + '*.csv')):
                    timeseries = csv.reader(open(file, 'r'), delimiter=' ', skipinitialspace=True)
                    timeseries = list(timeseries)

                    filename = os.path.basename(file)
                    dateArr = filename.split('.')[0].split('-')
                    cls.logger.debug('-'.join(dateArr[1:]))
                    startDate = datetime.datetime.strptime('-'.join(dateArr[1:]), '%Y-%m-%d')
                    endDate = startDate + datetime.timedelta(hours=len(timeseries))
                    cls.logger.debug('startDate: %s, endDate: %s', startDate, endDate)
                    stationMeta = dict(metaData)
                    stationMeta['station'] = station
                    stationMeta['start_date'] = startDate.strftime("%Y-%m-%d %H:%M:%S")
                    stationMeta['end_date'] = endDate.strftime("%Y-%m-%d %H:%M:%S")

                    for i in range(0, 3):
                        stationMeta['type'] = types[i]
                        eventId = cls.adapter.get_event_id(stationMeta)
                        if eventId is None:
                            eventId = cls.adapter.create_event_id(stationMeta)
                            cls.logger.debug('HASH SHA256 created: %s', eventId)
                        else:
                            cls.logger.debug('HASH SHA256 exists: %s', eventId)

                        # for l in timeseries[:3] + timeseries[-2:] :
                        #     print(l)
                        if eventId not in cls.eventIds:
                            cls.eventIds.append(eventId)
                        rowCount = cls.adapter.insert_timeseries(eventId,
                                                                 timeseries[i * DAY_INTERVAL:(i + 1) * DAY_INTERVAL],
                                                                 True)
                        cls.logger.debug('%s rows inserted.', rowCount)
            cls.logger.info('Inserted Rainfall data.')

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
            DISCHARGE_DIR = os.path.join(root_dir, 'data', 'Discharge')
            for station in stations:
                cls.logger.info('Inserting Discharges at %s', station)
                for file in glob(os.path.join(DISCHARGE_DIR, station + '*.csv')):
                    timeseries = csv.reader(open(file, 'r'), delimiter=',', skipinitialspace=True)
                    timeseries = list(timeseries)

                    filename = os.path.basename(file)
                    dateArr = filename.split('.')[0].split('-')
                    cls.logger.debug('-'.join(dateArr[1:]))
                    startDate = datetime.datetime.strptime('-'.join(dateArr[1:]), '%Y-%m-%d')
                    endDate = startDate + datetime.timedelta(hours=len(timeseries))
                    cls.logger.debug('startDate: %s, endDate: %s', startDate, endDate)
                    stationMeta = dict(metaData)
                    stationMeta['station'] = station
                    stationMeta['start_date'] = startDate.strftime("%Y-%m-%d %H:%M:%S")
                    stationMeta['end_date'] = endDate.strftime("%Y-%m-%d %H:%M:%S")

                    for i in range(0, 6):
                        stationMeta['type'] = types[i]
                        eventId = cls.adapter.get_event_id(stationMeta)
                        if eventId is None:
                            eventId = cls.adapter.create_event_id(stationMeta)
                            cls.logger.debug('HASH SHA256 created: %s', eventId)
                        else:
                            cls.logger.debug('HASH SHA256 exists: %s', eventId)

                        # for l in timeseries[:3] + timeseries[-2:] :
                        #     print(l)
                        if eventId not in cls.eventIds:
                            cls.eventIds.append(eventId)
                        rowCount = cls.adapter.insert_timeseries(eventId,
                                                                 timeseries[i * DAY_INTERVAL:(i + 1) * DAY_INTERVAL],
                                                                 True)
                        cls.logger.debug('%s rows inserted.', rowCount)
            cls.logger.info("Inserted Discharge data.")

            cls.contain_stations = ['Attanagalla', 'Daraniyagala', 'Glencourse', 'Holombuwa', 'Kitulgala']

        except Exception as e:
            traceback.print_exc()

    @classmethod
    def tearDownClass(self):
        print('tearDownClass')
        try:
            for eventId in self.eventIds:
                self.adapter.delete_timeseries(eventId)
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
        eventId = self.adapter.get_event_id(metaData)
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
        eventId = self.adapter.get_event_id(metaData)
        self.assertTrue(eventId is None)

    def test_getEventIdsWithEmptyQuery(self):
        response = self.adapter.get_event_ids()
        self.assertEqual(len(response), 15)

    def test_createEventIdWithReferenceIssueRaiseException(self):
        meta_data = {
            'station': 'Hanwella_Not_Exists',
            'variable': 'Discharge_Not_Exists',
            'unit': 'm3/s_Not_Exists',
            'type': 'Observed_Not_Exists',
            'source': 'HEC-HMS_Not_Exists',
            'name': 'Forecast Test Not Exists'
        }
        try:
            self.adapter.create_event_id(meta_data)
        except AdapterError.DatabaseConstrainAdapterError as de:
            self.assertTrue(isinstance(de, AdapterError.DatabaseConstrainAdapterError))
            self.assertEqual(de.message, 'Could not find station with value Hanwella_Not_Exists')

        meta_data['station'] = 'Hanwella'
        try:
            self.adapter.create_event_id(meta_data)
        except AdapterError.DatabaseConstrainAdapterError as de:
            self.assertTrue(isinstance(de, AdapterError.DatabaseConstrainAdapterError))
            self.assertEqual(de.message, 'Could not find variable with value Discharge_Not_Exists')

        meta_data['variable'] = 'Discharge'
        try:
            self.adapter.create_event_id(meta_data)
        except AdapterError.DatabaseConstrainAdapterError as de:
            self.assertTrue(isinstance(de, AdapterError.DatabaseConstrainAdapterError))
            self.assertEqual(de.message, 'Could not find unit with value m3/s_Not_Exists')

        meta_data['unit'] = 'm3/s'
        try:
            self.adapter.create_event_id(meta_data)
        except AdapterError.DatabaseConstrainAdapterError as de:
            self.assertTrue(isinstance(de, AdapterError.DatabaseConstrainAdapterError))
            self.assertEqual(de.message, 'Could not find type with value Observed_Not_Exists')

        meta_data['type'] = 'Observed'
        try:
            self.adapter.create_event_id(meta_data)
        except AdapterError.DatabaseConstrainAdapterError as de:
            self.assertTrue(isinstance(de, AdapterError.DatabaseConstrainAdapterError))
            self.assertEqual(de.message, 'Could not find source with value HEC-HMS_Not_Exists')

    def test_getEventIdsForGivenStation(self):
        metaQuery = {
            'station': 'Hanwella',
            'variable': 'Precipitation',
            'type': 'Forecast-0-d',
        }
        response = self.adapter.get_event_ids(metaQuery)
        self.assertEqual(len(response), 1)
        timeseries = self.adapter.retrieve_timeseries(response)
        self.assertEqual(len(timeseries[0]['timeseries']), 96)

    def test_getEventIdsForListOfStations(self):
        metaQuery = {
            'station': ['Hanwella', 'Colombo'],
            'variable': 'Precipitation',
            'type': 'Forecast-0-d',
        }
        response = self.adapter.get_event_ids(metaQuery)
        self.assertEqual(len(response), 2)
        timeseries = self.adapter.retrieve_timeseries(response)
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
        timeseries = self.adapter.retrieve_timeseries(metaQuery, opts)
        self.assertEqual(len(timeseries[0]['timeseries']), 48)
        self.assertEqual(len(timeseries), 1)

    def test_insertTimeseriesIntoProcessedData(self):
        meta_query = {
            'station': 'Hanwella',
            'variable': 'Precipitation',
            'type': 'Forecast-0-d',
            'unit': 'mm',
            'source': 'WRF',
            'name': 'Forecast Test',
        }
        opts = {
            'from': '2017-06-01 00:00:00',
            'to': '2017-06-01 23:00:00'
        }
        timeseries = self.adapter.retrieve_timeseries(meta_query, opts)
        self.assertEqual(len(timeseries[0]['timeseries']), 24)
        self.assertEqual(len(timeseries), 1)
        event_id = self.adapter.get_event_id(meta_query)
        self.assertTrue(isinstance(event_id, str))
        self.assertTrue(event_id.isalnum())
        opts = {'mode': Data.processed_data}
        row_count = self.adapter.insert_timeseries(event_id, timeseries[0]['timeseries'], upsert=True, opts=opts)
        self.assertEqual(row_count, 24)

    def test_retrieveTimeseriesFromProcessedData(self):
        meta_query = {
            'station': 'Colombo',
            'variable': 'Precipitation',
            'type': 'Forecast-0-d',
            'unit': 'mm',
            'source': 'WRF',
            'name': 'Forecast Test',
        }
        opts = {
            'from': '2017-06-01 00:00:00',
            'to': '2017-06-01 23:00:00'
        }
        timeseries = self.adapter.retrieve_timeseries(meta_query, opts)
        self.assertEqual(len(timeseries[0]['timeseries']), 24)
        self.assertEqual(len(timeseries), 1)
        event_id = self.adapter.get_event_id(meta_query)
        self.assertTrue(isinstance(event_id, str))
        self.assertTrue(event_id.isalnum())
        opts = {'mode': Data.processed_data}
        row_count = self.adapter.insert_timeseries(event_id, timeseries[0]['timeseries'], upsert=True, opts=opts)
        self.assertEqual(row_count, 24)
        # Get Processed Data
        new_opts = {
            'from': '2017-06-01 00:00:00',
            'to': '2017-06-02 23:00:00',
            'mode': Data.processed_data,
        }
        processed_timeseries = self.adapter.retrieve_timeseries(meta_query, new_opts)
        self.assertEqual(len(processed_timeseries[0]['timeseries']), 24)
        self.assertEqual(len(processed_timeseries), 1)

    def test_createStation(self):
        station = (Station.CUrW, 'curw_test_station', 'Test Station', 7.111666667, 80.14983333, 0, "Testing Adapter")
        self.logger.info(station)
        rowCount = self.adapter.create_station(station)
        self.assertEqual(rowCount, 1)
        rowCount = self.adapter.delete_station(station_id=station[1])
        self.assertEqual(rowCount, 1)

    def test_createStationWithPublic(self):
        station = (Station.Public, 'curw_test_station', 'Test Station', 7.111666667, 80.14983333, 0, "Testing Adapter")
        station2 = (Station.Public, 'curw_test_station2', 'Test Station 2', 7.111666668, 80.14983334, 0, "Testing Adapter")
        # Create first station
        self.logger.info(station)
        rowCount = self.adapter.create_station(station)
        self.assertEqual(rowCount, 1)
        # Create second station
        self.logger.info(station2)
        rowCount = self.adapter.create_station(station2)
        self.assertEqual(rowCount, 1)

        rowCount = self.adapter.delete_station(station_id=station[1])
        self.assertEqual(rowCount, 1)
        rowCount2 = self.adapter.delete_station(station_id=station2[1])
        self.assertEqual(rowCount2, 1)

    def test_createStationWithList(self):
        station = [Station.Satellite, 'curw_test_station', 'Test Station', 7.111666667, 80.14983333, 0, "Testing Adapter"]

        rowCount = self.adapter.create_station(station)
        self.assertEqual(rowCount, 1)
        rowCount = self.adapter.delete_station(station_id=station[1])
        self.assertEqual(rowCount, 1)

    def test_createStationWithGivenId(self):
        station = [110001, 'curw_test_station', 'Test Station', 7.111666667, 80.14983333, 0, "Testing Adapter"]

        rowCount = self.adapter.create_station(station)
        self.assertEqual(rowCount, 1)
        rowCount = self.adapter.delete_station(station[0])
        self.assertEqual(rowCount, 1)

    def test_getStationByName(self):
        query = {
            'name': 'Hanwella'
        }
        station = self.adapter.get_station(query)
        self.assertEqual(len(station.keys()), 7)
        self.assertTrue(isinstance(station, dict))

    def test_getUnavailableStation(self):
        query = {
            'name': 'Unavailable'
        }
        station = self.adapter.get_station(query)
        self.assertEqual(station, None)

    def test_getStationsInArea(self):
        query = {
            'latitude_lower': '6.83564',
            'longitude_lower': '80.0817',
            'latitude_upper': '7.18517',
            'longitude_upper': '80.6147'
        }
        stations = self.adapter.get_stations_in_area(query)
        match_stations = [x for x in stations if x['name'] in self.contain_stations]
        self.assertEqual(len(match_stations), 5)
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
        stations = self.adapter.get_stations_in_area(query)
        match_stations = [x for x in stations if x['name'] in self.contain_stations]
        self.assertEqual(len(match_stations), 5)
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
        timeseries = self.adapter.retrieve_timeseries(metaQuery, opts)
        self.assertEqual(len(timeseries[0]['timeseries']), 48)
        self.assertEqual(len(timeseries), 6)

    def test_sourceCRUDWithTuple(self):
        # Create Source
        parameters = {'key1': 'value1'}
        source = ('FLO2D_v2', json.dumps(parameters))
        self.logger.info(source)
        create_response = self.adapter.create_source(source)
        self.assertEqual(create_response.get('row_count', 0), 1)
        self.assertTrue(create_response.get('status', False))
        new_source = create_response.get('source', {})
        source_id = new_source[0]
        self.assertTrue(source_id > 0)
        # Get Source
        get_response = self.adapter.get_source(source_id)
        self.assertEqual(get_response.get('id'), source_id)
        new_parameters = json.loads(get_response.get('parameters', {}))
        self.assertTrue(new_parameters.get('key1'), parameters.get('key1'))
        # Delete Source
        delete_response = self.adapter.delete_source(source_id)
        self.assertTrue(delete_response.get('status'))
        self.assertEqual(delete_response.get('row_count'), 1)

    def test_sourceCRUDWithList(self):
        # Create Source without Parameters
        source = ['FLO2D_v2']
        self.logger.info(source)
        create_response = self.adapter.create_source(source)
        self.assertEqual(create_response.get('row_count', 0), 1)
        self.assertTrue(create_response.get('status', False))
        new_source = create_response.get('source', {})
        source_id = new_source[0]
        self.assertTrue(source_id > 0)
        # Get Source
        get_response = self.adapter.get_source(source_id)
        self.assertEqual(get_response.get('id'), source_id)
        # Delete Source
        delete_response = self.adapter.delete_source(source_id)
        self.assertTrue(delete_response.get('status'))
        self.assertEqual(delete_response.get('row_count'), 1)

    def test_sourceCRUDWithStr(self):
        # Create Source without Parameters
        source = 'FLO2D_v2'
        self.logger.info(source)
        create_response = self.adapter.create_source(source)
        self.assertEqual(create_response.get('row_count', 0), 1)
        self.assertTrue(create_response.get('status', False))
        new_source = create_response.get('source', {})
        source_id = new_source[0]
        self.assertTrue(source_id > 0)
        # Get Source
        get_response = self.adapter.get_source(source_id)
        self.assertEqual(get_response.get('id'), source_id)
        # Delete Source
        delete_response = self.adapter.delete_source(source_id)
        self.assertTrue(delete_response.get('status'))
        self.assertEqual(delete_response.get('row_count'), 1)

    def test_sourceNotExists(self):
        # Get Source not exists
        get_response = self.adapter.get_source(9999)
        self.assertTrue(len(get_response) < 1)
