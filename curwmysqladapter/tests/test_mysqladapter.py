import unittest2 as unittest
import traceback, json, os, glob, csv, datetime

from curwmysqladapter import mysqladapter

class MySQLAdapterTest(unittest.TestCase) :

    @classmethod
    def setUpClass(self):
        print('setUpClass')
        try :
            ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
            CONFIG = json.loads(open(ROOT_DIR + '/CONFIG.json').read())

            MYSQL_HOST="localhost"
            MYSQL_USER="root"
            MYSQL_DB="curw"
            MYSQL_PASSWORD=""

            if 'MYSQL_HOST' in CONFIG :
                MYSQL_HOST = CONFIG['MYSQL_HOST']
            if 'MYSQL_USER' in CONFIG :
                MYSQL_USER = CONFIG['MYSQL_USER']
            if 'MYSQL_DB' in CONFIG :
                MYSQL_DB = CONFIG['MYSQL_DB']
            if 'MYSQL_PASSWORD' in CONFIG :
                MYSQL_PASSWORD = CONFIG['MYSQL_PASSWORD']

            self.adapter = mysqladapter(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB)
            self.eventIds = []

            # Store Rainfall Data
            stations = ['Colombo', 'Hanwella']
            metaData = {
                'station': 'Hanwella',
                'variable': 'Precipitation',
                'unit': 'mm',
                'type': 'Forecast',
                'source': 'WRF',
                'name': 'Forecast Current',
                'start_date': '2017-05-01 00:00:00',
                'end_date': '2017-05-03 23:00:00'
            }
            RAINFALL_DIR = os.path.join(ROOT_DIR, 'data', 'Rainfall')
            print(RAINFALL_DIR)
            for station in stations :
                print('Inserting Rainfall at ', station)
                for file in glob.glob(os.path.join(RAINFALL_DIR, station + '*.csv')):
                    timeseries = csv.reader(open(file, 'r'), delimiter=' ', skipinitialspace=True)
                    timeseries = list(timeseries)

                    filename = os.path.basename(file)
                    dateArr = filename.split('.')[0].split('-')
                    print('-'.join(dateArr[1:]))
                    startDate = datetime.datetime.strptime('-'.join(dateArr[1:]), '%Y-%m-%d')
                    endDate = startDate + datetime.timedelta(hours=len(timeseries))
                    print(startDate, endDate)
                    stationMeta = dict(metaData)
                    stationMeta['station'] = station
                    stationMeta['start_date'] = startDate.strftime("%Y-%m-%d %H:%M:%S")
                    stationMeta['end_date'] = endDate.strftime("%Y-%m-%d %H:%M:%S")

                    eventId = self.adapter.getEventId(stationMeta)
                    if eventId is None :
                        eventId = self.adapter.createEventId(stationMeta)
                        self.eventIds.append(eventId)
                        print('HASH SHA256 created: ', eventId)
                        # for l in timeseries[:3] + timeseries[-2:] :
                        #     print(l)
                        rowCount = self.adapter.insertTimeseries(eventId, timeseries)
                        print('%s rows inserted.' % rowCount)
                    else :
                        self.eventIds.append(eventId)
                        print('HASH SHA256 exists: ', eventId)
                        deleteCount = self.adapter.deleteTimeseries(eventId)
                        print('%s rows deleted.' % deleteCount)
                        # for l in timeseries[:3] + timeseries[-2:] :
                        #     print(l)
                        rowCount = self.adapter.insertTimeseries(eventId, timeseries)
                        print('%s rows inserted.' % rowCount)


            # Store Discharge Data
            stations = ['Hanwella']
            metaData = {
                'station': 'Hanwella',
                'variable': 'Discharge',
                'unit': 'm3/s',
                'type': 'Forecast',
                'source': 'HEC-HMS',
                'name': 'Forecast Current',
                'start_date': '2017-05-01 00:00:00',
                'end_date': '2017-05-03 23:00:00'
            }
            DISCHARGE_DIR = os.path.join(ROOT_DIR, 'data', 'Discharge')
            for station in stations :
                print('Inserting Discharges at ', station)
                for file in glob.glob(os.path.join(DISCHARGE_DIR, station + '*.csv')):
                    timeseries = csv.reader(open(file, 'r'), delimiter=',', skipinitialspace=True)
                    timeseries = list(timeseries)

                    filename = os.path.basename(file)
                    dateArr = filename.split('.')[0].split('-')
                    print('-'.join(dateArr[1:]))
                    startDate = datetime.datetime.strptime('-'.join(dateArr[1:]), '%Y-%m-%d')
                    endDate = startDate + datetime.timedelta(hours=len(timeseries))
                    print(startDate, endDate)
                    stationMeta = dict(metaData)
                    stationMeta['station'] = station
                    stationMeta['start_date'] = startDate.strftime("%Y-%m-%d %H:%M:%S")
                    stationMeta['end_date'] = endDate.strftime("%Y-%m-%d %H:%M:%S")

                    eventId = self.adapter.getEventId(stationMeta)
                    if eventId is None :
                        eventId = self.adapter.createEventId(stationMeta)
                        self.eventIds.append(eventId)
                        print('HASH SHA256 : ', eventId)
                        # for l in timeseries[:3] + timeseries[-2:] :
                        #     print(l)
                        rowCount = self.adapter.insertTimeseries(eventId, timeseries)
                        print('%s rows inserted.' % rowCount)
                    else :
                        self.eventIds.append(eventId)
                        print('HASH SHA256 : ', eventId)
                        deleteCount = self.adapter.deleteTimeseries(eventId)
                        print('%s rows deleted.' % deleteCount)
                        # for l in timeseries[:3] + timeseries[-2:] :
                        #     print(l)
                        rowCount = self.adapter.insertTimeseries(eventId, timeseries)
                        print('%s rows inserted.' % rowCount)


        except Exception as e :
            traceback.print_exc()

    @classmethod
    def tearDownClass(self):
        print('tearDownClass')
        try :
            for id in self.eventIds :
                self.adapter.deleteTimeseries(id)
            self.adapter.close()
        except Exception as e :
            traceback.print_exc()

    def setUp(self):
        print('setUp')

    def tearDown(self):
        print('tearDown')

    def test_getEventIdsWithEmptyQuery(self) :
        response = self.adapter.getEventIds()
        self.assertEqual(len(response), 12)

    def test_getEventIdsForGivenDate(self) :
        metaQuery = {
            'station': 'Hanwella',
            'variable': 'Precipitation',
            'type': 'Forecast',
            'start_date': '2017-05-30 00:00:00'
        }
        response = self.adapter.getEventIds(metaQuery)   
        self.assertEqual(len(response), 1)
        timeseries = self.adapter.retrieveTimeseries(response)
        self.assertEqual(len(timeseries[0]['timeseries']), 72)

    def test_retrieveTimeseriesForGivenDate(self) :
        metaQuery = {
            'station': 'Hanwella',
            'variable': 'Precipitation',
            'type': 'Forecast',
            'start_date': '2017-05-30 00:00:00'
        }
        timeseries = self.adapter.retrieveTimeseries(metaQuery)
        self.assertEqual(len(timeseries[0]['timeseries']), 72)
        self.assertEqual(len(timeseries), 1)

    def test_retrieveTimeseriesFromToDate(self) :
        metaQuery = {
            'station': 'Hanwella',
            'variable': 'Precipitation',
            'type': 'Forecast',
            'from': '2017-05-30 00:00:00',
            'to': '2017-06-01 23:00:00'
        }
        timeseries = self.adapter.retrieveTimeseries(metaQuery)
        self.assertEqual(len(timeseries[0]['timeseries']), 72)
        self.assertEqual(len(timeseries), 3)

    def test_getStations(self) :
        query = {
            'latitude_lower': '6.83564',
            'longitude_lower': '80.0817',
            'latitude_upper': '7.18517',
            'longitude_upper': '80.6147'
        }
        stations = self.adapter.getStations(query)
        self.assertEqual(len(stations), 5)
        self.assertTrue('name' in stations[0])
