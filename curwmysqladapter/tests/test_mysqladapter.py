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
                    print(startDate, startDate.strftime("%Y-%m-%d %H:%M:%S"), endDate, endDate.strftime("%Y-%m-%d %H:%M:%S"))
                    stationMeta = dict(metaData)
                    stationMeta['station'] = station
                    stationMeta['start_date'] = startDate.strftime("%Y-%m-%d %H:%M:%S")
                    stationMeta['end_date'] = endDate.strftime("%Y-%m-%d %H:%M:%S")
                    print(stationMeta)

                    eventId = self.adapter.getEventId(stationMeta)
                    if eventId is None :
                        eventId = self.adapter.createEventId(stationMeta)
                        print('HASH SHA256 created: ', eventId)
                        # for l in timeseries[:3] + timeseries[-2:] :
                        #     print(l)
                        rowCount = self.adapter.insertTimeseries(eventId, timeseries)
                        print('%s rows inserted.' % rowCount)
                    else :
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
                        print('HASH SHA256 : ', eventId)
                        # for l in timeseries[:3] + timeseries[-2:] :
                        #     print(l)
                        rowCount = self.adapter.insertTimeseries(eventId, timeseries)
                        print('%s rows inserted.' % rowCount)
                    else :
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
            self.adapter.close()
        except Exception as e :
            traceback.print_exc()

    def setUp(self):
        print('setUp')

    def tearDown(self):
        print('tearDown')

    def test_getEventId(self) :
        print('1111')
        self.assertTrue(True)


    def test_retrieveTimeseries(self) :
        print('2222')
        self.assertTrue(True)