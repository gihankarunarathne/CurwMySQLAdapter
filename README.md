# CurwMySQLAdapter

### Table of Contents
* Requirements
* Installation
* Documentation
* Example
* License

This package contains MySQL adapter that helps to store and retrieve multiple Weather Series data on MySQL Database.

## Requirements

* Python -- one of the following:
  - [CPython](http://www.python.org/) >= 2.6 or >= 3.3
  - [PyPy](http://pypy.org/) >= 4.0
  - [IronPython](http://ironpython.net/) 2.7
* MySQL Server -- one of the following:
  - [MySQL](http://www.mysql.com/) >= 4.1  (tested with only 5.5~)
  - [MariaDB](https://mariadb.org/) >= 5.1

## Installation

The last stable release is available on PyPI and can be installed with ``pip``::

    $ `pip install git+https://github.com/gihankarunarathne/CurwMySQLAdapter.git`

In order to update the package with latest changes, use `-U` or `--upgrade`

    $ `pip install git+https://github.com/gihankarunarathne/CurwMySQLAdapter.git -U`

## Documentation

Documentation is available online soon: http://curwmysqladapter.readthedocs.io/

## Example

The following examples make use of a simple table

```python
from curwmysqladapter import mysqladapter

# Connect to the database
adapter = mysqladapter(host='localhost',
                        user='user',
                        password='passwd',
                        db='db')

timeseries = [
    ['2017-05-01 00:00:00', 0.01],
    ['2017-05-01 00:01:00', 0.02],
    ['2017-05-01 00:02:00', 0.03],
    ['2017-05-01 00:03:00', 0.04],
    ['2017-05-01 00:04:00', 0.05],
    ['2017-05-01 00:05:00', 0.06]
]
metaData = {
    'station': 'Hanwella',
    'variable': 'Discharge',
    'unit': 'm3/s',
    'type': 'Forecast',
    'source': 'HEC-HMS',
    'name': 'HEC-HMS 1st',
    'start_date': '2017-05-01 00:00:00',
    'end_date': '2017-05-03 23:00:00'
}
eventId = adapter.getEventId(metaData)

if eventId is None :
    print('eventId is None. Creating a New.')
    eventId = adapter.createEventId(metaData)
    rowCount = adapter.insertTimeseries(eventId, timeseries)
    print('%s rows inserted.' % rowCount)
else:
    rowCount = adapter.insertTimeseries(eventId, timeseries, True)
    print('%s rows inserted.' % rowCount)

adapter.close()
```

## Testing

Run test cases with `python setup.py test`

## Resources

PyMySQL http://pymysql.readthedocs.io/en/latest/

## License

CurwMySQLAdapter is released under the Apache-2.0 License. See LICENSE for more information.
