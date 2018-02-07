#!/usr/bin/env python
from setuptools import setup

with open('./README.md') as f:
    readme = f.read()

setup(name='curwmysqladapter',
      version='0.2.3',
      description='MySQL Adapter for storing Weather Timeseries',
      long_description=readme,
      url='http://github.com/gihankarunarathne/CurwMySQLAdapter',
      author='Gihan Karunarathne',
      author_email='gckarunarathne@gmail.com',
      license='Apache-2.0',
      packages=['curwmysqladapter'],
      install_requires=[
          'PyMySQL',
      ],
      test_suite='nose.collector',
      tests_require=[
          'nose',
          'unittest2',
      ],
      zip_safe=False
      )
