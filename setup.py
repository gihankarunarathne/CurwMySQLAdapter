#!/usr/bin/env python
import io
from setuptools import setup, find_packages

with open('./README.md', encoding='utf-8') as f:
    readme = f.read()

setup(name='curwmysqladapter',
      version='0.1',
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
      zip_safe=False)