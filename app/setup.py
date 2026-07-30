#!/usr/bin/env python

from distutils.core import setup

setup(name='aptmerge',
      version='1.0',
      description='Apartment listing ETL and merge pipeline',
      author='Matthew Flood',
      author_email='matthew.data.flood@gmail.com',
      url='https://www.github.com/mflood/',
      packages=['aptmerge'],
      install_requires=[
          'PyMySQL',
          'psycopg2-binary',
      ],
     )
