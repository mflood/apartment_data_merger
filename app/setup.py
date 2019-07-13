#!/usr/bin/env python

from distutils.core import setup

setup(name='digible',
      version='1.0',
      description='Digible coding Challenge',
      author='Matthew Flood',
      author_email='matthew.data.flood@gmail.com',
      url='https://www.github.com/mflood/',
      packages=['digible'],
      install_requires=[
          'PyMySQL',
          'psycopg2-binary',
      ],
     )
