#!/usr/bin/env python3

from setuptools import setup, Extension

setup(name='pyh3',
      version='1.0',
      description='Python interface to H3: A High speed, High Volume and Highly available object storage',
      url='https://www.ics.forth.gr/carv/',
      author='FORTH-ICS',
      license='Apache-2.0',
      packages=['pyh3'],
      ext_modules=[Extension('pyh3.h3lib',
                             sources=['pyh3/h3lib.c'],
                             libraries=['h3lib'])],
      python_requires='>=3.6',
      classifiers=['Development Status :: 3 - Alpha',
                   'Environment :: Console',
                   'Programming Language :: Python :: 3.6',
                   'Programming Language :: C'
                   'Operating System :: OS Independent',
                   'Topic :: Software Development :: Libraries :: Python Modules'
                   'License :: OSI Approved :: Apache Software License'])
