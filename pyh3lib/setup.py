#!/usr/bin/env python3

from setuptools import setup, Extension

from pyh3lib.version import __version__

setup(name='pyh3lib',
      version=__version__,
      description='Python interface to H3: A High speed, High Volume and Highly available object storage',
      url='https://www.ics.forth.gr/carv/',
      author='FORTH-ICS',
      license='Apache-2.0',
      packages=['pyh3lib'],
      ext_modules=[Extension('pyh3lib.h3lib',
                             sources=['pyh3lib/h3lib.c'],
                             libraries=['h3lib'])],
      entry_points={'console_scripts': ['h3cli = pyh3lib.cli:main']},
      python_requires='>=3.6',
      classifiers=['Development Status :: 4 - Beta',
                   'Environment :: Console',
                   'Programming Language :: Python :: 3.6',
                   'Programming Language :: C'
                   'Operating System :: OS Independent',
                   'Topic :: Software Development :: Libraries :: Python Modules'
                   'License :: OSI Approved :: Apache Software License'])
