##############################################################################
#
# Copyright (c) 2002, 2003 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Zope Object Database: object database and persistence

The Zope Object Database provides an object-oriented database for
Python that provides a high-degree of transparency. Applications can
take advantage of object database features with few, if any, changes
to application logic.  ZODB includes features such as a plugable storage
interface, rich transaction support, and undo.
"""

version = "5.0.0a5"

import os
from setuptools import setup, find_packages

classifiers = """\
Intended Audience :: Developers
License :: OSI Approved :: Zope Public License
Programming Language :: Python
Programming Language :: Python :: 2
Programming Language :: Python :: 2.7
Programming Language :: Python :: 3
Programming Language :: Python :: 3.3
Programming Language :: Python :: 3.4
Programming Language :: Python :: 3.5
Programming Language :: Python :: Implementation :: CPython
Programming Language :: Python :: Implementation :: PyPy
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Microsoft :: Windows
Operating System :: Unix
Framework :: ZODB
"""

def _modname(path, base, name=''):
    if path == base:
        return name
    dirname, basename = os.path.split(path)
    return _modname(dirname, base, basename + '.' + name)

def _flatten(suite, predicate=lambda *x: True):
    from unittest import TestCase
    for suite_or_case in suite:
        if predicate(suite_or_case):
            if isinstance(suite_or_case, TestCase):
                yield suite_or_case
            else:
                for x in _flatten(suite_or_case):
                    yield x

def _no_layer(suite_or_case):
    return getattr(suite_or_case, 'layer', None) is None

def _unittests_only(suite, mod_suite):
    for case in _flatten(mod_suite, _no_layer):
        suite.addTest(case)

def alltests():
    import logging
    import pkg_resources
    import unittest

    # Something wacked in setting recursion limit when running setup test
    import ZODB.FileStorage.tests
    del ZODB.FileStorage.tests._save_index

    class NullHandler(logging.Handler):
        level = 50

        def emit(self, record):
            pass

    logging.getLogger().addHandler(NullHandler())

    suite = unittest.TestSuite()
    base = pkg_resources.working_set.find(
        pkg_resources.Requirement.parse('ZODB')).location
    for dirpath, dirnames, filenames in os.walk(base):
        if os.path.basename(dirpath) == 'tests':
            for filename in filenames:
                if filename.endswith('.py') and filename.startswith('test'):
                    mod = __import__(
                        _modname(dirpath, base, os.path.splitext(filename)[0]),
                        {}, {}, ['*'])
                    _unittests_only(suite, mod.test_suite())
        elif 'tests.py' in filenames:
            mod = __import__(_modname(dirpath, base, 'tests'), {}, {}, ['*'])
            _unittests_only(suite, mod.test_suite())
    return suite

doclines = __doc__.split("\n")

def read_file(*path):
    base_dir = os.path.dirname(__file__)
    file_path = (base_dir, ) + tuple(path)
    with open(os.path.join(*file_path), 'rb') as file:
        return file.read()

long_description = str(
    ("\n".join(doclines[2:]) + "\n\n" +
     ".. contents::\n\n" +
     read_file("README.rst").decode('latin-1')  + "\n\n" +
     read_file("CHANGES.rst").decode('latin-1')))

tests_require = ['zope.testing', 'manuel']

setup(name="ZODB",
      version=version,
      setup_requires=['persistent'],
      author="Jim Fulton",
      author_email="jim@zope.com",
      maintainer="Zope Foundation and Contributors",
      maintainer_email="zodb-dev@zope.org",
      keywords="database nosql python zope",
      packages = find_packages('src'),
      package_dir = {'': 'src'},
      url = 'http://www.zodb.org/',
      license = "ZPL 2.1",
      platforms = ["any"],
      description = doclines[0],
      classifiers = list(filter(None, classifiers.split("\n"))),
      long_description = long_description,
      test_suite="__main__.alltests", # to support "setup.py test"
      tests_require = tests_require,
      extras_require = {
        'test': tests_require,
      },
      install_requires = [
        'persistent >= 4.2.0',
        'BTrees >= 4.2.0',
        'ZConfig',
        'transaction >= 1.6.1',
        'six',
        'zc.lockfile',
        'zope.interface',
        'zodbpickle >= 0.6.0',
      ],
      zip_safe = False,
      entry_points = """
      [console_scripts]
      fsdump = ZODB.FileStorage.fsdump:main
      fsoids = ZODB.scripts.fsoids:main
      fsrefs = ZODB.scripts.fsrefs:main
      fstail = ZODB.scripts.fstail:Main
      repozo = ZODB.scripts.repozo:main
      """,
      include_package_data = True,
      )
