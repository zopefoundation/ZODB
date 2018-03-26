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
import os
from setuptools import setup, find_packages

version = '5.4.0'

classifiers = """\
Intended Audience :: Developers
License :: OSI Approved :: Zope Public License
Programming Language :: Python
Programming Language :: Python :: 2
Programming Language :: Python :: 2.7
Programming Language :: Python :: 3
Programming Language :: Python :: 3.4
Programming Language :: Python :: 3.5
Programming Language :: Python :: 3.6
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
    for dirpath, _dirnames, filenames in os.walk(base):
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

def read(path):
    with open(path) as f:
        return f.read()

long_description = read("README.rst") + "\n\n" + read("CHANGES.rst")

tests_require = [
    'manuel',
    'zope.testing',
    'zope.testrunner >= 4.4.6',
]

setup(
    name="ZODB",
    version=version,
    author="Jim Fulton",
    author_email="jim@zope.com",
    maintainer="Zope Foundation and Contributors",
    maintainer_email="zodb-dev@zope.org",
    keywords="database nosql python zope",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    url='http://www.zodb.org/',
    license="ZPL 2.1",
    platforms=["any"],
    classifiers=list(filter(None, classifiers.split("\n"))),
    description=long_description.split('\n', 2)[1],
    long_description=long_description,
    test_suite="__main__.alltests", # to support "setup.py test"
    tests_require=tests_require,
    extras_require={
        'test': tests_require,
    },
    install_requires=[
        'persistent >= 4.2.0',
        'BTrees >= 4.2.0',
        'ZConfig',
        'transaction >= 2.0.3',
        'six',
        'zc.lockfile',
        'zope.interface',
        'zodbpickle >= 0.6.0',
    ],
    zip_safe=False,
    entry_points="""
      [console_scripts]
      fsdump = ZODB.FileStorage.fsdump:main
      fsoids = ZODB.scripts.fsoids:main
      fsrefs = ZODB.scripts.fsrefs:main
      fstail = ZODB.scripts.fstail:Main
      repozo = ZODB.scripts.repozo:main
    """,
    include_package_data=True,
    python_requires='>=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*',
)
