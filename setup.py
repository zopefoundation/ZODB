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

VERSION = "3.11dev"

from ez_setup import use_setuptools
use_setuptools()

from setuptools import setup, find_packages
from setuptools.extension import Extension
import os
import sys

if sys.version_info < (2, 5):
    print "This version of ZODB requires Python 2.5 or higher"
    sys.exit(0)

# The (non-obvious!) choices for the Trove Development Status line:
# Development Status :: 5 - Production/Stable
# Development Status :: 4 - Beta
# Development Status :: 3 - Alpha

classifiers = """\
Intended Audience :: Developers
License :: OSI Approved :: Zope Public License
Programming Language :: Python
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Microsoft :: Windows
Operating System :: Unix
Framework :: ZODB
"""

# Include directories for C extensions
include = ['src']

# Set up dependencies for the BTrees package
base_btrees_depends = [
    "src/BTrees/BTreeItemsTemplate.c",
    "src/BTrees/BTreeModuleTemplate.c",
    "src/BTrees/BTreeTemplate.c",
    "src/BTrees/BucketTemplate.c",
    "src/BTrees/MergeTemplate.c",
    "src/BTrees/SetOpTemplate.c",
    "src/BTrees/SetTemplate.c",
    "src/BTrees/TreeSetTemplate.c",
    "src/BTrees/sorters.c",
    "src/persistent/cPersistence.h",
    ]

_flavors = {"O": "object", "I": "int", "F": "float", 'L': 'int'}

KEY_H = "src/BTrees/%skeymacros.h"
VALUE_H = "src/BTrees/%svaluemacros.h"

def BTreeExtension(flavor):
    key = flavor[0]
    value = flavor[1]
    name = "BTrees._%sBTree" % flavor
    sources = ["src/BTrees/_%sBTree.c" % flavor]
    kwargs = {"include_dirs": include}
    if flavor != "fs":
        kwargs["depends"] = (base_btrees_depends + [KEY_H % _flavors[key],
                                                    VALUE_H % _flavors[value]])
    else:
        kwargs["depends"] = base_btrees_depends
    if key != "O":
        kwargs["define_macros"] = [('EXCLUDE_INTSET_SUPPORT', None)]
    return Extension(name, sources, **kwargs)

exts = [BTreeExtension(flavor)
        for flavor in ("OO", "IO", "OI", "II", "IF",
                       "fs", "LO", "OL", "LL", "LF",
                       )]
exts = []

cPersistence = Extension(name = 'persistent.cPersistence',
                         include_dirs = include,
                         sources= ['src/persistent/cPersistence.c',
                                   'src/persistent/ring.c'],
                         depends = ['src/persistent/cPersistence.h',
                                    'src/persistent/ring.h',
                                    'src/persistent/ring.c']
                         )

cPickleCache = Extension(name = 'persistent.cPickleCache',
                         include_dirs = include,
                         sources= ['src/persistent/cPickleCache.c',
                                   'src/persistent/ring.c'],
                         depends = ['src/persistent/cPersistence.h',
                                    'src/persistent/ring.h',
                                    'src/persistent/ring.c']
                         )

TimeStamp = Extension(name = 'persistent.TimeStamp',
                      include_dirs = include,
                      sources= ['src/persistent/TimeStamp.c']
                      )


exts += [cPersistence,
         cPickleCache,
         TimeStamp,
        ]

def _modname(path, base, name=''):
    if path == base:
        return name
    dirname, basename = os.path.split(path)
    return _modname(dirname, base, basename + '.' + name)

def alltests():
    import logging
    import pkg_resources
    import unittest
    import ZEO.ClientStorage

    class NullHandler(logging.Handler):
        level = 50

        def emit(self, record):
            pass

    logging.getLogger().addHandler(NullHandler())

    suite = unittest.TestSuite()
    base = pkg_resources.working_set.find(
        pkg_resources.Requirement.parse('ZODB3')).location
    for dirpath, dirnames, filenames in os.walk(base):
        if os.path.basename(dirpath) == 'tests':
            for filename in filenames:
                if filename != 'testZEO.py': continue
                if filename.endswith('.py') and filename.startswith('test'):
                    mod = __import__(
                        _modname(dirpath, base, os.path.splitext(filename)[0]),
                        {}, {}, ['*'])
                    suite.addTest(mod.test_suite())
        elif 'tests.py' in filenames:
            continue
            mod = __import__(_modname(dirpath, base, 'tests'), {}, {}, ['*'])
            suite.addTest(mod.test_suite())
    return suite

doclines = __doc__.split("\n")

def read_file(*path):
    base_dir = os.path.dirname(__file__)
    file_path = (base_dir, ) + tuple(path)
    return file(os.path.join(*file_path)).read()

long_description = str(
    ("\n".join(doclines[2:]) + "\n\n" +
     ".. contents::\n\n" +
     read_file("README.txt")  + "\n\n" +
     read_file("src", "CHANGES.txt")
    ).decode('latin-1').replace(u'L\xf6wis', '|Lowis|')
    )+ '''\n\n.. |Lowis| unicode:: L \\xf6 wis\n'''

setup(name="ZODB3",
      version=VERSION,
      maintainer="Zope Foundation and Contributors",
      maintainer_email="zodb-dev@zope.org",
      packages = find_packages('src'),
      package_dir = {'': 'src'},
      ext_modules = exts,
      headers = ['src/persistent/cPersistence.h',
                 'src/persistent/py24compat.h',
                 'src/persistent/ring.h'],
      license = "ZPL 2.1",
      platforms = ["any"],
      description = doclines[0],
      classifiers = filter(None, classifiers.split("\n")),
      long_description = long_description,
      test_suite="__main__.alltests", # to support "setup.py test"
      tests_require = ['zope.testing', 'manuel'],
      extras_require = dict(test=['zope.testing', 'manuel']),
      install_requires = [
        'transaction >=1.1.0',
        'zc.lockfile',
        'ZConfig',
        'zdaemon',
        'zope.event',
        'zope.interface',
        ],
      zip_safe = False,
      entry_points = """
      [console_scripts]
      fsdump = ZODB.FileStorage.fsdump:main
      fsoids = ZODB.scripts.fsoids:main
      fsrefs = ZODB.scripts.fsrefs:main
      fstail = ZODB.scripts.fstail:Main
      repozo = ZODB.scripts.repozo:main
      zeopack = ZEO.scripts.zeopack:main
      runzeo = ZEO.runzeo:main
      zeopasswd = ZEO.zeopasswd:main
      zeoctl = ZEO.zeoctl:main
      """,
      include_package_data = True,
      )
