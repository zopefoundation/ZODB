##############################################################################
#
# Copyright (c) 2002, 2003 Zope Corporation and Contributors.
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

# The (non-obvious!) choices for the Trove Development Status line:
# Development Status :: 5 - Production/Stable
# Development Status :: 4 - Beta
# Development Status :: 3 - Alpha

classifiers = """\
Development Status :: 3 - Alpha
Intended Audience :: Developers
License :: OSI Approved :: Zope Public License
Programming Language :: Python
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Microsoft :: Windows
Operating System :: Unix
"""

import glob
import os
import sys
from distutils.extension import Extension
from distutils import dir_util
from distutils.dist import Distribution
from distutils.command.install_lib import install_lib
from distutils.command.build_py import build_py
from distutils.util import convert_path

if sys.version_info < (2, 4, 3):
    print "This version of ZODB requires Python 2.4.3 or higher"
    sys.exit(0)

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

_flavors = {"O": "object", "I": "int", "F": "float"}

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
    if key != "O":
        kwargs["define_macros"] = [('EXCLUDE_INTSET_SUPPORT', None)]
    return Extension(name, sources, **kwargs)

exts = [BTreeExtension(flavor)
        for flavor in ("OO", "IO", "OI", "II", "IF", "fs")]

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

winlock = Extension(name = 'ZODB.winlock',
                    include_dirs = include,
                    sources = ['src/ZODB/winlock.c']
                    )

exts += [cPersistence,
         cPickleCache,
         TimeStamp,
         winlock,
        ]

# The ZODB.zodb4 code is not being packaged, because it is only
# need to convert early versions of Zope3 databases to ZODB3.

packages = ["BTrees", "BTrees.tests",
            "ZEO", "ZEO.auth", "ZEO.zrpc", "ZEO.tests",
            "ZODB", "ZODB.FileStorage", "ZODB.tests",
            "persistent", "persistent.tests",
            "transaction", "transaction.tests",
            "ThreadedAsync",
            "ZopeUndo", "ZopeUndo.tests",
            ]

scripts = ["src/scripts/fsdump.py",
           "src/scripts/fsoids.py",
           "src/scripts/fsrefs.py",
           "src/scripts/fstail.py",
           "src/scripts/fstest.py",
           "src/scripts/repozo.py",
           "src/scripts/zeopack.py",
           "src/scripts/runzeo.py",
           "src/scripts/zeopasswd.py",
           "src/scripts/mkzeoinst.py",
           "src/scripts/zeoctl.py",
           ]

def copy_other_files(cmd, outputbase):
    # A delicate dance to copy files with certain extensions
    # into a package just like .py files.
    extensions = ["*.conf", "*.xml", "*.txt", "*.sh"]
    directories = [
        "transaction",
        "transaction/tests",
        "persistent/tests",
        "ZEO",
        "ZODB",
        "ZODB/tests",
        ]
    for dir in directories:
        exts = extensions
        dir = convert_path(dir)
        inputdir = os.path.join("src", dir)
        outputdir = os.path.join(outputbase, dir)
        if not os.path.exists(outputdir):
            dir_util.mkpath(outputdir)
        for pattern in exts:
            for fn in glob.glob(os.path.join(inputdir, pattern)):
                # glob is going to give us a path including "src",
                # which must be stripped to get the destination dir
                dest = os.path.join(outputbase, fn[4:])
                cmd.copy_file(fn, dest)

class MyLibInstaller(install_lib):
    """Custom library installer, used to put hosttab in the right place."""

    # We use the install_lib command since we need to put hosttab
    # inside the library directory.  This is where we already have the
    # real information about where to install it after the library
    # location has been set by any relevant distutils command line
    # options.

    def run(self):
        install_lib.run(self)
        copy_other_files(self, self.install_dir)

class MyPyBuilder(build_py):
    def build_packages(self):
        build_py.build_packages(self)
        copy_other_files(self, self.build_lib)

class MyDistribution(Distribution):
    # To control the selection of MyLibInstaller and MyPyBuilder, we
    # have to set it into the cmdclass instance variable, set in
    # Distribution.__init__().

    def __init__(self, *attrs):
        Distribution.__init__(self, *attrs)
        self.cmdclass['build_py'] = MyPyBuilder
        self.cmdclass['install_lib'] = MyLibInstaller

doclines = __doc__.split("\n")

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
    extra = {}
else:
    extra = dict(
        install_requires = [
            'zope.interface',
            'zope.proxy',
            'zope.testing',
            'ZConfig',
            'zdaemon',
            ],
        zip_safe = False,
        dependency_links = ['http://download.zope.org/distribution/'],
        )
    scripts = []
    

setup(name="ZODB3",
      version="3.7.0a2",
      maintainer="Zope Corporation",
      maintainer_email="zodb-dev@zope.org",
      url = "http://www.zope.org/Wikis/ZODB",
      download_url = "http://www.zope.org/Products/ZODB3.5",
      packages = packages,
      package_dir = {'': 'src'},
      ext_modules = exts,
      headers = ['src/persistent/cPersistence.h',
                 'src/persistent/ring.h'],
      license = "ZPL 2.1",
      platforms = ["any"],
      description = doclines[0],
      classifiers = filter(None, classifiers.split("\n")),
      long_description = "\n".join(doclines[2:]),
      distclass = MyDistribution,
      scripts = scripts,
      **extra)
