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
from setuptools import setup, find_packages

version = '5.5.1'

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
Programming Language :: Python :: 3.7
Programming Language :: Python :: Implementation :: CPython
Programming Language :: Python :: Implementation :: PyPy
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Microsoft :: Windows
Operating System :: Unix
Framework :: ZODB
"""

def read(path):
    with open(path) as f:
        return f.read()

long_description = read("README.rst") + "\n\n" + read("CHANGES.rst")

tests_require = [
    'manuel',
    'mock; python_version == "2.7"',
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
    tests_require=tests_require,
    extras_require={
        'test': tests_require,
    },
    install_requires=[
        'persistent >= 4.4.0',
        'BTrees >= 4.2.0',
        'ZConfig',
        'transaction >= 2.4',
        'six',
        'zc.lockfile',
        'zope.interface',
        'zodbpickle >= 1.0.1',
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
