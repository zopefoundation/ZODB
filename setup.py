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
from setuptools import find_packages
from setuptools import setup


def read(path):
    with open(path) as f:
        return f.read()


long_description = read("README.rst") + "\n\n" + read("CHANGES.rst")


setup(
    name="ZODB",
    version='6.0.1',
    author="Jim Fulton",
    author_email="jim@zope.com",
    maintainer="Zope Foundation and Contributors",
    maintainer_email="zodb-dev@zope.dev",
    keywords="database nosql python zope",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    url='http://zodb-docs.readthedocs.io',
    license="ZPL 2.1",
    platforms=["any"],
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Zope Public License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: Unix",
        "Framework :: ZODB",
    ],
    description=long_description.split('\n', 2)[1],
    long_description=long_description,
    long_description_content_type='text/x-rst',
    extras_require={
        'test': [
            'manuel',
            'zope.testing',
            'zope.testrunner >= 4.4.6',
        ],
        'docs': [
            'Sphinx < 7',
            'ZODB',
            'j1m.sphinxautozconfig',
            'sphinx_rtd_theme',
            'sphinxcontrib_zopeext',
        ],
    },
    install_requires=[
        'persistent >= 4.4.0',
        'BTrees >= 4.2.0',
        'ZConfig',
        'transaction >= 2.4',
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
    python_requires='>=3.7',
)
