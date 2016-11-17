====
ZODB
====

Introduction
============

The ZODB  package provides a  set of tools  for using the  Zope Object
Database (ZODB).

Our primary development platforms are Linux and Mac OS X.  The test
suite should pass without error on these platforms and, hopefully,
Windows, although it can take a long time on Windows -- longer if you
use ZoneAlarm.

Compatibility
=============

ZODB 5 requires Python 2.7 (>= 2.7.9) or Python >= 3.3.

Documentation
=============

See http://zodb-docs.readthedocs.io/en/latest/

For developers of ZODB
======================

Building
---------

Bootstrap buildout, if necessary using ``bootstrap.py``::

  python bootstrap.py

Run the buildout::

  bin/buildout

Testing
-------

The ZODB checkouts are `buildouts <http://www.python.org/pypi/zc.buildout>`_.
When working from a ZODB checkout, first run the bootstrap.py script
to initialize the buildout:

    % python bootstrap.py

and then use the buildout script to build ZODB and gather the dependencies:

    % bin/buildout

This creates a test script:

    % bin/test -v

This command will run all the tests, printing a single dot for each
test.  When it finishes, it will print a test summary.  The exact
number of tests can vary depending on platform and available
third-party libraries.::

    Ran 1182 tests in 241.269s

    OK

The test script has many more options.  Use the ``-h`` or ``--help``
options to see a file list of options.  The default test suite omits
several tests that depend on third-party software or that take a long
time to run.  To run all the available tests use the ``--all`` option.
Running all the tests takes much longer.::

    Ran 1561 tests in 1461.557s

    OK

Generating docs
---------------

cd to the doc directory and::

  make html

Contributing
------------

Almost any code change should include tests.

Any change that changes features should include documentation updates.

Maintenance scripts
-------------------

Several scripts are provided with the ZODB and can help for analyzing,
debugging, checking for consistency, summarizing content, reporting space used
by objects, doing backups, artificial load testing, etc.
Look at the ZODB/script directory for more informations.

License
=======

ZODB is distributed under the Zope Public License, an OSI-approved
open source license.  Please see the LICENSE.txt file for terms and
conditions.

More information
================

See http://zodb.org/


.. image:: https://badges.gitter.im/zopefoundation/ZODB.svg
   :alt: Join the chat at https://gitter.im/zopefoundation/ZODB
   :target: https://gitter.im/zopefoundation/ZODB?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge
