======================
For developers of ZODB
======================

Building
========

Bootstrap buildout, if necessary using ``bootstrap.py``::

  python bootstrap.py

Run the buildout::

  bin/buildout

Testing
=======

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

Our primary development platforms are Linux and Mac OS X.  The test
suite should pass without error on these platforms and, hopefully,
Windows, although it can take a long time on Windows -- longer if you
use ZoneAlarm.

Generating docs
===============

cd to the doc directory and::

  make html

Contributing
============

Almost any code change should include tests.

Any change that changes features should include documentation updates.
