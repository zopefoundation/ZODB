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

ZODB 4.0 requires Python 2.6 or 2.7.

Prerequisites
=============

You must have Python installed. If you're using a system Python
install, make sure development support is installed too.

You also need the transaction, BTrees, persistent, zc.lockfile,
ZConfig, zdaemon, zope.event, zope.interface, zope.proxy and
zope.testing packages.  If you don't have them and you can connect to
the Python Package Index, then these will be installed for you if you
don't have them.

Installation
============

ZODB is released as a distutils package.  The easiest ways to build
and install it are to use `easy_install
<http://peak.telecommunity.com/DevCenter/EasyInstall>`_, or
`zc.buildout <http://www.python.org/pypi/zc.buildout>`_.

To install by hand, first install the dependencies, ZConfig, zdaemon,
zope.interface, zope.proxy and zope.testing.  These can be found
in the `Python Package Index <http://www.python.org/pypi>`_.

To run the tests, use the test setup command::

  python setup.py test

It will download dependencies if needed.  If this happens, ou may get
an import error when the test command gets to looking for tests.  Try
running the test command a second time and you should see the tests
run.

::

  python setup.py test

To install, use the install command::

  python setup.py install


Testing for Developers
======================

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

There is a Mailman mailing list in place to discuss all issues related
to ZODB.  You can send questions to

    zodb-dev@zope.org

or subscribe at

    http://lists.zope.org/mailman/listinfo/zodb-dev

and view its archives at

    http://lists.zope.org/pipermail/zodb-dev

Note that Zope Corp mailing lists have a subscriber-only posting policy.

Bugs and Patches
================

Bug reports and patches should be added to the Launchpad:

    https://launchpad.net/zodb
