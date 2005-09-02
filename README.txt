ZODB 3.6
========

Introduction
------------

The ZODB package provides a set of tools for using the Zope Object
Database (ZODB) in Python programs separately from Zope.  The tools
you get are identical to the ones provided in Zope, because they come
from the same source repository.  They have been packaged for use in
non-Zope stand-alone Python applications.

The components you get with the ZODB release are as follows:

- Core ZODB, including the persistence machinery
- Standard storages such as FileStorage
- The persistent BTrees modules
- ZEO
- ZConfig -- a Zope configuration language
- documentation

Our primary development platforms are Linux and Windows 2000.  The
test suite should pass without error on all of these platforms,
although it can take a long time on Windows -- longer if you use
ZoneAlarm.  Many particularly slow tests are skipped unless you pass
--all as an argument to test.py.

Compatibility
-------------

ZODB 3.6 requires Python 2.3.4 or later.  For best results, we recommend
Python 2.3.5.  Python 2.4.1 can also be used.

The Zope 2.8 release, and Zope3 releases, should be compatible with this
version of ZODB.  Note that Zope 2.7 and higher includes ZEO, so this package
should only be needed to run a ZEO server.

ZEO servers and clients are wholly compatible among 3.3, 3.3.1, 3.4, 3.5, and
3.6; a ZEO client from any of those versions can talk with a ZEO server from
any.

Trying to mix ZEO clients and servers from 3.3 or later from ZODB releases
before 3.3 is much harder.   ZODB 3.3 introduced multiversion concurrency
control (MVCC), and earlier ZEO servers do not support MVCC:  a 3.3+ ZEO
client cannot talk with an older ZEO server as a result.

In the other direction, a 3.3+ ZEO server can talk with older ZEO clients,
but because the names of some basic classes have changed, if any 3.3+ clients
commit modifications to the database it's likely that the database will
contain instances of classes that don't exist in (can't be loaded by) older
ZEO clients.  For example, the database root object was an instance of
``ZODB.PersistentMapping.PersistentMapping`` before ZODB 3.3, but is an
instance of ``persistent.mapping.PersistentMapping`` in ZODB 3.3.  A 3.3.1+
client can still load a ``ZODB.PersistentMapping.PersistentMapping`` object,
but this is just an alias for ``persistent.mapping.PersistentMapping``, and
an object of the latter type will be stored if a 3.3 client commits a change
to the root object.  An older ZEO client cannot load the root object so
changed.

This limits migration possibilities:  a 3.3+ ZEO server can be used with
older (pre-3.3) ZEO clients and serve an older database, so long as no 3.3+
ZEO clients commit changes to the database.  The most practical upgrade path
is to bring up both servers and clients using 3.3+, not trying to mix pre-3.3
and post-3.3 ZEO clients and servers.

Prerequisites
-------------

You must have Python installed.  If you've installed Python from RPM,
be sure that you've installed the development RPMs too, since ZODB
builds Python extensions.  If you have the source release of ZODB,
you will need a C compiler.

Installation
------------

ZODB is released as a distutils package.  To build it, run the setup
script::

    % python setup.py build

To test the build, run the test script::

    % python test.py

For more verbose test output, append one or two '-v' arguments to this
command.

If all the tests succeeded, you can install ZODB using the setup
script::

    % python setup.py install

This should now make all of ZODB accessible to your Python programs.

Testing
-------

ZODB comes with a large test suite that can be run from the source
directory before ZODB is installed.  The simplest way to run the tests
is::

    % python test.py -v

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


History
-------

The historical version numbering schemes for ZODB and ZEO are complicated.
Starting with ZODB 3.4, the ZODB and ZEO version numbers are the same.

In the ZODB 3.1 through 3.3 lines, the ZEO version number was "one smaller"
than the ZODB version number; e.g., ZODB 3.2.7 included ZEO 2.2.7.  ZODB and
ZEO were distinct releases prior to ZODB 3.1, and had independent version
numbers.

Historically, ZODB was distributed as a part of the Zope application
server.  Jim Fulton's paper at the Python conference in 2000 described
a version of ZODB he called ZODB 3, based on an earlier persistent
object system called BoboPOS.  The earliest versions of ZODB 3 were
released with Zope 2.0.

Andrew Kuchling extracted ZODB from Zope 2.4.1 and packaged it for
use by standalone Python programs.  He called this version
"StandaloneZODB".  Andrew's guide to using ZODB is included in the Doc
directory.  This version of ZODB was hosted at
http://sf.net/projects/zodb.  It supported Python 1.5.2, and might
still be of interest to users of this very old Python version.

Zope Corporation released a version of ZODB called "StandaloneZODB
1.0" in Feb. 2002.  This release was based on Andrew's packaging, but
built from the same CVS repository as Zope.  It is roughly equivalent
to the ZODB in Zope 2.5.

Why not call the current release StandaloneZODB?  The name
StandaloneZODB is a bit of a mouthful.  The standalone part of the
name suggests that the Zope version is the real version and that this
is an afterthought, which isn't the case.  So we're calling this
release "ZODB".

To make matters worse, we worked on a ZODB4 package for a while and
made a couple of alpha releases.  We've now abandoned that effort,
because we didn't have the resources to pursue ot while also maintaining
ZODB(3).



License
-------

ZODB is distributed under the Zope Public License, an OSI-approved
open source license.  Please see the LICENSE.txt file for terms and
conditions.

The ZODB/ZEO Programming Guide included in the documentation is a
modified version of Andrew Kuchling's original guide, provided under
the terms of the GNU Free Documentation License.


More information
----------------

We maintain a Wiki page about all things ZODB, including status on
future directions for ZODB.  Please see

    http://www.zope.org/Wikis/ZODB

and feel free to contribute your comments.  There is a Mailman mailing
list in place to discuss all issues related to ZODB.  You can send
questions to

    zodb-dev@zope.org

or subscribe at

    http://lists.zope.org/mailman/listinfo/zodb-dev

and view its archives at

    http://lists.zope.org/pipermail/zodb-dev

Note that Zope Corp mailing lists have a subscriber-only posting policy.

Andrew's ZODB Programmers Guide is made available in several
forms, including DVI and HTML.  To view it online, point your
browser at the file Doc/guide/zodb/index.html


Bugs and Patches
----------------

Bug reports and patches should be added to the Zope Collector, with
topic "Database":

    http://collector.zope.org/Zope


..
   Local Variables:
   mode: indented-text
   indent-tabs-mode: nil
   sentence-end-double-space: t
   fill-column: 70
   End:
