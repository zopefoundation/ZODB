ZODB3 3.3 alpha 3
=================

Introduction
------------

The ZODB3 package provides a set of tools for using the Zope Object
Database (ZODB) in Python programs separately from Zope.  The tools
you get are identical to the ones provided in Zope, because they come
from the same source repository.  They have been packaged for use in
non-Zope stand-alone Python applications.

The components you get with the ZODB3 release are as follows:

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

ZODB 3.3 is known to work with Python 2.3.3.  For best results, we
recommend using Python 2.3.3.  Note that Python 2.2 and earlier are
not supported.

The Zope 2.8 release should be compatible with this version of ZODB.
Note that Zope 2.7 and higher includes ZEO, so this package should
only be needed to run a ZEO server.

The ZEO server in ZODB 3.3 is currently incompatible with earlier
versions of ZODB.  If you want to test the software, you must be
running this release for both client and server.  A backwards
compatibility mechanism will be provide in the beta release.

Prerequisites
-------------

You must have Python installed.  If you've installed Python from RPM,
be sure that you've installed the development RPMs too, since ZODB3
builds Python extensions.  If you have the source release of ZODB3,
you will need a C compiler.

The ZConfig package requires an XML parser with SAX support.  The
pyexpat module should be sufficient; note that pyexpat requires expat
be installed.

Installation
------------

ZODB3 is released as a distutils package.  To build it, run the setup
script::

    % python setup.py build

To test the build, run the test script::

    % python test.py

For more verbose test output, append one or two '-v' arguments to this
command.

If all the tests succeeded, you can install ZODB3 using the setup
script::

    % python setup.py install

This should now make all of ZODB accessible to your Python programs.

Testing
-------

ZODB3 comes with a large test suite that can be run from the source
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

The version numbering scheme for ZODB is complicated.  Starting with
the ZODB 3.1 release, we tried to make it simpler.  Versions prior to
3.1 had different names and different numbers.  This section describes
the gory details.

Historically, ZODB was distributed as a part of the Zope application
server.  Jim Fulton's paper at the Python conference in 2000 described
a version of ZODB he called ZODB 3, based on an earlier persistent
object system called BoboPOS.  The earliest versions of ZODB 3 were
released with Zope 2.0.

Andrew Kuchling extracted ZODB from Zope 2.4.1 and packaged them for
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
release "ZODB3".

To make matters worse, we worked on a ZODB4 package for a while and
made a couple of alpha releases.  We've now abandoned that effort,
because we didn't have the resources to pursue while also maintaining
ZODB3.

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

Andrew's ZODB Programmers Guide is made available in several
forms, including DVI and HTML.  To view it online, point your
browser at the file Doc/guide/zodb/index.html


Bugs and Patches
----------------

You can submit bug reports and patches on Andrew's ZODB SourceForge
project at:

    http://sourceforge.net/tracker/?group_id=15628



..
   Local Variables:
   mode: indented-text
   indent-tabs-mode: nil
   sentence-end-double-space: t
   fill-column: 70
   End:
