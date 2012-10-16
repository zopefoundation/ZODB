This directory contains a collection of utilities for managing ZODB
databases.  Some are more useful than others.  If you install ZODB
using distutils ("python setup.py install"), a few of these will be installed.

Unless otherwise noted, these scripts are invoked with the name of the
Data.fs file as their only argument.  Example: checkbtrees.py data.fs.


analyze.py -- a transaction analyzer for FileStorage

Reports on the data in a FileStorage.  The report is organized by
class.  It shows total data, as well as separate reports for current
and historical revisions of objects.


checkbtrees.py -- checks BTrees in a FileStorage for corruption

Attempts to find all the BTrees contained in a Data.fs, calls their
_check() methods, and runs them through BTrees.check.check().


fsdump.py -- summarize FileStorage contents, one line per revision

Prints a report of FileStorage contents, with one line for each
transaction and one line for each data record in that transaction.
Includes time stamps, file positions, and class names.


fsoids.py -- trace all uses of specified oids in a FileStorage

For heavy debugging.
A set of oids is specified by text file listing and/or command line.
A report is generated showing all uses of these oids in the database:
all new-revision creation/modifications, all references from all
revisions of other objects, and all creation undos.


fstest.py -- simple consistency checker for FileStorage

usage: fstest.py [-v] data.fs

The fstest tool will scan all the data in a FileStorage and report an
error if it finds any corrupt transaction data.  The tool will print a
message when the first error is detected an exit.

The tool accepts one or more -v arguments.  If a single -v is used, it
will print a line of text for each transaction record it encounters.
If two -v arguments are used, it will also print a line of text for
each object.  The objects for a transaction will be printed before the
transaction itself.

Note: It does not check the consistency of the object pickles.  It is
possible for the damage to occur only in the part of the file that
stores object pickles.  Those errors will go undetected.


space.py -- report space used by objects in a FileStorage

usage: space.py [-v] data.fs

This ignores revisions and versions.


netspace.py -- hackish attempt to report on size of objects

usage: netspace.py [-P | -v] data.fs

-P: do a pack first
-v: print info for all objects, even if a traversal path isn't found

Traverses objects from the database root and attempts to calculate
size of object, including all reachable subobjects.


repozo.py -- incremental backup utility for FileStorage

Run the script with the -h option to see usage details.


timeout.py -- script to test transaction timeout

usage: timeout.py address delay [storage-name]

This script connects to a storage, begins a transaction, calls store()
and tpc_vote(), and then sleeps forever.  This should trigger the
transaction timeout feature of the server.

zodbload.py -- exercise ZODB under a heavy synthesized Zope-like load

See the module docstring for details.  Note that this script requires
Zope.  New in ZODB3 3.1.4.


fsrefs.py -- check FileStorage for dangling references


fstail.py -- display the most recent transactions in a FileStorage

usage:  fstail.py [-n nxtn] data.fs

The most recent ntxn transactions are displayed, to stdout.
Optional argument -n specifies ntxn, and defaults to 10.


migrate.py -- do a storage migration and gather statistics

See the module docstring for details.
