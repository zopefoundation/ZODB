This directory contains a collect of utilities for managing ZODB
databases.  Some are more useful than others.  If you install ZODB
using distutils ("python setup.py install"), fsdump.py, fstest.py,
repozo.py, and zeopack.py will be installed in /usr/local/bin.

Unless otherwise noted, these scripts are invoked with the name of the
Data.fs file as their only argument.  Example: checkbtrees.py data.fs.


analyze.py -- A transaction analyzer for FileStorage

Reports on the data in a FileStorage.  The report is organized by
class.  It shows total data, as well as separate reports for current
and historical revisions of objects.


checkbtrees.py -- Checks BTrees in a FileStorage for corruption.

Attempts to find all the BTrees contained in a Data.fs and calls their
_check() methods.


fsdump.py -- Summarize FileStorage contents, one line per revision.

Prints a report of FileStorage contents, with one line for each
transaction and one line for each data record in that transaction.
Includes time stamps, file positions, and class names.


fstest.py -- Simple consistency checker for FileStorage

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


netspace.py -- Hackish attempt to report on size of objects

usage: netspace.py [-P | -v] data.fs

-P: do a pack first
-v: print info for all objects, even if a traversal path isn't found

Traverses objects from the database root and attempts to calculate
size of object, including all reachable subobjects.


parsezeolog.py -- Parse BLATHER logs from ZEO server.

This script may be obsolete.  It has not been tested against the
current log output of the ZEO server.

Reports on the time and size of transactions committed by a ZEO
server, by inspecting log messages at BLATHER level.


repozo.py -- Incremental backup utility for FileStorage.

Run the script with the -h option to see usage details.


timeout.py -- Script to test transaction timeout

usage: timeout.py address delay [storage-name]

This script connects to a storage, begins a transaction, calls store()
and tpc_vote(), and then sleeps forever.  This should trigger the
transaction timeout feature of the server.


zeopack.py -- Script to pack a ZEO server.

The script connects to a server and calls pack() on a specific
storage.  See the script for usage details.


zeoreplay.py -- Experimental script to replay transactions from a ZEO log.

Like parsezeolog.py, this may be obsolete because it was written
against an earlier version of the ZEO server.  See the script for
usage details.


zeoup.py

Usage: zeoup.py [options]

The test will connect to a ZEO server, load the root object, and
attempt to update the zeoup counter in the root.  It will report
success if it updates to counter or if it gets a ConflictError.  A
ConflictError is considered a success, because the client was able to
start a transaction.

See the script for details about the options.


zodbload.py - exercise ZODB under a heavy synthesized Zope-like load

See the module docstring for details.  Note that this script requires
Zope.  New in ZODB3 3.1.4.


zeoserverlog.py - analyze ZEO server log for performance statistics

See the module docstring for details; there are a large number of
options.  New in ZODB3 3.1.4.