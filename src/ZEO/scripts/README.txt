This directory contains a collection of utilities for working with
ZEO.  Some are more useful than others.  If you install ZODB using
distutils ("python setup.py install"), some of these will be
installed.

Unless otherwise noted, these scripts are invoked with the name of the
Data.fs file as their only argument.  Example: checkbtrees.py data.fs.


parsezeolog.py -- parse BLATHER logs from ZEO server

This script may be obsolete.  It has not been tested against the
current log output of the ZEO server.

Reports on the time and size of transactions committed by a ZEO
server, by inspecting log messages at BLATHER level.



timeout.py -- script to test transaction timeout

usage: timeout.py address delay [storage-name]

This script connects to a storage, begins a transaction, calls store()
and tpc_vote(), and then sleeps forever.  This should trigger the
transaction timeout feature of the server.


zeopack.py -- pack a ZEO server

The script connects to a server and calls pack() on a specific
storage.  See the script for usage details.


zeoreplay.py -- experimental script to replay transactions from a ZEO log

Like parsezeolog.py, this may be obsolete because it was written
against an earlier version of the ZEO server.  See the script for
usage details.


zeoup.py

usage: zeoup.py [options]

The test will connect to a ZEO server, load the root object, and
attempt to update the zeoup counter in the root.  It will report
success if it updates to counter or if it gets a ConflictError.  A
ConflictError is considered a success, because the client was able to
start a transaction.

See the script for details about the options.



zeoserverlog.py -- analyze ZEO server log for performance statistics

See the module docstring for details; there are a large number of
options.  New in ZODB3 3.1.4.


zeoqueue.py -- report number of clients currently waiting in the ZEO queue

See the module docstring for details.
