=======
ZEO 2.0
=======

What's ZEO?
-----------

ZEO stands for Zope Enterprise Objects.  ZEO is an add-on for Zope
that allows multiple processes to connect to a single ZODB storage.
Those processes can live on different machines, but don't need to.
ZEO 2 has many improvements over ZEO 1, and is incompatible with ZEO 1;
if you upgrade an existing ZEO 1 installation, you must upgrade the
server and all clients simultaneous.  If you received ZEO 2 as part of
the ZODB 3 distribution, the ZEO 1 sources are provided in a separate
directory (ZEO1).  Some documentation for ZEO is available in the ZODB 3
package in the Doc subdirectory.  ZEO depends on the ZODB software; it
can be used with the version of ZODB distributed with Zope 2.5.1 or
later.  More information about ZEO can be found in the ZODB Wiki:

    http://www.zope.org/Wikis/ZODB

What's here?
------------

This list of filenames is mostly for ZEO developers::

 ClientCache.py          client-side cache implementation
 ClientStorage.py        client-side storage implementation
 ClientStub.py           RPC stubs for callbacks from server to client
 CommitLog.py            buffer used during two-phase commit on the server
 Exceptions.py           definitions of exceptions
 ICache.py               interface definition for the client-side cache
 ServerStub.py           RPC stubs for the server
 StorageServer.py        server-side storage implementation
 TransactionBuffer.py    buffer used for transaction data in the client
 __init__.py             near-empty file to make this directory a package
 simul.py                command-line tool to simulate cache behavior
 start.py                command-line tool to start the storage server
 stats.py                command-line tool to process client cache traces
 tests/                  unit tests and other test utilities
 util.py                 utilities used by the server startup tool
 version.txt             text file indicating the ZEO version
 zrpc/                   subpackage implementing Remote Procedure Call (RPC)

