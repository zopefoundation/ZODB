ZODB Documentation
==================

Simple text files
-----------------

This is a brief summary of the text documentation included with ZODB.
Most of the text actually uses the restructured text format.  The
summary lists the title and path of each document.

BerkeleyDB Storages for ZODB
Doc/BDBStorage.txt

Using zdctl and zdrun to manage server processes
Doc/zdctl.txt

ZEO Client Cache
Doc/ZEO/cache.txt

Running a ZEO Server HOWTO
Doc/ZEO/howto.txt

ZEO Client Cache Tracing
Doc/ZEO/trace.txt

Formatted documents
-------------------

There are two documents written the Python documentation tools.

  ZODB/ZEO Programming Guide
    PDF:  zodb.pdf
    HTML: zodb/zodb.html

  ZODB Storage API
    PDF:  storage.pdf
    HTML: storage/storage.html

The documents located here can be formatted using the mkhowto script
which is part of the Python documentation tools.  The recommended use
of this script is to create a symlink from some handy bin/ directory
to the script, located in Doc/tools/mkhowto in the Python source
distribution; that script will locate the various support files it
needs appropriately.

The Makefile contains the commands to produce both the PDF and HTML
versions of the documents.
