==================
ZODB documentation
==================

``zodbdocs`` is the source documentation for the website http://zodb.org. It
contains all ZODB relevant documentation like "ZODB/ZEO Programming Guide",
some ZODB articles and links to the ZODB release notes.


Building the documentation
--------------------------

All documentation is formatted as restructured text. To generate HTML using
Sphinx, use the following::

    python bootstrap.py
    ./bin/buildout
    make html
