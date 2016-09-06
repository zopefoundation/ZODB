===========================
Installing and running ZODB
===========================

This topic discusses some boring nitty-gritty details needed to
actually run ZODB.

Installation
============

Installation of ZODB is pretty straightforward using Python's
packaging system. For example, using pip::

  pip install ZODB

You may need additional optional packages, such as `ZEO
<https://pypi.python.org/pypi/ZEO>`_ or `RelStorage
<https://pypi.python.org/pypi/RelStorage>`_, depending your deployment
choices.

Configuration
=============

You can set up ZODB in your application using either Python, or
ZODB's configuration language.  For simple database setup, and
especially for exploration, the Python APIs are sufficient.

For more complex configurations, you'll probably find ZODB's
configuration language easier to use.

To understand database setup, it's important to understand ZODB's
architecture.  In particular, ZODB separates database functionality
from low-level storage concerns. When you create a database object,
you specify a storage object for it to use, as in::

    import ZODB, ZODB.FileStorage

    storage = ZODB.FileStorage.FileStorage('mydata.fs')
    db = ZODB.DB(storage)

So when you define a database, you'll also define a storage. In the
example above, we define a :class:`file storage
<ZODB.FileStorage.FileStorage.FileStorage>` and then use it to define
a database.

Sometimes, storages are created through composition.  For example, if
we want to save space, we could layer a ``ZlibStorage``
[#zlibstorage_]_ over the file storage::

    import ZODB, ZODB.FileStorage, zc.zlibstorage

    storage = ZODB.FileStorage.FileStorage('mydata.fs')
    compressed_storage = zc.zlibstorage.ZlibStorage(storage)
    db = ZODB.DB(compressed_storage)

`ZlibStorage <https://pypi.python.org/pypi/zc.zlibstorage>`_
compresses database records using the compression algorithm used by
`gzip <http://www.gzip.org/>`_.

Python configuration
--------------------

To set up a database with Python, you'll construct a storage using the
ref:`storage APIs <included-storages-label>`, and then pass the
storage to the class:`~ZODB.DB` class to create a database, as shown
in the examples in the previous section.

The class:`~ZODB.DB` class also accepts a string path name as it's
storage argument to automatically create a file storage.  You can also
pass ``None`` as the storage to automatically use a
:class:`~ZODB.MappingStorage.MappingStorage`, which is convenient when
exploring ZODB::

  db = ZODB.DB(None) # Create an in-memory database.


.. [#zlibstorage_] `zc.zlibstorage
   <https://pypi.python.org/pypi/zc.zlibstorage>`_ is an optional
   package that you need to install separately.
