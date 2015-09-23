==========================================
ZODB - a native object database for Python
==========================================

    Don't squeeze your objects into tables: store them in an object database.

Overview
========

Python programs are written with the object-oriented paradigm. You use objects
that reference each other freely and can be of any form and shape: no object
has to adhere to a specific schema and can hold arbitrary information.

Storing those objects in relational databases requires you to give up on the
freedom of reference and schema. The constraints of the relational model
reduces your ability to write object-oriented code.

The ZODB is a native object database, that stores your objects while allowing
you to work with any paradigms that can be expressed in Python. Thereby your
code becomes simpler, more robust and easier to understand.

Also, there is no gap between the database and your program: no glue code to
write, no mappings to configure. Have a look at the tutorial to see how easy
it is.

Some of the features that ZODB brings to you:

* Transparent persistence for Python objects
* Full ACID-compatible transaction support (including savepoints)
* History/undo ability
* Efficient support for binary large objects (BLOBs)
* Pluggable storages
* Scalable architecture


Documentation
=============

.. toctree::
   :maxdepth: 1

   documentation/tutorial
   documentation/guide/index
   documentation/articles/index

* `The ZODB Book (in progress) <http://zodb.readthedocs.org/en/latest/>`_ 

Downloads
=========

ZODB is distributed through the `Python Package Index
<http://pypi.python.org/pypi/ZODB>`_.

You can install the ZODB using pip command::

    $ pip install ZODB

Community and contributing
==========================

Discussion occurs on the `ZODB mailing list
<https://groups.google.com/forum/#!forum/zodb>`_. (And for the
transaction system on the `transaction list
<https://groups.google.com/forum/#!forum/python-transaction>`_

Bug reporting and feature requests are submitted through github issue
trackers for various ZODB components:

- `ZODB <https://github.com/zopefoundation/zodb>`_

- `persistent <https://github.com/zopefoundation/persistent>`_

- `transactuon <https://github.com/zopefoundation/transaction>`_

- `BTrees <https://github.com/zopefoundation/BTrees>`_

- `ZEO (client-server framework) <https://github.com/zopefoundation/ZEO>`_

If you'd like to contribute then we'll gladly accept work on documentation,
helping out other developers and users at the mailing list, submitting bugs,
creating proposals and writing code.

ZODB is a project managed by the Zope Foundation so you can get write access
for contributing directly - check out the foundation's `Zope Developer Information <http://docs.zope.org/developer>`_.
