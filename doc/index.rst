==========================================
ZODB - a native object database for Python
==========================================

Because ZODB is an object database:

- no separate language for database operations

- very little impact on your code to make objects persistent

- no database mapper that partially hides the database.

  Using an object-relational mapping **is not** like using an object database.

- almost no seam between code and database.

- Relationships between objects are handled very naturally, supporting
  complex object graphs without joins.

Check out the :doc:`tutorial`!

ZODB runs on Python 2.7 or Python 3.4 and above. It also runs on PyPy.

Learning more
=============

.. toctree::
   :maxdepth: 1

   introduction
   tutorial
   guide/index
   reference/index
   articles/index

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
<https://groups.google.com/forum/#!forum/python-transaction>`_)

Bug reporting and feature requests are submitted through github issue
trackers for various ZODB components:

- ZODB `repository <https://github.com/zopefoundation/zodb>`_

- persistent `documentation <https://persistent.readthedocs.io/en/stable/>`_ and its `repository <https://github.com/zopefoundation/persistent>`_.

- transaction `documentation <https://transaction.readthedocs.io/en/stable/>`_ and its `repository <https://github.com/zopefoundation/transaction>`_

- BTrees `documentation <https://btrees.readthedocs.io/en/stable/>`_ and their `repository <https://github.com/zopefoundation/BTrees>`_

- ZEO (client-server framework) `repository <https://github.com/zopefoundation/ZEO>`_

- relstorage `documentation <https://relstorage.readthedocs.io/en/latest/>`_ and its `repository <https://github.com/zodb/relstorage/>`_

- zodburi `documentation <https://docs.pylonsproject.org/projects/zodburi/en/latest/>`_ and its `repository <https://github.com/Pylons/zodburi>`_

- NEO `documentation <https://neo.nexedi.com/>`_ and its `repository <https://lab.nexedi.com/nexedi/neoppod/>`_

- readonlystorage `repository <https://gitlab.com/yaal/readonlystorage>`_

If you'd like to contribute then we'll gladly accept work on documentation,
helping out other developers and users at the mailing list, submitting bugs,
creating proposals and writing code.

ZODB is a project managed by the Zope Foundation so you can get write access
for contributing directly - check out the foundation's `Zope Developer Information <http://docs.zope.org/developer>`_.
