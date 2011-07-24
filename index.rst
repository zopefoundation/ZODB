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
write, no mappings to configure. Have a look at the tutorial to see, how easy
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
   bugs
   features

* `The ZODB Book (in progress) <http://www.zodb.org/zodbbook>`_

Downloads
=========

ZODB is distributed as Python eggs through the `Python Package Index <http://pypi.python.org/pypi/ZODB3>`_.

You can install the egg using setuptools' easy_install command::

    $ easy_install ZODB3

`Other downloads <downloads.html>`_ are available for old (non-egg) releases
and via Subversion.

Community and contributing
==========================

Discussion occurs on the `ZODB developers' mailing list <http://mail.zope.org/mailman/listinfo/zodb-dev>`_.

:doc:`Bug reporting<bugs>`, :doc:`feature requests<features>`, and release planning are done on `Launchpad <http://launchpad.net/zodb>`_.

If you'd like to contribute then we'll gladly accept work on documentation,
helping out other developers and users at the mailing list, submitting bugs,
creating proposals and writing code.

ZODB is a project managed by the Zope Foundation so you can get write access
for contributing directly - check out the foundation's `Zope Developer Information <http://docs.zope.org/developer>`_.
