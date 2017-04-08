============
Transactions
============

Transaction support is provided by the `transaction
<http://transaction.readthedocs.io/en/latest/>`_ package
[#transaction-package-can-be-used-wo-ZODB]_, which is installed
automatically when you install ZODB.  There are two important APIs
provided by the transaction package, ``ITransactionManager`` and
``ITransaction``, described below.

ITransactionManager
===================

.. autointerface:: transaction.interfaces.ITransactionManager
   :members: begin, get, commit, abort, doom, isDoomed, savepoint

ITransaction
============

.. autointerface:: transaction.interfaces.ITransaction
   :members: user, description, commit, abort, doom, savepoint, note,
             setUser, setExtendedInfo,
             addBeforeCommitHook, getBeforeCommitHooks,
             addAfterCommitHook, getAfterCommitHooks


.. [#transaction-package-can-be-used-wo-ZODB] The :mod:transaction
   package is a general purpose package for managing `distributed
   transactions
   <https://en.wikipedia.org/wiki/Distributed_transaction>`_ with a
   `two-phase commit protocol
   <https://en.wikipedia.org/wiki/Two-phase_commit_protocol>`_.  It
   can and occasionally is used with packages other than ZODB.
