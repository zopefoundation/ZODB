============
Transactions
============

Transaction support is provided by the `transaction
<http://transaction.readthedocs.io/en/latest/>`_ package, which is
installed automatically when you install ZODB.  There are 2 important
APIs provided by the transaction package, ``ITransactionManager`` and
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

