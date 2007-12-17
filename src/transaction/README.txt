============
Transactions
============

This package contains a generic transaction implementation for Python. It is
mainly used by the ZODB, though.

Note that the data manager API, ``transaction.interfaces.IDataManager``,
is syntactically simple, but semantically complex.  The semantics
were not easy to express in the interface. This could probably use
more work.  The semantics are presented in detail through examples of
a sample data manager in ``transaction.tests.test_SampleDataManager``.

