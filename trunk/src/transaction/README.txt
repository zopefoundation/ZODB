This package is currently a facade of the ZODB.Transaction module.

It exists to support:

- Application code that uses the ZODB 4 transaction API

- ZODB4-style data managers (transaction.interfaces.IDataManager)

Note that the data manager API, transaction.interfaces.IDataManager,
is syntactically simple, but semantically complex.  The semantics
were not easy to express in the interface. This could probably use
more work.  The semantics are presented in detail through examples of
a sample data manager in transaction.tests.test_SampleDataManager.
