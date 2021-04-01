=======================================
ZODB, a Python object-oriented database
=======================================

.. image:: https://img.shields.io/pypi/v/ZODB.svg
   :target: https://pypi.org/project/ZODB/
   :alt: Latest release

.. image:: https://img.shields.io/pypi/pyversions/ZODB.svg
   :target: https://pypi.org/project/ZODB/
   :alt: Supported Python versions

.. image:: https://travis-ci.com/zopefoundation/ZODB.svg?branch=master
   :target: https://travis-ci.com/zopefoundation/ZODB
   :alt: Build status

.. image:: https://coveralls.io/repos/github/zopefoundation/ZODB/badge.svg
   :target: https://coveralls.io/github/zopefoundation/ZODB
   :alt: Coverage status

.. image:: https://readthedocs.org/projects/zodb-docs/badge/?version=latest
   :target: https://zodb-docs.readthedocs.io/en/latest/
   :alt: Documentation status

ZODB provides an object-oriented database for Python that provides a
high-degree of transparency. ZODB runs on Python 2.7 or Python 3.4 and
above. It also runs on PyPy.

- no separate language for database operations

- very little impact on your code to make objects persistent

- no database mapper that partially hides the database.

  Using an object-relational mapping **is not** like using an
  object-oriented database.

- almost no seam between code and database.

ZODB is an ACID Transactional database.

To learn more, visit: https://zodb-docs.readthedocs.io

The github repository is at https://github.com/zopefoundation/zodb

If you're interested in contributing to ZODB itself, see the
`developer notes
<https://github.com/zopefoundation/ZODB/blob/master/DEVELOPERS.rst>`_.
