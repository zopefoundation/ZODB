======================
Historical Connections
======================

.. We need to mess with time to prevent spurious test failures on windows

    >>> _now = 1231019584.0
    >>> def faux_time_time():
    ...     global _now
    ...     _now += .001 # must be less than 0.01
    ...     return _now
    >>> import time
    >>> real_time_time = time.time
    >>> real_time_sleep = time.sleep
    >>> def faux_time_sleep(amt):
    ...    global _now
    ...    _now += amt
    >>> if isinstance(time,type):
    ...    time.time = staticmethod(faux_time_time) # Jython
    ...    time.sleep = faux_time_sleep
    ... else:
    ...     time.time = faux_time_time
    ...     time.sleep = faux_time_sleep
    >>> def utcnow():
    ...     mus = (int(_now % 1 * 1000000), )
    ...     return datetime.datetime(*time.gmtime(_now)[:6] + mus)

Usage
=====

A database can be opened with a read-only, historical connection when given
a specific transaction or datetime.  This can enable full-context application
level conflict resolution, historical exploration and preparation for reverts,
or even the use of a historical database revision as "production" while
development continues on a "development" head.

A database can be opened historically ``at`` or ``before`` a given transaction
serial or datetime. Here's a simple example. It should work with any storage
that supports ``loadBefore``.

We'll begin our example with a fairly standard set up.  We

- make a storage and a database;
- open a normal connection;
- modify the database through the connection;
- commit a transaction, remembering the time in UTC;
- modify the database again; and
- commit a transaction.

    >>> import ZODB.MappingStorage
    >>> db = ZODB.MappingStorage.DB()
    >>> conn = db.open()

    >>> import persistent.mapping

    >>> conn.root()['first'] = persistent.mapping.PersistentMapping(count=0)

    >>> import transaction
    >>> transaction.commit()

We wait for some time to pass, record he time, and then make some other changes.

    >>> import time
    >>> time.sleep(.01)

    >>> import datetime
    >>> now = utcnow()
    >>> time.sleep(.01)

    >>> root = conn.root()
    >>> root['second'] = persistent.mapping.PersistentMapping()
    >>> root['first']['count'] += 1

    >>> transaction.commit()

Now we will show a historical connection. We'll open one using the ``now``
value we generated above, and then demonstrate that the state of the original
connection, at the mutable head of the database, is different than the
historical state.

    >>> transaction1 = transaction.TransactionManager()

    >>> historical_conn = db.open(transaction_manager=transaction1, at=now)

    >>> sorted(conn.root().keys())
    ['first', 'second']
    >>> conn.root()['first']['count']
    1

    >>> sorted(historical_conn.root().keys())
    ['first']
    >>> historical_conn.root()['first']['count']
    0

Moreover, the historical connection cannot commit changes.

    >>> historical_conn.root()['first']['count'] += 1
    >>> historical_conn.root()['first']['count']
    1
    >>> transaction1.commit()
    Traceback (most recent call last):
    ...
    ReadOnlyHistoryError
    >>> transaction1.abort()
    >>> historical_conn.root()['first']['count']
    0

(It is because of the mutable behavior outside of transactional semantics that
we must have a separate connection, and associated object cache, per thread,
even though the semantics should be readonly.)

As demonstrated, a timezone-naive datetime will be interpreted as UTC.  You
can also pass a timezone-aware datetime or a serial (transaction id).
Here's opening with a serial--the serial of the root at the time of the first
commit.

    >>> historical_serial = historical_conn.root()._p_serial
    >>> historical_conn.close()

    >>> historical_conn = db.open(transaction_manager=transaction1,
    ...                           at=historical_serial)
    >>> sorted(historical_conn.root().keys())
    ['first']
    >>> historical_conn.root()['first']['count']
    0
    >>> historical_conn.close()

We've shown the ``at`` argument. You can also ask to look ``before`` a datetime
or serial. (It's an error to pass both [#not_both]_) In this example, we're
looking at the database immediately prior to the most recent change to the
root.

    >>> serial = conn.root()._p_serial
    >>> historical_conn = db.open(
    ...     transaction_manager=transaction1, before=serial)
    >>> sorted(historical_conn.root().keys())
    ['first']
    >>> historical_conn.root()['first']['count']
    0

In fact, ``at`` arguments are translated into ``before`` values because the
underlying mechanism is a storage's loadBefore method.  When you look at a
connection's ``before`` attribute, it is normalized into a ``before`` serial,
no matter what you pass into ``db.open``.

    >>> print(conn.before)
    None
    >>> historical_conn.before == serial
    True

    >>> conn.close()

Configuration
=============

Like normal connections, the database lets you set how many total historical
connections can be active without generating a warning, and
how many objects should be kept in each historical connection's object cache.

    >>> db.getHistoricalPoolSize()
    3
    >>> db.setHistoricalPoolSize(4)
    >>> db.getHistoricalPoolSize()
    4

    >>> db.getHistoricalCacheSize()
    1000
    >>> db.setHistoricalCacheSize(2000)
    >>> db.getHistoricalCacheSize()
    2000

In addition, you can specify the minimum number of seconds that an unused
historical connection should be kept.

    >>> db.getHistoricalTimeout()
    300
    >>> db.setHistoricalTimeout(400)
    >>> db.getHistoricalTimeout()
    400

All three of these values can be specified in a ZConfig file.

    >>> import ZODB.config
    >>> db2 = ZODB.config.databaseFromString('''
    ...     <zodb>
    ...       <mappingstorage/>
    ...       historical-pool-size 3
    ...       historical-cache-size 1500
    ...       historical-timeout 6m
    ...     </zodb>
    ... ''')
    >>> db2.getHistoricalPoolSize()
    3
    >>> db2.getHistoricalCacheSize()
    1500
    >>> db2.getHistoricalTimeout()
    360


The pool lets us reuse connections.  To see this, we'll open some
connections, close them, and then open them again:

    >>> conns1 = [db2.open(before=serial) for i in range(4)]
    >>> _ = [c.close() for c in conns1]
    >>> conns2 = [db2.open(before=serial) for i in range(4)]

Now let's look at what we got.  The first connection in conns 2 is the
last connection in conns1, because it was the last connection closed.

    >>> conns2[0] is conns1[-1]
    True

Also for the next two:

    >>> (conns2[1] is conns1[-2]), (conns2[2] is conns1[-3])
    (True, True)

But not for the last:

    >>> conns2[3] is conns1[-4]
    False

Because the pool size was set to 3.

Connections are also discarded if they haven't been used in a while.
To see this, let's close two of the connections:

    >>> conns2[0].close(); conns2[1].close()

We'l also set the historical timeout to be very low:

    >>> db2.setHistoricalTimeout(.01)
    >>> time.sleep(.1)
    >>> conns2[2].close(); conns2[3].close()

Now, when we open 4 connections:

    >>> conns1 = [db2.open(before=serial) for i in range(4)]

We'll see that only the last 2 connections from conn2 are in the
result:

    >>> [c in conns1 for c in conns2]
    [False, False, True, True]


If you change the historical cache size, that changes the size of the
persistent cache on our connection.

    >>> historical_conn._cache.cache_size
    2000
    >>> db.setHistoricalCacheSize(1500)
    >>> historical_conn._cache.cache_size
    1500

Invalidations
=============

Invalidations are ignored for historical connections. This is another white box
test.

    >>> historical_conn = db.open(
    ...     transaction_manager=transaction1, at=serial)
    >>> conn = db.open()
    >>> sorted(conn.root().keys())
    ['first', 'second']
    >>> conn.root()['first']['count']
    1
    >>> sorted(historical_conn.root().keys())
    ['first', 'second']
    >>> historical_conn.root()['first']['count']
    1
    >>> conn.root()['first']['count'] += 1
    >>> conn.root()['third'] = persistent.mapping.PersistentMapping()
    >>> transaction.commit()
    >>> historical_conn.close()

Note that if you try to open an historical connection to a time in the future,
you will get an error.

    >>> historical_conn = db.open(
    ...     at=utcnow()+datetime.timedelta(1))
    Traceback (most recent call last):
    ...
    ValueError: cannot open an historical connection in the future.

Warnings
========

First, if you use datetimes to get a historical connection, be aware that the
conversion from datetime to transaction id has some pitfalls. Generally, the
transaction ids in the database are only as time-accurate as the system clock
was when the transaction id was created. Moreover, leap seconds are handled
somewhat naively in the ZODB (largely because they are handled naively in Unix/
POSIX time) so any minute that contains a leap second may contain serials that
are a bit off. This is not generally a problem for the ZODB, because serials
are guaranteed to increase, but it does highlight the fact that serials are not
guaranteed to be accurately connected to time. Generally, they are about as
reliable as time.time.

Second, historical connections currently introduce potentially wide variance in
memory requirements for the applications. Since you can open up many
connections to different serials, and each gets their own pool, you may collect
quite a few connections. For now, at least, if you use this feature you need to
be particularly careful of your memory usage. Get rid of pools when you know
you can, and reuse the exact same values for ``at`` or ``before`` when
possible. If historical connections are used for conflict resolution, these
connections will probably be temporary--not saved in a pool--so that the extra
memory usage would also be brief and unlikely to overlap.


.. cleanup

    >>> db.close()
    >>> db2.close()

.. restore time

    >>> time.time = real_time_time
    >>> time.sleep = real_time_sleep

.. ......... ..
.. Footnotes ..
.. ......... ..

.. [#not_both] It is an error to try and pass both `at` and `before`.

    >>> historical_conn = db.open(
    ...     transaction_manager=transaction1, at=now, before=historical_serial)
    Traceback (most recent call last):
    ...
    ValueError: can only pass zero or one of `at` and `before`
