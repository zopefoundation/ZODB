=====================
ZODB Utilities Module
=====================

The ZODB.utils module provides a number of helpful, somewhat random
:), utility functions.

    >>> import ZODB.utils

This document documents a few of them. Over time, it may document
more.

64-bit integers and strings
===========================

ZODB uses 64-bit transaction ids that are typically represented as
strings, but are sometimes manipulated as integers.  Object ids are
strings too and it is common to use 64-bit strings that are just
packed integers.

Functions p64 and u64 pack and unpack integers as strings:

    >>> ZODB.utils.p64(250347764455111456)
    '\x03yi\xf7"\xa8\xfb '

    >>> print(ZODB.utils.u64(b'\x03yi\xf7"\xa8\xfb '))
    250347764455111456

The constant z64 has zero packed as a 64-bit string:

    >>> ZODB.utils.z64
    '\x00\x00\x00\x00\x00\x00\x00\x00'

Transaction id generation
=========================

Storages assign transaction ids as transactions are committed.  These
are based on UTC time, but must be strictly increasing.  The
newTid function makes this pretty easy.

To see this work (in a predictable way), we'll first hack time.time:

    >>> import time
    >>> old_time = time.time
    >>> time_value = 1224825068.12
    >>> faux_time = lambda: time_value
    >>> if isinstance(time,type):
    ...    time.time = staticmethod(faux_time) # Jython
    ... else:
    ...     time.time = faux_time

Now, if we ask for a new time stamp, we'll get one based on our faux
time:

    >>> tid = ZODB.utils.newTid(None)
    >>> tid
    '\x03yi\xf7"\xa54\x88'

newTid requires an old tid as an argument. The old tid may be None, if
we don't have a previous transaction id.

This time is based on the current time, which we can see by converting
it to a time stamp.

    >>> import ZODB.TimeStamp
    >>> print(ZODB.TimeStamp.TimeStamp(tid))
    2008-10-24 05:11:08.120000

To assure that we get a new tid that is later than the old, we can
pass an existing tid.  Let's pass the tid we just got.

    >>> tid2 = ZODB.utils.newTid(tid)
    >>> ZODB.utils.u64(tid), ZODB.utils.u64(tid2)
    (250347764454864008, 250347764454864009)

Here, since we called it at the same time, we got a time stamp that
was only slightly larger than the previos one.  Of course, at a later
time, the time stamp we get will be based on the time:

    >>> time_value = 1224825069.12
    >>> tid = ZODB.utils.newTid(tid2)
    >>> print(ZODB.TimeStamp.TimeStamp(tid))
    2008-10-24 05:11:09.120000


    >>> time.time = old_time


Locking support
===============

Storages are required to be thread safe.  The locking descriptor helps
automate that. It arranges for a lock to be acquired when a function
is called and released when a function exits.  To demonstrate this,
we'll create a "lock" type that simply prints when it is called:

    >>> class Lock:
    ...     def acquire(self):
    ...         print('acquire')
    ...     def release(self):
    ...         print('release')
    ...     def __enter__(self):
    ...         return self.acquire()
    ...     def __exit__(self, *ignored):
    ...         return self.release()

Now we'll demonstrate the descriptor:

    >>> class C:
    ...     _lock = Lock()
    ...     _lock_acquire = _lock.acquire
    ...     _lock_release = _lock.release
    ...
    ...     @ZODB.utils.locked
    ...     def meth(self, *args, **kw):
    ...         print('meth %r %r' %(args, kw))

The descriptor expects the instance it wraps to have a '_lock
attribute.

    >>> C().meth(1, 2, a=3)
    acquire
    meth (1, 2) {'a': 3}
    release

.. Edge cases

   We can get the method from the class:

    >>> C.meth # doctest: +ELLIPSIS
    <ZODB.utils.Locked object at ...>

    >>> C.meth(C())
    acquire
    meth () {}
    release

    >>> class C2:
    ...     _lock = Lock()
    ...     _lock_acquire = _lock.acquire
    ...     _lock_release = _lock.release

    # XXX: Py3: Pytohn 3 does not have the concept of an unbound method.
    #>>> C.meth(C2()) # doctest: +NORMALIZE_WHITESPACE
    #Traceback (most recent call last):
    #...
    #TypeError: unbound method meth() must be called with C instance
    #as first argument (got C2 instance instead)

Preconditions
=============

Often, we want to supply method preconditions. The locking descriptor
supports optional method preconditions [1]_.

    >>> class C:
    ...     def __init__(self):
    ...         self._lock = Lock()
    ...         self._opened = True
    ...         self._transaction = None
    ...
    ...     def opened(self):
    ...         """The object is open
    ...         """
    ...         print('checking if open')
    ...         return self._opened
    ...
    ...     def not_in_transaction(self):
    ...         """The object is not in a transaction
    ...         """
    ...         print('checking if in a transaction')
    ...         return self._transaction is None
    ...
    ...     @ZODB.utils.locked(opened, not_in_transaction)
    ...     def meth(self, *args, **kw):
    ...         print('meth %r %r' % (args, kw))

    >>> c = C()
    >>> c.meth(1, 2, a=3)
    acquire
    checking if open
    checking if in a transaction
    meth (1, 2) {'a': 3}
    release

    >>> c._transaction = 1
    >>> c.meth(1, 2, a=3) # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    AssertionError:
    ('Failed precondition: ', 'The object is not in a transaction')

    >>> c._opened = False
    >>> c.meth(1, 2, a=3) # doctest: +NORMALIZE_WHITESPACE
    Traceback (most recent call last):
    ...
    AssertionError: ('Failed precondition: ', 'The object is open')


.. [1] Arguably, preconditions should be handled via separate
   descriptors, but for ZODB storages, almost all methods need to be
   locked.  Combining preconditions with locking provides both
   efficiency and concise expressions.  A more general-purpose
   facility would almost certainly provide separate descriptors for
   preconditions.
