##############################################################################
#
# Copyright (c) 2001, 2002, 2005 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Test transaction behavior for variety of cases.

I wrote these unittests to investigate some odd transaction
behavior when doing unittests of integrating non sub transaction
aware objects, and to insure proper txn behavior. these
tests test the transaction system independent of the rest of the
zodb.

you can see the method calls to a jar by passing the
keyword arg tracing to the modify method of a dataobject.
the value of the arg is a prefix used for tracing print calls
to that objects jar.

the number of times a jar method was called can be inspected
by looking at an attribute of the jar that is the method
name prefixed with a c (count/check).

i've included some tracing examples for tests that i thought
were illuminating as doc strings below.

TODO

    add in tests for objects which are modified multiple times,
    for example an object that gets modified in multiple sub txns.

$Id$
"""

import unittest
import warnings

import transaction
from ZODB.utils import positive_id
from ZODB.tests.warnhook import WarningsHook

class TransactionTests(unittest.TestCase):

    def setUp(self):
        mgr = self.transaction_manager = transaction.TransactionManager()
        self.sub1 = DataObject(mgr)
        self.sub2 = DataObject(mgr)
        self.sub3 = DataObject(mgr)
        self.nosub1 = DataObject(mgr, nost=1)

    # basic tests with two sub trans jars
    # really we only need one, so tests for
    # sub1 should identical to tests for sub2
    def testTransactionCommit(self):

        self.sub1.modify()
        self.sub2.modify()

        self.transaction_manager.commit()

        assert self.sub1._p_jar.ccommit_sub == 0
        assert self.sub1._p_jar.ctpc_finish == 1

    def testTransactionAbort(self):

        self.sub1.modify()
        self.sub2.modify()

        self.transaction_manager.abort()

        assert self.sub2._p_jar.cabort == 1

    def testTransactionNote(self):

        t = self.transaction_manager.get()

        t.note('This is a note.')
        self.assertEqual(t.description, 'This is a note.')
        t.note('Another.')
        self.assertEqual(t.description, 'This is a note.\n\nAnother.')

        t.abort()


    # repeat adding in a nonsub trans jars

    def testNSJTransactionCommit(self):

        self.nosub1.modify()

        self.transaction_manager.commit()

        assert self.nosub1._p_jar.ctpc_finish == 1

    def testNSJTransactionAbort(self):

        self.nosub1.modify()

        self.transaction_manager.abort()

        assert self.nosub1._p_jar.ctpc_finish == 0
        assert self.nosub1._p_jar.cabort == 1


    ### Failure Mode Tests
    #
    # ok now we do some more interesting
    # tests that check the implementations
    # error handling by throwing errors from
    # various jar methods
    ###

    # first the recoverable errors

    def testExceptionInAbort(self):

        self.sub1._p_jar = BasicJar(errors='abort')

        self.nosub1.modify()
        self.sub1.modify(nojar=1)
        self.sub2.modify()

        try:
            self.transaction_manager.abort()
        except TestTxnException: pass

        assert self.nosub1._p_jar.cabort == 1
        assert self.sub2._p_jar.cabort == 1

    def testExceptionInCommit(self):

        self.sub1._p_jar = BasicJar(errors='commit')

        self.nosub1.modify()
        self.sub1.modify(nojar=1)

        try:
            self.transaction_manager.commit()
        except TestTxnException: pass

        assert self.nosub1._p_jar.ctpc_finish == 0
        assert self.nosub1._p_jar.ccommit == 1
        assert self.nosub1._p_jar.ctpc_abort == 1

    def testExceptionInTpcVote(self):

        self.sub1._p_jar = BasicJar(errors='tpc_vote')

        self.nosub1.modify()
        self.sub1.modify(nojar=1)

        try:
            self.transaction_manager.commit()
        except TestTxnException: pass

        assert self.nosub1._p_jar.ctpc_finish == 0
        assert self.nosub1._p_jar.ccommit == 1
        assert self.nosub1._p_jar.ctpc_abort == 1
        assert self.sub1._p_jar.ctpc_abort == 1

    def testExceptionInTpcBegin(self):
        """
        ok this test reveals a bug in the TM.py
        as the nosub tpc_abort there is ignored.

        nosub calling method tpc_begin
        nosub calling method commit
        sub calling method tpc_begin
        sub calling method abort
        sub calling method tpc_abort
        nosub calling method tpc_abort
        """
        self.sub1._p_jar = BasicJar(errors='tpc_begin')

        self.nosub1.modify()
        self.sub1.modify(nojar=1)

        try:
            self.transaction_manager.commit()
        except TestTxnException:
            pass

        assert self.nosub1._p_jar.ctpc_abort == 1
        assert self.sub1._p_jar.ctpc_abort == 1

    def testExceptionInTpcAbort(self):
        self.sub1._p_jar = BasicJar(errors=('tpc_abort', 'tpc_vote'))

        self.nosub1.modify()
        self.sub1.modify(nojar=1)

        try:
            self.transaction_manager.commit()
        except TestTxnException:
            pass

        assert self.nosub1._p_jar.ctpc_abort == 1

    # last test, check the hosing mechanism

##    def testHoserStoppage(self):
##        # It's hard to test the "hosed" state of the database, where
##        # hosed means that a failure occurred in the second phase of
##        # the two phase commit.  It's hard because the database can
##        # recover from such an error if it occurs during the very first
##        # tpc_finish() call of the second phase.

##        for obj in self.sub1, self.sub2:
##            j = HoserJar(errors='tpc_finish')
##            j.reset()
##            obj._p_jar = j
##            obj.modify(nojar=1)

##        try:
##            transaction.commit()
##        except TestTxnException:
##            pass

##        self.assert_(Transaction.hosed)

##        self.sub2.modify()

##        try:
##            transaction.commit()
##        except Transaction.POSException.TransactionError:
##            pass
##        else:
##            self.fail("Hosed Application didn't stop commits")


class DataObject:

    def __init__(self, transaction_manager, nost=0):
        self.transaction_manager = transaction_manager
        self.nost = nost
        self._p_jar = None

    def modify(self, nojar=0, tracing=0):
        if not nojar:
            if self.nost:
                self._p_jar = BasicJar(tracing=tracing)
            else:
                self._p_jar = BasicJar(tracing=tracing)
        self.transaction_manager.get().join(self._p_jar)

class TestTxnException(Exception):
    pass

class BasicJar:

    def __init__(self, errors=(), tracing=0):
        if not isinstance(errors, tuple):
            errors = errors,
        self.errors = errors
        self.tracing = tracing
        self.cabort = 0
        self.ccommit = 0
        self.ctpc_begin = 0
        self.ctpc_abort = 0
        self.ctpc_vote = 0
        self.ctpc_finish = 0
        self.cabort_sub = 0
        self.ccommit_sub = 0

    def __repr__(self):
        return "<%s %X %s>" % (self.__class__.__name__,
                               positive_id(self),
                               self.errors)

    def sortKey(self):
        # All these jars use the same sort key, and Python's list.sort()
        # is stable.  These two
        return self.__class__.__name__

    def check(self, method):
        if self.tracing:
            print '%s calling method %s'%(str(self.tracing),method)

        if method in self.errors:
            raise TestTxnException("error %s" % method)

    ## basic jar txn interface

    def abort(self, *args):
        self.check('abort')
        self.cabort += 1

    def commit(self, *args):
        self.check('commit')
        self.ccommit += 1

    def tpc_begin(self, txn, sub=0):
        self.check('tpc_begin')
        self.ctpc_begin += 1

    def tpc_vote(self, *args):
        self.check('tpc_vote')
        self.ctpc_vote += 1

    def tpc_abort(self, *args):
        self.check('tpc_abort')
        self.ctpc_abort += 1

    def tpc_finish(self, *args):
        self.check('tpc_finish')
        self.ctpc_finish += 1

class HoserJar(BasicJar):

    # The HoserJars coordinate their actions via the class variable
    # committed.  The check() method will only raise its exception
    # if committed > 0.

    committed = 0

    def reset(self):
        # Calling reset() on any instance will reset the class variable.
        HoserJar.committed = 0

    def check(self, method):
        if HoserJar.committed > 0:
            BasicJar.check(self, method)

    def tpc_finish(self, *args):
        self.check('tpc_finish')
        self.ctpc_finish += 1
        HoserJar.committed += 1


def test_join():
    """White-box test of the join method

    The join method is provided for "backward-compatability" with ZODB 4
    data managers.

    The argument to join must be a zodb4 data manager,
    transaction.interfaces.IDataManager.

    >>> from ZODB.tests.sampledm import DataManager
    >>> from transaction._transaction import DataManagerAdapter
    >>> t = transaction.Transaction()
    >>> dm = DataManager()
    >>> t.join(dm)

    The end result is that a data manager adapter is one of the
    transaction's objects:

    >>> isinstance(t._resources[0], DataManagerAdapter)
    True
    >>> t._resources[0]._datamanager is dm
    True

    """

def hook():
    pass

# deprecated38; remove this then
def test_beforeCommitHook():
    """Test beforeCommitHook.

    Let's define a hook to call, and a way to see that it was called.

      >>> log = []
      >>> def reset_log():
      ...     del log[:]

      >>> def hook(arg='no_arg', kw1='no_kw1', kw2='no_kw2'):
      ...     log.append("arg %r kw1 %r kw2 %r" % (arg, kw1, kw2))

    beforeCommitHook is deprecated, so we need cruft to suppress the
    warnings.

      >>> whook = WarningsHook()
      >>> whook.install()

    Fool the warnings module into delivering the warnings despite that
    they've been seen before; this is needed in case this test is run
    more than once.

      >>> import warnings
      >>> warnings.filterwarnings("always", category=DeprecationWarning)

    Now register the hook with a transaction.

      >>> import transaction
      >>> t = transaction.begin()
      >>> t.beforeCommitHook(hook, '1')

    Make sure it triggered a deprecation warning:

      >>> len(whook.warnings)
      1
      >>> message, category, filename, lineno = whook.warnings[0]
      >>> print message
      This will be removed in ZODB 3.8:
      Use addBeforeCommitHook instead of beforeCommitHook.
      >>> category.__name__
      'DeprecationWarning'
      >>> whook.clear()

    We can see that the hook is indeed registered.

      >>> [(hook.func_name, args, kws)
      ...  for hook, args, kws in t.getBeforeCommitHooks()]
      [('hook', ('1',), {})]

    When transaction commit starts, the hook is called, with its
    arguments.

      >>> log
      []
      >>> t.commit()
      >>> log
      ["arg '1' kw1 'no_kw1' kw2 'no_kw2'"]
      >>> reset_log()

    A hook's registration is consumed whenever the hook is called.  Since
    the hook above was called, it's no longer registered:

      >>> len(list(t.getBeforeCommitHooks()))
      0
      >>> transaction.commit()
      >>> log
      []

    The hook is only called for a full commit, not for a savepoint.

      >>> t = transaction.begin()
      >>> t.beforeCommitHook(hook, 'A', kw1='B')
      >>> dummy = t.savepoint()
      >>> log
      []
      >>> t.commit()
      >>> log
      ["arg 'A' kw1 'B' kw2 'no_kw2'"]
      >>> reset_log()

    If a transaction is aborted, no hook is called.

      >>> t = transaction.begin()
      >>> t.beforeCommitHook(hook, "OOPS!")
      >>> transaction.abort()
      >>> log
      []
      >>> transaction.commit()
      >>> log
      []

    The hook is called before the commit does anything, so even if the
    commit fails the hook will have been called.  To provoke failures in
    commit, we'll add failing resource manager to the transaction.

      >>> class CommitFailure(Exception):
      ...     pass
      >>> class FailingDataManager:
      ...     def tpc_begin(self, txn, sub=False):
      ...         raise CommitFailure
      ...     def abort(self, txn):
      ...         pass

      >>> t = transaction.begin()
      >>> t.join(FailingDataManager())

      >>> t.beforeCommitHook(hook, '2')
      >>> t.commit()
      Traceback (most recent call last):
      ...
      CommitFailure
      >>> log
      ["arg '2' kw1 'no_kw1' kw2 'no_kw2'"]
      >>> reset_log()

    Let's register several hooks.

      >>> t = transaction.begin()
      >>> t.beforeCommitHook(hook, '4', kw1='4.1')
      >>> t.beforeCommitHook(hook, '5', kw2='5.2')

    They are returned in the same order by getBeforeCommitHooks.

      >>> [(hook.func_name, args, kws)     #doctest: +NORMALIZE_WHITESPACE
      ...  for hook, args, kws in t.getBeforeCommitHooks()]
      [('hook', ('4',), {'kw1': '4.1'}),
       ('hook', ('5',), {'kw2': '5.2'})]

    And commit also calls them in this order.

      >>> t.commit()
      >>> len(log)
      2
      >>> log  #doctest: +NORMALIZE_WHITESPACE
      ["arg '4' kw1 '4.1' kw2 'no_kw2'",
       "arg '5' kw1 'no_kw1' kw2 '5.2'"]
      >>> reset_log()

    While executing, a hook can itself add more hooks, and they will all
    be called before the real commit starts.

      >>> def recurse(txn, arg):
      ...     log.append('rec' + str(arg))
      ...     if arg:
      ...         txn.beforeCommitHook(hook, '-')
      ...         txn.beforeCommitHook(recurse, txn, arg-1)

      >>> t = transaction.begin()
      >>> t.beforeCommitHook(recurse, t, 3)
      >>> transaction.commit()
      >>> log  #doctest: +NORMALIZE_WHITESPACE
      ['rec3',
               "arg '-' kw1 'no_kw1' kw2 'no_kw2'",
       'rec2',
               "arg '-' kw1 'no_kw1' kw2 'no_kw2'",
       'rec1',
               "arg '-' kw1 'no_kw1' kw2 'no_kw2'",
       'rec0']
      >>> reset_log()

    We have to uninstall the warnings hook so that other warnings don't get
    lost.

      >>> whook.uninstall()

    Obscure:  There is no API call for removing the filter we added, but
    filters appears to be a public variable.

      >>> del warnings.filters[0]
    """

def test_addBeforeCommitHook():
    """Test addBeforeCommitHook.

    Let's define a hook to call, and a way to see that it was called.

      >>> log = []
      >>> def reset_log():
      ...     del log[:]

      >>> def hook(arg='no_arg', kw1='no_kw1', kw2='no_kw2'):
      ...     log.append("arg %r kw1 %r kw2 %r" % (arg, kw1, kw2))

    Now register the hook with a transaction.

      >>> import transaction
      >>> t = transaction.begin()
      >>> t.addBeforeCommitHook(hook, '1')

    We can see that the hook is indeed registered.

      >>> [(hook.func_name, args, kws)
      ...  for hook, args, kws in t.getBeforeCommitHooks()]
      [('hook', ('1',), {})]

    When transaction commit starts, the hook is called, with its
    arguments.

      >>> log
      []
      >>> t.commit()
      >>> log
      ["arg '1' kw1 'no_kw1' kw2 'no_kw2'"]
      >>> reset_log()

    A hook's registration is consumed whenever the hook is called.  Since
    the hook above was called, it's no longer registered:

      >>> len(list(t.getBeforeCommitHooks()))
      0
      >>> transaction.commit()
      >>> log
      []

    The hook is only called for a full commit, not for a savepoint.

      >>> t = transaction.begin()
      >>> t.addBeforeCommitHook(hook, 'A', dict(kw1='B'))
      >>> dummy = t.savepoint()
      >>> log
      []
      >>> t.commit()
      >>> log
      ["arg 'A' kw1 'B' kw2 'no_kw2'"]
      >>> reset_log()

    If a transaction is aborted, no hook is called.

      >>> t = transaction.begin()
      >>> t.addBeforeCommitHook(hook, ["OOPS!"])
      >>> transaction.abort()
      >>> log
      []
      >>> transaction.commit()
      >>> log
      []

    The hook is called before the commit does anything, so even if the
    commit fails the hook will have been called.  To provoke failures in
    commit, we'll add failing resource manager to the transaction.

      >>> class CommitFailure(Exception):
      ...     pass
      >>> class FailingDataManager:
      ...     def tpc_begin(self, txn, sub=False):
      ...         raise CommitFailure
      ...     def abort(self, txn):
      ...         pass

      >>> t = transaction.begin()
      >>> t.join(FailingDataManager())

      >>> t.addBeforeCommitHook(hook, '2')
      >>> t.commit()
      Traceback (most recent call last):
      ...
      CommitFailure
      >>> log
      ["arg '2' kw1 'no_kw1' kw2 'no_kw2'"]
      >>> reset_log()

    Let's register several hooks.

      >>> t = transaction.begin()
      >>> t.addBeforeCommitHook(hook, '4', dict(kw1='4.1'))
      >>> t.addBeforeCommitHook(hook, '5', dict(kw2='5.2'))

    They are returned in the same order by getBeforeCommitHooks.

      >>> [(hook.func_name, args, kws)     #doctest: +NORMALIZE_WHITESPACE
      ...  for hook, args, kws in t.getBeforeCommitHooks()]
      [('hook', ('4',), {'kw1': '4.1'}),
       ('hook', ('5',), {'kw2': '5.2'})]

    And commit also calls them in this order.

      >>> t.commit()
      >>> len(log)
      2
      >>> log  #doctest: +NORMALIZE_WHITESPACE
      ["arg '4' kw1 '4.1' kw2 'no_kw2'",
       "arg '5' kw1 'no_kw1' kw2 '5.2'"]
      >>> reset_log()

    While executing, a hook can itself add more hooks, and they will all
    be called before the real commit starts.

      >>> def recurse(txn, arg):
      ...     log.append('rec' + str(arg))
      ...     if arg:
      ...         txn.addBeforeCommitHook(hook, '-')
      ...         txn.addBeforeCommitHook(recurse, (txn, arg-1))

      >>> t = transaction.begin()
      >>> t.addBeforeCommitHook(recurse, (t, 3))
      >>> transaction.commit()
      >>> log  #doctest: +NORMALIZE_WHITESPACE
      ['rec3',
               "arg '-' kw1 'no_kw1' kw2 'no_kw2'",
       'rec2',
               "arg '-' kw1 'no_kw1' kw2 'no_kw2'",
       'rec1',
               "arg '-' kw1 'no_kw1' kw2 'no_kw2'",
       'rec0']
      >>> reset_log()

    When modifing persitent objects within before commit hooks
    modifies the objects, of course :)
    
    Start a new transaction

      >>> t = transaction.begin()

    Create a DB instance and add a IOBTree within

      >>> from ZODB.tests.util import DB
      >>> from ZODB.tests.util import P
      >>> db = DB()
      >>> con = db.open()
      >>> root = con.root()
      >>> root['p'] = P('julien')
      >>> p = root['p']

      >>> p.name
      'julien'
      
    This hook will get the object from the `DB` instance and change
    the flag attribute.

      >>> def hookmodify(status, arg=None, kw1='no_kw1', kw2='no_kw2'):
      ...     p.name = 'jul'

    Now register this hook and commit.

      >>> t.addBeforeCommitHook(hookmodify, (p, 1))
      >>> transaction.commit()

    Nothing should have changed since it should have been aborted.

      >>> p.name
      'jul'

      >>> db.close()
    """

def test_addAfterCommitHook():
    """Test addAfterCommitHook.

    Let's define a hook to call, and a way to see that it was called.

      >>> log = []
      >>> def reset_log():
      ...     del log[:]

      >>> def hook(status, arg='no_arg', kw1='no_kw1', kw2='no_kw2'):
      ...     log.append("%r arg %r kw1 %r kw2 %r" % (status, arg, kw1, kw2))

    Now register the hook with a transaction.

      >>> import transaction
      >>> t = transaction.begin()
      >>> t.addAfterCommitHook(hook, '1')

    We can see that the hook is indeed registered.

      >>> [(hook.func_name, args, kws)
      ...  for hook, args, kws in t.getAfterCommitHooks()]
      [('hook', ('1',), {})]

    When transaction commit is done, the hook is called, with its
    arguments.

      >>> log
      []
      >>> t.commit()
      >>> log
      ["True arg '1' kw1 'no_kw1' kw2 'no_kw2'"]
      >>> reset_log()

    A hook's registration is consumed whenever the hook is called.  Since
    the hook above was called, it's no longer registered:

      >>> len(list(t.getAfterCommitHooks()))
      0
      >>> transaction.commit()
      >>> log
      []

    The hook is only called after a full commit, not for a savepoint.

      >>> t = transaction.begin()
      >>> t.addAfterCommitHook(hook, 'A', dict(kw1='B'))
      >>> dummy = t.savepoint()
      >>> log
      []
      >>> t.commit()
      >>> log
      ["True arg 'A' kw1 'B' kw2 'no_kw2'"]
      >>> reset_log()

    If a transaction is aborted, the hook is called with False:

      >>> t = transaction.begin()
      >>> t.addAfterCommitHook(hook, ["OOPS!"])
      >>> transaction.abort()
      >>> log
      ["False arg 'OOPS!' kw1 'no_kw1' kw2 'no_kw2'"]
      >>> reset_log()
      >>> transaction.commit()
      >>> log
      []

    The hook is called after the commit is done, so even if the
    commit fails the hook will have been called.  To provoke failures in
    commit, we'll add failing resource manager to the transaction.

      >>> class CommitFailure(Exception):
      ...     pass
      >>> class FailingDataManager:
      ...     def tpc_begin(self, txn):
      ...         raise CommitFailure
      ...     def abort(self, txn):
      ...         pass

      >>> t = transaction.begin()
      >>> t.join(FailingDataManager())

      >>> t.addAfterCommitHook(hook, '2')
      >>> t.commit()
      Traceback (most recent call last):
      ...
      CommitFailure
      >>> log
      ["False arg '2' kw1 'no_kw1' kw2 'no_kw2'"]
      >>> reset_log()

    Let's register several hooks.

      >>> t = transaction.begin()
      >>> t.addAfterCommitHook(hook, '4', dict(kw1='4.1'))
      >>> t.addAfterCommitHook(hook, '5', dict(kw2='5.2'))

    They are returned in the same order by getAfterCommitHooks.

      >>> [(hook.func_name, args, kws)     #doctest: +NORMALIZE_WHITESPACE
      ...  for hook, args, kws in t.getAfterCommitHooks()]
      [('hook', ('4',), {'kw1': '4.1'}),
       ('hook', ('5',), {'kw2': '5.2'})]

    And commit also calls them in this order.

      >>> t.commit()
      >>> len(log)
      2
      >>> log  #doctest: +NORMALIZE_WHITESPACE
      ["True arg '4' kw1 '4.1' kw2 'no_kw2'",
       "True arg '5' kw1 'no_kw1' kw2 '5.2'"]
      >>> reset_log()

    While executing, a hook can itself add more hooks, and they will all
    be called before the real commit starts.

      >>> def recurse(status, txn, arg):
      ...     log.append('rec' + str(arg))
      ...     if arg:
      ...         txn.addAfterCommitHook(hook, '-')
      ...         txn.addAfterCommitHook(recurse, (txn, arg-1))

      >>> t = transaction.begin()
      >>> t.addAfterCommitHook(recurse, (t, 3))
      >>> transaction.commit()
      >>> log  #doctest: +NORMALIZE_WHITESPACE
      ['rec3',
               "True arg '-' kw1 'no_kw1' kw2 'no_kw2'",
       'rec2',
               "True arg '-' kw1 'no_kw1' kw2 'no_kw2'",
       'rec1',
               "True arg '-' kw1 'no_kw1' kw2 'no_kw2'",
       'rec0']
      >>> reset_log()

    If an after commit hook is raising an exception then it will log a
    message at error level so that if other hooks are registered they
    can be executed. We don't support execution dependencies at this level.

      >>> mgr = transaction.TransactionManager()
      >>> do = DataObject(mgr)

      >>> def hookRaise(status, arg='no_arg', kw1='no_kw1', kw2='no_kw2'):
      ...     raise TypeError("Fake raise")

      >>> t = transaction.begin()

      >>> t.addAfterCommitHook(hook, ('-', 1))
      >>> t.addAfterCommitHook(hookRaise, ('-', 2))
      >>> t.addAfterCommitHook(hook, ('-', 3))
      >>> transaction.commit()

      >>> log
      ["True arg '-' kw1 1 kw2 'no_kw2'", "True arg '-' kw1 3 kw2 'no_kw2'"]

      >>> reset_log()

    Test that the associated transaction manager has been cleanup when
    after commit hooks are registered

      >>> mgr = transaction.TransactionManager()
      >>> do = DataObject(mgr)

      >>> t = transaction.begin()
      >>> len(t._manager._txns)
      1

      >>> t.addAfterCommitHook(hook, ('-', 1))
      >>> transaction.commit()

      >>> log
      ["True arg '-' kw1 1 kw2 'no_kw2'"]

      >>> len(t._manager._txns)
      0

      >>> reset_log()


    The transaction is already committed when the after commit hooks
    will be executed. Executing the hooks must not have further
    effects on persistent objects.

    Start a new transaction

      >>> t = transaction.begin()

    Create a DB instance and add a IOBTree within

      >>> from ZODB.tests.util import DB
      >>> from ZODB.tests.util import P
      >>> db = DB()
      >>> con = db.open()
      >>> root = con.root()
      >>> root['p'] = P('julien')
      >>> p = root['p']

      >>> p.name
      'julien'
      
    This hook will get the object from the `DB` instance and change
    the flag attribute.

      >>> def badhook(status, arg=None, kw1='no_kw1', kw2='no_kw2'):
      ...     p.name = 'jul'

    Now register this hook and commit.

      >>> t.addAfterCommitHook(badhook, (p, 1))
      >>> transaction.commit()

    Nothing should have changed since it should have been aborted.

      >>> p.name
      'julien'

      >>> db.close()

    """

def test_suite():
    from zope.testing.doctest import DocTestSuite, DocFileSuite
    return unittest.TestSuite((
        DocFileSuite('doom.txt'),
        DocTestSuite(),
        unittest.makeSuite(TransactionTests),
        ))

if __name__ == '__main__':
    unittest.TextTestRunner().run(test_suite())
