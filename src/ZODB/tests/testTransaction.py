##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

"""
Revision information:
$Id: testTransaction.py,v 1.12 2003/01/27 20:29:50 bwarsaw Exp $
"""

"""

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

"""

import random
from types import TupleType
import unittest

from ZODB import Transaction

class TransactionTests(unittest.TestCase):

    def setUp(self):

        Transaction.hosed = 0
        self.sub1 = DataObject()
        self.sub2 = DataObject()
        self.sub3 = DataObject()
        self.nosub1 = DataObject(nost=1)

    def tearDown(self):

        Transaction.free_transaction()

    # basic tests with two sub trans jars
    # really we only need one, so tests for
    # sub1 should identical to tests for sub2
    def testTransactionCommit(self):

        self.sub1.modify()
        self.sub2.modify()

        get_transaction().commit()

        assert self.sub1._p_jar.ccommit_sub == 0
        assert self.sub1._p_jar.ctpc_finish == 1

    def testTransactionAbort(self):

        self.sub1.modify()
        self.sub2.modify()

        get_transaction().abort()

        assert self.sub2._p_jar.cabort == 1

    def testSubTransactionCommitCommit(self):

        self.sub1.modify()
        self.sub2.modify()

        get_transaction().commit(1)

        assert self.sub1._p_jar.ctpc_vote == 0
        assert self.sub1._p_jar.ctpc_finish == 1

        get_transaction().commit()

        assert self.sub1._p_jar.ccommit_sub == 1
        assert self.sub1._p_jar.ctpc_vote == 1

    def testSubTransactionCommitAbort(self):

        self.sub1.modify()
        self.sub2.modify()

        get_transaction().commit(1)
        get_transaction().abort()

        assert self.sub1._p_jar.ctpc_vote == 0
        assert self.sub1._p_jar.cabort == 0
        assert self.sub1._p_jar.cabort_sub == 1

    def testMultipleSubTransactionCommitCommit(self):

        # add it
        self.sub1.modify()

        get_transaction().commit(1)

        # add another
        self.sub2.modify()

        # reset a flag on the original to test it again
        self.sub1.ctpc_finish = 0

        get_transaction().commit(1)

        # this is interesting.. we go through
        # every subtrans commit with all subtrans capable
        # objects... i don't like this but its an impl artifact

        assert self.sub1._p_jar.ctpc_vote == 0
        assert self.sub1._p_jar.ctpc_finish > 0

        # add another before we do the entire txn commit
        self.sub3.modify()

        get_transaction().commit()

        # we did an implicit sub commit, is this impl artifiact?
        assert self.sub3._p_jar.ccommit_sub == 1
        assert self.sub1._p_jar.ctpc_finish > 1


    def testMultipleSubTransactionCommitAbortSub(self):
        """
        sub1 calling method commit
        sub1 calling method tpc_finish
        sub2 calling method tpc_begin
        sub2 calling method commit
        sub2 calling method tpc_finish
        sub3 calling method abort
        sub1 calling method commit_sub
        sub2 calling method commit_sub
        sub2 calling method tpc_vote
        sub1 calling method tpc_vote
        sub1 calling method tpc_finish
        sub2 calling method tpc_finish
        """

        # add it
        self.sub1.modify()

        get_transaction().commit(1)

        # add another
        self.sub2.modify()

        get_transaction().commit(1)

        assert self.sub1._p_jar.ctpc_vote == 0
        assert self.sub1._p_jar.ctpc_finish > 0

        # add another before we do the entire txn commit
        self.sub3.modify()

        # abort the sub transaction
        get_transaction().abort(1)

        # commit the container transaction
        get_transaction().commit()

        assert self.sub3._p_jar.cabort == 1
        assert self.sub1._p_jar.ccommit_sub == 1
        assert self.sub1._p_jar.ctpc_finish > 1

    # repeat adding in a nonsub trans jars

    def testNSJTransactionCommit(self):

        self.nosub1.modify()

        get_transaction().commit()

        assert self.nosub1._p_jar.ctpc_finish == 1

    def testNSJTransactionAbort(self):

        self.nosub1.modify()

        get_transaction().abort()

        assert self.nosub1._p_jar.ctpc_finish == 0
        assert self.nosub1._p_jar.cabort == 1

    # XXX
    def BUGtestNSJSubTransactionCommitAbort(self):
        """
        this reveals a bug in transaction.py
        the nosub jar should not have tpc_finish
        called on it till the containing txn
        ends.

        sub calling method commit
        nosub calling method tpc_begin
        sub calling method tpc_finish
        nosub calling method tpc_finish
        nosub calling method abort
        sub calling method abort_sub
        """

        self.sub1.modify(tracing='sub')
        self.nosub1.modify(tracing='nosub')

        get_transaction().commit(1)

        assert self.sub1._p_jar.ctpc_finish == 1

        # bug, non sub trans jars are getting finished
        # in a subtrans
        assert self.nosub1._p_jar.ctpc_finish == 0

        get_transaction().abort()

        assert self.nosub1._p_jar.cabort == 1
        assert self.sub1._p_jar.cabort_sub == 1

    def testNSJSubTransactionCommitCommit(self):

        self.sub1.modify()
        self.nosub1.modify()

        get_transaction().commit(1)

        assert self.nosub1._p_jar.ctpc_vote == 0

        get_transaction().commit()

        #assert self.nosub1._p_jar.ccommit_sub == 0
        assert self.nosub1._p_jar.ctpc_vote == 1
        assert self.sub1._p_jar.ccommit_sub == 1
        assert self.sub1._p_jar.ctpc_vote == 1


    def testNSJMultipleSubTransactionCommitCommit(self):
        """
        sub1 calling method tpc_begin
        sub1 calling method commit
        sub1 calling method tpc_finish
        nosub calling method tpc_begin
        nosub calling method tpc_finish
        sub2 calling method tpc_begin
        sub2 calling method commit
        sub2 calling method tpc_finish
        nosub calling method tpc_begin
        nosub calling method commit
        sub1 calling method commit_sub
        sub2 calling method commit_sub
        sub1 calling method tpc_vote
        nosub calling method tpc_vote
        sub2 calling method tpc_vote
        sub2 calling method tpc_finish
        nosub calling method tpc_finish
        sub1 calling method tpc_finish
        """

        # add it
        self.sub1.modify()

        get_transaction().commit(1)

        # add another
        self.nosub1.modify()

        get_transaction().commit(1)

        assert self.sub1._p_jar.ctpc_vote == 0
        assert self.nosub1._p_jar.ctpc_vote == 0
        assert self.sub1._p_jar.ctpc_finish > 0

        # add another before we do the entire txn commit
        self.sub2.modify()

        # commit the container transaction
        get_transaction().commit()

        # we did an implicit sub commit
        assert self.sub2._p_jar.ccommit_sub == 1
        assert self.sub1._p_jar.ctpc_finish > 1

    ### Failure Mode Tests
    #
    # ok now we do some more interesting
    # tests that check the implementations
    # error handling by throwing errors from
    # various jar methods
    ###

    # first the recoverable errors

    def testExceptionInAbort(self):

        self.sub1._p_jar = SubTransactionJar(errors='abort')

        self.nosub1.modify()
        self.sub1.modify(nojar=1)
        self.sub2.modify()

        try:
            get_transaction().abort()
        except TestTxnException: pass

        assert self.nosub1._p_jar.cabort == 1
        assert self.sub2._p_jar.cabort == 1

    def testExceptionInCommit(self):

        self.sub1._p_jar = SubTransactionJar(errors='commit')

        self.nosub1.modify()
        self.sub1.modify(nojar=1)

        try:
            get_transaction().commit()
        except TestTxnException: pass

        assert self.nosub1._p_jar.ctpc_finish == 0
        assert self.nosub1._p_jar.ccommit == 1
        assert self.nosub1._p_jar.ctpc_abort == 1
        assert Transaction.hosed == 0

    def testExceptionInTpcVote(self):

        self.sub1._p_jar = SubTransactionJar(errors='tpc_vote')

        self.nosub1.modify()
        self.sub1.modify(nojar=1)

        try:
            get_transaction().commit()
        except TestTxnException: pass

        assert self.nosub1._p_jar.ctpc_finish == 0
        assert self.nosub1._p_jar.ccommit == 1
        assert self.nosub1._p_jar.ctpc_abort == 1
        assert self.sub1._p_jar.ctpc_abort == 1
        assert Transaction.hosed == 0

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
        self.sub1._p_jar = SubTransactionJar(errors='tpc_begin')

        self.nosub1.modify()
        self.sub1.modify(nojar=1)

        try:
            get_transaction().commit()
        except TestTxnException: pass

        assert self.nosub1._p_jar.ctpc_abort == 1
        assert self.sub1._p_jar.ctpc_abort == 1

    def testExceptionInTpcAbort(self):

        self.sub1._p_jar = SubTransactionJar(
                                errors=('tpc_abort', 'tpc_vote'))

        self.nosub1.modify()
        self.sub1.modify(nojar=1)

        try:
            get_transaction().commit()
        except TestTxnException: pass

        assert self.nosub1._p_jar.ctpc_abort == 1
        assert Transaction.hosed == 0

    ### More Failure modes...
    # now we mix in some sub transactions
    ###

    def testExceptionInSubCommitSub(self):
        """
        this tests exhibits some odd behavior,
        nothing thats technically incorrect...

        basically it seems non deterministic, even
        stranger the behavior seems dependent on what
        values i test after the fact... very odd,
        almost relativistic.

        in-retrospect this is from the fact that
        dictionaries are used to store jars at some point

        """

        self.sub1.modify()

        get_transaction().commit(1)

        self.nosub1.modify()

        self.sub2._p_jar = SubTransactionJar(errors='commit_sub')
        self.sub2.modify(nojar=1)

        get_transaction().commit(1)

        self.sub3.modify()

        try:
            get_transaction().commit()
        except TestTxnException: pass


        # odd this doesn't seem to be entirely deterministic..
        if self.sub1._p_jar.ccommit_sub:
            assert self.sub1._p_jar.ctpc_abort == 1
        else:
            assert self.sub1._p_jar.cabort_sub == 1

        if self.sub3._p_jar.ccommit_sub:
            assert self.sub3._p_jar.ctpc_abort == 1
        else:
            assert self.sub3._p_jar.cabort_sub == 1

        assert self.sub2._p_jar.ctpc_abort == 1
        assert self.nosub1._p_jar.ctpc_abort == 1

    def testExceptionInSubAbortSub(self):
        self.sub1._p_jar = SubTransactionJar(errors='commit_sub')
        self.sub1.modify(nojar=1)
        get_transaction().commit(1)

        self.nosub1.modify()
        self.sub2._p_jar = SubTransactionJar(errors='abort_sub')
        self.sub2.modify(nojar=1)
        get_transaction().commit(1)

        self.sub3.modify()

        try:
            get_transaction().commit()
        except TestTxnException, err:
            pass
        else:
            self.fail("expected transaction to fail")

        # The last commit failed.  If the commit_sub() method was
        # called, then tpc_abort() should be called to abort the
        # actual transaction.  If not, then calling abort_sub() is
        # sufficient.
        if self.sub3._p_jar.ccommit_sub == 1:
            self.assertEqual(self.sub3._p_jar.ctpc_abort, 1)
        else:
            self.assertEqual(self.sub3._p_jar.cabort_sub, 1)

    # last test, check the hosing mechanism

    def testHoserStoppage(self):
        # XXX We should consult ZConfig to decide whether we can get into a
        # hosed state or not.
        return

        # It's hard to test the "hosed" state of the database, where
        # hosed means that a failure occurred in the second phase of
        # the two phase commit.  It's hard because the database can
        # recover from such an error if it occurs during the very first
        # tpc_finish() call of the second phase.

        for obj in self.sub1, self.sub2:
            j = HoserJar(errors='tpc_finish')
            j.reset()
            obj._p_jar = j
            obj.modify(nojar=1)

        try:
            get_transaction().commit()
        except TestTxnException:
            pass

        self.assert_(Transaction.hosed)

        self.sub2.modify()

        try:
            get_transaction().commit()
        except Transaction.POSException.TransactionError:
            pass
        else:
            raise "Hosed Application didn't stop commits"


class DataObject:

    def __init__(self, nost=0):
        self.nost= nost
        self._p_jar = None

    def modify(self, nojar=0, tracing=0):
        if not nojar:
            if self.nost:
                self._p_jar = NoSubTransactionJar(tracing=tracing)
            else:
                self._p_jar = SubTransactionJar(tracing=tracing)
        get_transaction().register(self)

class TestTxnException(Exception):
    pass

class BasicJar:

    def __init__(self, errors=(), tracing=0):
        if not isinstance(errors, TupleType):
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
        return "<jar %X %s>" % (id(self), self.errors)

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

class SubTransactionJar(BasicJar):

    def abort_sub(self, txn):
        self.check('abort_sub')
        self.cabort_sub = 1

    def commit_sub(self, txn):
        self.check('commit_sub')
        self.ccommit_sub = 1

class NoSubTransactionJar(BasicJar):
    pass

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
        

def test_suite():

    return unittest.makeSuite(TransactionTests)

if __name__ == '__main__':
    unittest.TextTestRunner().run(test_suite())
