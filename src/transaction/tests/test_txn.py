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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Basic tests of the transaction manager."""

import unittest

from transaction.interfaces import *
from transaction.manager import TransactionManager, ThreadedTransactionManager
from transaction.txn import Status

class TestDataManager:

    def __init__(self, fail=None, vote=True):
        # pass the name of a method that should fail as fail
        self._fail = fail
        # pass the return value for prepare as vote
        self._vote = vote

    def prepare(self, txn):
        if self._fail == "prepare":
            raise RuntimeError
        return self._vote

    def abort(self, txn):
        if self._fail == "abort":
            raise RuntimeError

    def commit(self, txn):
        if self._fail == "commit":
            raise RuntimeError

    def savepoint(self, txn):
        if self._fail == "savepoint":
            raise RuntimeError
        # XXX should anything be done here?

class BaseTxnTests(unittest.TestCase):

    def setUp(self):
        self.manager = self.ManagerFactory()

    def tearDown(self):
        pass

    def testBegin(self):
        txn = self.manager.begin()
        self.assertEqual(txn.status(), Status.ACTIVE)

        txn2 = self.manager.get()
        self.assertEqual(id(txn), id(txn2))

        txn3 = self.manager.begin()
        self.assert_(id(txn) != id(txn3))
        self.assertEqual(txn.status(), Status.ABORTED)

    # the trivial tests don't involve any resource managers

    def testTrivialCommit(self):
        txn = self.manager.begin()
        txn.commit()
        self.assertEqual(txn.status(), Status.COMMITTED)
        self.assertRaises(IllegalStateError, txn.commit)
        self.assertRaises(IllegalStateError, txn.savepoint)
        self.assertRaises(IllegalStateError, txn.abort)

    def testTrivialAbort(self):
        txn = self.manager.begin()
        txn.abort()
        self.assertEqual(txn.status(), Status.ABORTED)
        self.assertRaises(IllegalStateError, txn.commit)
        self.assertRaises(IllegalStateError, txn.savepoint)
        self.assertRaises(IllegalStateError, txn.abort)

    def testTrivialSavepoint(self):
        txn = self.manager.begin()
        r1 = txn.savepoint()
        r2 = txn.savepoint()
        r2.rollback()
        txn.abort()
        self.assertRaises(IllegalStateError, r2.rollback)

    def testTrivialSuspendResume(self):
        txn1 = self.manager.begin()
        txn1.suspend()
        self.assertRaises(TransactionError, txn1.suspend)
        txn2 = self.manager.begin()
        self.assert_(txn1 != txn2)
        txn2.suspend()
        txn1.resume()
        txn1.commit()
        self.assertRaises(TransactionError, txn1.suspend)
        txn2.resume()
        txn2.abort()
        self.assertRaises(TransactionError, txn2.suspend)

    # XXX need a multi-threaded test of suspend / resume

    # more complex tests use a simple data manager

    def testCommit(self):
        txn = self.manager.begin()
        for i in range(3):
            txn.join(TestDataManager())
        txn.commit()

    def testCommitPrepareException(self):
        txn = self.manager.begin()
        txn.join(TestDataManager())
        txn.join(TestDataManager(fail="prepare"))
        self.assertRaises(RuntimeError, txn.commit)
        self.assertEqual(txn.status(), Status.FAILED)
        txn.abort()

    def testCommitPrepareFalse(self):
        txn = self.manager.begin()
        txn.join(TestDataManager())
        txn.join(TestDataManager(vote=False))
        self.assertRaises(AbortError, txn.commit)
        self.assertEqual(txn.status(), Status.FAILED)
        self.assertRaises(IllegalStateError, txn.commit)
        txn.abort()

class SimpleTxnTests(BaseTxnTests):

    ManagerFactory = TransactionManager

class ThreadedTxnTests(BaseTxnTests):

    ManagerFactory = ThreadedTransactionManager

def test_suite():
    s = unittest.TestSuite()
    for klass in SimpleTxnTests, ThreadedTxnTests:
        s.addTest(unittest.makeSuite(klass))
    return s

