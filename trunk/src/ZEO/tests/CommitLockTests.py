##############################################################################
#
# Copyright (c) 2002 Zope Corporation and Contributors.
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
"""Tests of the distributed commit lock."""

import threading
import time

from ZODB.Transaction import Transaction
from ZODB.TimeStamp import TimeStamp
from ZODB.tests.StorageTestBase import zodb_pickle, MinPO

import ZEO.ClientStorage
from ZEO.Exceptions import Disconnected
from ZEO.tests.TestThread import TestThread

ZERO = '\0'*8

class DummyDB:
    def invalidate(self, *args):
        pass

class WorkerThread(TestThread):

    # run the entire test in a thread so that the blocking call for
    # tpc_vote() doesn't hang the test suite.

    def __init__(self, testcase, storage, trans, method="tpc_finish"):
        self.storage = storage
        self.trans = trans
        self.method = method
        self.ready = threading.Event()
        TestThread.__init__(self, testcase)

    def testrun(self):
        try:
            self.storage.tpc_begin(self.trans)
            oid = self.storage.new_oid()
            p = zodb_pickle(MinPO("c"))
            self.storage.store(oid, ZERO, p, '', self.trans)
            oid = self.storage.new_oid()
            p = zodb_pickle(MinPO("c"))
            self.storage.store(oid, ZERO, p, '', self.trans)
            self.ready.set()
            self.storage.tpc_vote(self.trans)
            if self.method == "tpc_finish":
                self.storage.tpc_finish(self.trans)
            else:
                self.storage.tpc_abort(self.trans)
        except Disconnected:
            pass

class CommitLockTests:

    # The commit lock tests verify that the storage successfully
    # blocks and restarts transactions when there is content for a
    # single storage.  There are a lot of cases to cover.

    # CommitLock1 checks the case where a single transaction delays
    # other transactions before they actually block.  IOW, by the time
    # the other transactions get to the vote stage, the first
    # transaction has finished.

    def checkCommitLock1OnCommit(self):
        self._storages = []
        try:
            self._checkCommitLock("tpc_finish", self._dosetup1, self._dowork1)
        finally:
            self._cleanup()

    def checkCommitLock1OnAbort(self):
        self._storages = []
        try:
            self._checkCommitLock("tpc_abort", self._dosetup1, self._dowork1)
        finally:
            self._cleanup()

    def checkCommitLock2OnCommit(self):
        self._storages = []
        try:
            self._checkCommitLock("tpc_finish", self._dosetup2, self._dowork2)
        finally:
            self._cleanup()

    def checkCommitLock2OnAbort(self):
        self._storages = []
        try:
            self._checkCommitLock("tpc_abort", self._dosetup2, self._dowork2)
        finally:
            self._cleanup()

    def _cleanup(self):
        for store, trans in self._storages:
            store.tpc_abort(trans)
            store.close()
        self._storages = []

    def _checkCommitLock(self, method_name, dosetup, dowork):
        # check the commit lock when a client attemps a transaction,
        # but fails/exits before finishing the commit.

        # The general flow of these tests is to start a transaction by
        # calling tpc_begin().  Then begin one or more other
        # connections that also want to commit.  This causes the
        # commit lock code to be exercised.  Once the other
        # connections are started, the first transaction completes.
        # Either by commit or abort, depending on whether method_name
        # is "tpc_finish."

        # The tests are parameterized by method_name, dosetup(), and
        # dowork().  The dosetup() function is called with a
        # connectioned client storage, transaction, and timestamp.
        # Any work it does occurs after the first transaction has
        # started, but before it finishes.  The dowork() function
        # executes after the first transaction has completed.

        # Start on transaction normally.
        t = Transaction()
        self._storage.tpc_begin(t)

        # Start a second transaction on a different connection without
        # blocking the test thread.
        self._storages = []
        for i in range(4):
            storage2 = self._duplicate_client()
            t2 = Transaction()
            tid = self._get_timestamp()
            dosetup(storage2, t2, tid)
            if i == 0:
                storage2.close()
            else:
                self._storages.append((storage2, t2))

        oid = self._storage.new_oid()
        self._storage.store(oid, ZERO, zodb_pickle(MinPO(1)), '', t)
        self._storage.tpc_vote(t)
        if method_name == "tpc_finish":
            self._storage.tpc_finish(t)
            self._storage.load(oid, '')
        else:
            self._storage.tpc_abort(t)

        dowork(method_name)

        # Make sure the server is still responsive
        self._dostore()

    def _dosetup1(self, storage, trans, tid):
        storage.tpc_begin(trans, tid)

    def _dowork1(self, method_name):
        for store, trans in self._storages:
            oid = store.new_oid()
            store.store(oid, ZERO, zodb_pickle(MinPO("c")), '', trans)
            store.tpc_vote(trans)
            if method_name == "tpc_finish":
                store.tpc_finish(trans)
            else:
                store.tpc_abort(trans)

    def _dosetup2(self, storage, trans, tid):
        self._threads = []
        t = WorkerThread(self, storage, trans)
        self._threads.append(t)
        t.start()
        t.ready.wait()

    def _dowork2(self, method_name):
        for t in self._threads:
            t.cleanup()

    def _duplicate_client(self):
        "Open another ClientStorage to the same server."
        # XXX argh it's hard to find the actual address
        # The rpc mgr addr attribute is a list.  Each element in the
        # list is a socket domain (AF_INET, AF_UNIX, etc.) and an
        # address.
        addr = self._storage._rpc_mgr.addr[0][1]
        new = ZEO.ClientStorage.ClientStorage(addr, wait=1)
        new.registerDB(DummyDB(), None)
        return new

    def _get_timestamp(self):
        t = time.time()
        t = apply(TimeStamp,(time.gmtime(t)[:5]+(t%60,)))
        return `t`
