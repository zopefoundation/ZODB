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
"""Compromising positions involving threads."""

import threading

from ZODB.Transaction import Transaction
from ZODB.tests.StorageTestBase import zodb_pickle, MinPO

import ZEO.ClientStorage
from ZEO.Exceptions import Disconnected

ZERO = '\0'*8

class BasicThread(threading.Thread):
    def __init__(self, storage, doNextEvent, threadStartedEvent):
        self.storage = storage
        self.trans = Transaction()
        self.doNextEvent = doNextEvent
        self.threadStartedEvent = threadStartedEvent
        self.gotValueError = 0
        self.gotDisconnected = 0
        threading.Thread.__init__(self)
        self.setDaemon(1)

    def join(self):
        threading.Thread.join(self, 10)
        assert not self.isAlive()


class GetsThroughVoteThread(BasicThread):
    # This thread gets partially through a transaction before it turns
    # execution over to another thread.  We're trying to establish that a
    # tpc_finish() after a storage has been closed by another thread will get
    # a ClientStorageError error.
    #
    # This class gets does a tpc_begin(), store(), tpc_vote() and is waiting
    # to do the tpc_finish() when the other thread closes the storage.
    def run(self):
        self.storage.tpc_begin(self.trans)
        oid = self.storage.new_oid()
        self.storage.store(oid, ZERO, zodb_pickle(MinPO("c")), '', self.trans)
        self.storage.tpc_vote(self.trans)
        self.threadStartedEvent.set()
        self.doNextEvent.wait(10)
        try:
            self.storage.tpc_finish(self.trans)
        except ZEO.ClientStorage.ClientStorageError:
            self.gotValueError = 1
            self.storage.tpc_abort(self.trans)


class GetsThroughBeginThread(BasicThread):
    # This class is like the above except that it is intended to be run when
    # another thread is already in a tpc_begin().  Thus, this thread will
    # block in the tpc_begin until another thread closes the storage.  When
    # that happens, this one will get disconnected too.
    def run(self):
        try:
            self.storage.tpc_begin(self.trans)
        except ZEO.ClientStorage.ClientStorageError:
            self.gotValueError = 1


class AbortsAfterBeginFailsThread(BasicThread):
    # This class is identical to GetsThroughBeginThread except that it
    # attempts to tpc_abort() after the tpc_begin() fails.  That will raise a
    # ClientDisconnected exception which implies that we don't have the lock,
    # and that's what we really want to test (but it's difficult given the
    # threading module's API).
    def run(self):
        try:
            self.storage.tpc_begin(self.trans)
        except ZEO.ClientStorage.ClientStorageError:
            self.gotValueError = 1
        try:
            self.storage.tpc_abort(self.trans)
        except Disconnected:
            self.gotDisconnected = 1


class ThreadTests:
    # Thread 1 should start a transaction, but not get all the way through it.
    # Main thread should close the connection.  Thread 1 should then get
    # disconnected.
    def checkDisconnectedOnThread2Close(self):
        doNextEvent = threading.Event()
        threadStartedEvent = threading.Event()
        thread1 = GetsThroughVoteThread(self._storage,
                                        doNextEvent, threadStartedEvent)
        thread1.start()
        threadStartedEvent.wait(10)
        self._storage.close()
        doNextEvent.set()
        thread1.join()
        self.assertEqual(thread1.gotValueError, 1)

    # Thread 1 should start a transaction, but not get all the way through
    # it.  While thread 1 is in the middle of the transaction, a second thread
    # should start a transaction, and it will block in the tcp_begin() --
    # because thread 1 has acquired the lock in its tpc_begin().  Now the main
    # thread closes the storage and both sub-threads should get disconnected.
    def checkSecondBeginFails(self):
        doNextEvent = threading.Event()
        threadStartedEvent = threading.Event()
        thread1 = GetsThroughVoteThread(self._storage,
                                        doNextEvent, threadStartedEvent)
        thread2 = GetsThroughBeginThread(self._storage,
                                         doNextEvent, threadStartedEvent)
        thread1.start()
        threadStartedEvent.wait(1)
        thread2.start()
        self._storage.close()
        doNextEvent.set()
        thread1.join()
        thread2.join()
        self.assertEqual(thread1.gotValueError, 1)
        self.assertEqual(thread2.gotValueError, 1)

    def checkThatFailedBeginDoesNotHaveLock(self):
        doNextEvent = threading.Event()
        threadStartedEvent = threading.Event()
        thread1 = GetsThroughVoteThread(self._storage,
                                        doNextEvent, threadStartedEvent)
        thread2 = AbortsAfterBeginFailsThread(self._storage,
                                              doNextEvent, threadStartedEvent)
        thread1.start()
        threadStartedEvent.wait(1)
        thread2.start()
        self._storage.close()
        doNextEvent.set()
        thread1.join()
        thread2.join()
        self.assertEqual(thread1.gotValueError, 1)
        self.assertEqual(thread2.gotValueError, 1)
        self.assertEqual(thread2.gotDisconnected, 1)
