##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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
"""Log a transaction's commit info during two-phase commit.

A storage server allows multiple clients to commit transactions, but
must serialize them as the actually execute at the server.  The
concurrent commits are achieved by logging actions up until the
tpc_vote().  At that point, the entire transaction is committed on the
real storage.

"""

import cPickle
import tempfile


class CommitLog:

    def __init__(self):
        self.file = tempfile.TemporaryFile(suffix=".comit-log")
        self.pickler = cPickle.Pickler(self.file, 1)
        self.pickler.fast = 1
        self.stores = 0

    def size(self):
        return self.file.tell()

    def delete(self, oid, serial):
        self.pickler.dump(('_delete', (oid, serial)))
        self.stores += 1

    def store(self, oid, serial, data):
        self.pickler.dump(('_store', (oid, serial, data)))
        self.stores += 1

    def restore(self, oid, serial, data, prev_txn):
        self.pickler.dump(('_restore', (oid, serial, data, prev_txn)))
        self.stores += 1

    def undo(self, transaction_id):
        self.pickler.dump(('_undo', (transaction_id, )))
        self.stores += 1

    def __iter__(self):
        self.file.seek(0)
        unpickler = cPickle.Unpickler(self.file)
        for i in range(self.stores):
            yield unpickler.load()

    def close(self):
        if self.file:
            self.file.close()
            self.file = None
