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
        self.file = tempfile.TemporaryFile(suffix=".log")
        self.pickler = cPickle.Pickler(self.file, 1)
        self.pickler.fast = 1
        self.stores = 0
        self.read = 0

    def store(self, oid, serial, data, version):
        self.pickler.dump((oid, serial, data, version))
        self.stores += 1

    def get_loader(self):
        self.read = 1
        self.file.seek(0)
        return self.stores, cPickle.Unpickler(self.file)
