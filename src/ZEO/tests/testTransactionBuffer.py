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
import random
import unittest

from ZEO.TransactionBuffer import TransactionBuffer

def random_string(size):
    """Return a random string of size size."""
    l = [chr(random.randrange(256)) for i in range(size)]
    return "".join(l)

def new_store_data():
    """Return arbitrary data to use as argument to store() method."""
    return random_string(8), '', random_string(random.randrange(1000))

def new_invalidate_data():
    """Return arbitrary data to use as argument to invalidate() method."""
    return random_string(8), ''

class TransBufTests(unittest.TestCase):

    def checkTypicalUsage(self):
        tbuf = TransactionBuffer()
        tbuf.store(*new_store_data())
        tbuf.invalidate(*new_invalidate_data())
        tbuf.begin_iterate()
        while 1:
            o = tbuf.next()
            if o is None:
                break
        tbuf.clear()

    def doUpdates(self, tbuf):
        data = []
        for i in range(10):
            d = new_store_data()
            tbuf.store(*d)
            data.append(d)
            d = new_invalidate_data()
            tbuf.invalidate(*d)
            data.append(d)

        tbuf.begin_iterate()
        for i in range(len(data)):
            x = tbuf.next()
            if x[2] is None:
                # the tbuf add a dummy None to invalidates
                x = x[:2]
            self.assertEqual(x, data[i])

    def checkOrderPreserved(self):
        tbuf = TransactionBuffer()
        self.doUpdates(tbuf)

    def checkReusable(self):
        tbuf = TransactionBuffer()
        self.doUpdates(tbuf)
        tbuf.clear()
        self.doUpdates(tbuf)
        tbuf.clear()
        self.doUpdates(tbuf)

def test_suite():
    return unittest.makeSuite(TransBufTests, 'check')
