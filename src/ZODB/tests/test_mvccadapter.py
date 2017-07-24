##############################################################################
#
# Copyright (c) 2017 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

import unittest

from ZODB import mvccadapter


class TestBase(unittest.TestCase):

    def test_getattr_does_not_hide_exceptions(self):
        class TheException(Exception):
            pass

        class RaisesOnAccess(object):

            @property
            def thing(self):
                raise TheException()

        base = mvccadapter.Base(RaisesOnAccess())
        base._copy_methods = ('thing',)

        with self.assertRaises(TheException):
            getattr(base, 'thing')

    def test_getattr_raises_if_missing(self):
        base = mvccadapter.Base(self)
        base._copy_methods = ('thing',)

        with self.assertRaises(AttributeError):
            getattr(base, 'thing')


class TestHistoricalStorageAdapter(unittest.TestCase):

    def test_forwards_release(self):
        class Base(object):
            released = False

            def release(self):
                self.released = True

        base = Base()
        adapter = mvccadapter.HistoricalStorageAdapter(base, None)

        adapter.release()

        self.assertTrue(base.released)
