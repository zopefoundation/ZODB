##############################################################################
#
# Copyright (c) Zope Foundation and Contributors.
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
import unittest
import warnings

from .._compat import dumps
from .._compat import loads
from ..Connection import TransactionMetaData


class TransactionMetaDataTests(unittest.TestCase):

    def test_basic(self):
        extension = dict(foo='FOO')
        t = TransactionMetaData(u'user\x80', u'description\x80', extension)
        self.assertEqual(t.user, b'user\xc2\x80')
        self.assertEqual(t.description, b'description\xc2\x80')
        self.assertEqual(t.extension, extension)
        self.assertEqual(loads(t.extension_bytes), extension)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.assertEqual(t._extension, t.extension)
            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[-1].category, DeprecationWarning))
            self.assertTrue("_extension is deprecated" in str(w[-1].message))

    def test_basic_no_encoding(self):
        extension = dict(foo='FOO')
        extension_bytes = dumps(extension)
        t = TransactionMetaData(b'user', b'description', extension_bytes)
        self.assertEqual(t.user, b'user')
        self.assertEqual(t.description, b'description')
        self.assertEqual(t.extension, extension)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.assertEqual(t._extension, t.extension)
        self.assertIs(t.extension_bytes, extension_bytes)

    def test_constructor_default_args(self):
        t = TransactionMetaData()
        self.assertEqual(t.user, b'')
        self.assertEqual(t.description, b'')
        self.assertEqual(t.extension, {})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.assertEqual(t._extension, t.extension)

    def test_set_extension(self):
        data = {}
        t = TransactionMetaData(u'', u'', data)
        self.assertEqual(t.user, b'')
        self.assertEqual(t.description, b'')
        self.assertIs(t.extension, data)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.assertEqual(t._extension, t.extension)
            self.assertEqual(t.extension_bytes, b'')

            for name in 'extension', '_extension':
                data = {name: name + 'foo'}
                setattr(t, name, data)
                self.assertIs(t.extension, data)
                self.assertIs(t._extension, t.extension)
                extension_bytes = t.extension_bytes
                self.assertEqual(loads(extension_bytes), data)
                empty = {}
                setattr(t, name, empty)
                self.assertIs(t.extension, empty)
                self.assertIs(t._extension, t.extension)
                self.assertEqual(t.extension_bytes, b'')

    def test_used_by_connection(self):
        import ZODB
        from ZODB.MappingStorage import MappingStorage

        class Storage(MappingStorage):
            def tpc_begin(self, transaction):
                self.test_transaction = transaction
                return MappingStorage.tpc_begin(self, transaction)

        storage = Storage()
        conn = ZODB.connection(storage)
        with conn.transaction_manager as t:
            t.user = u'user\x80'
            t.description = u'description\x80'
            t.setExtendedInfo('foo', 'FOO')
            conn.root.x = 1

        t = storage.test_transaction
        self.assertEqual(t.__class__, TransactionMetaData)
        self.assertEqual(t.user, b'user\xc2\x80')
        self.assertEqual(t.description, b'description\xc2\x80')
        self.assertEqual(t.extension, dict(foo='FOO'))

    def test_data(self):
        t = TransactionMetaData()

        # Can't get data that wasn't set:
        with self.assertRaises(KeyError) as c:
            t.data(self)
        self.assertEqual(c.exception.args, (self,))

        data = dict(a=1)
        t.set_data(self, data)
        self.assertEqual(t.data(self), data)

        # Can't get something we haven't stored.
        with self.assertRaises(KeyError) as c:
            t.data(data)
        self.assertEqual(c.exception.args, (data,))


def test_suite():
    return unittest.makeSuite(TransactionMetaDataTests)
