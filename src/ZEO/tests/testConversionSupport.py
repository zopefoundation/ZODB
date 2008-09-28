##############################################################################
#
# Copyright (c) 2006 Zope Corporation and Contributors.
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
from zope.testing import doctest

class FakeStorageBase:

    def __getattr__(self, name):
        if name in ('getTid', 'history', 'load', 'loadSerial',
                    'lastTransaction', 'getSize', 'getName', 'supportsUndo',
                    'tpc_transaction'):
           return lambda *a, **k: None
        raise AttributeError(name)

    def isReadOnly(self):
        return False

    def __len__(self):
        return 4

class FakeStorage(FakeStorageBase):

    def record_iternext(self, next=None):
        if next == None:
           next = '0'
        next = str(int(next) + 1)
        oid = next
        if next == '4':
            next = None

        return oid, oid*8, 'data ' + oid, next

class FakeServer:
    storages = {
        '1': FakeStorage(),
        '2': FakeStorageBase(),
        }

    def register_connection(*args):
        return None, None

def test_server_record_iternext():
    """
    
On the server, record_iternext calls are simply delegated to the
underlying storage.

    >>> import ZEO.StorageServer

    >>> zeo = ZEO.StorageServer.ZEOStorage(FakeServer(), False)
    >>> zeo.register('1', False)

    >>> next = None
    >>> while 1:
    ...     oid, serial, data, next = zeo.record_iternext(next)
    ...     print oid
    ...     if next is None:
    ...         break
    1
    2
    3
    4
    
The storage info also reflects the fact that record_iternext is supported.

    >>> zeo.get_info()['supports_record_iternext']
    True

    >>> zeo = ZEO.StorageServer.ZEOStorage(FakeServer(), False)
    >>> zeo.register('2', False)

    >>> zeo.get_info()['supports_record_iternext']
    False

"""


def test_client_record_iternext():
    """\

The client simply delegates record_iternext calls to it's server stub.

There's really no decent way to test ZEO without running to muc crazy
stuff.  I'd rather to a lame test than a really lame test, so here goes.

First, fake out the connection manager so we can make a connection:

    >>> import ZEO.ClientStorage
    >>> from ZEO.ClientStorage import ClientStorage
    >>> oldConnectionManagerClass = ClientStorage.ConnectionManagerClass
    >>> class FauxConnectionManagerClass:
    ...     def __init__(*a, **k):
    ...         pass
    ...     def attempt_connect(self):
    ...         return True
    >>> ClientStorage.ConnectionManagerClass = FauxConnectionManagerClass
    >>> client = ClientStorage('', wait=False)
    >>> ClientStorage.ConnectionManagerClass = oldConnectionManagerClass

Now we'll have our way with it's provate _server attr:

    >>> client._server = FakeStorage()
    >>> next = None
    >>> while 1:
    ...     oid, serial, data, next = client.record_iternext(next)
    ...     print oid
    ...     if next is None:
    ...         break
    1
    2
    3
    4

"""

def test_server_stub_record_iternext():
    """\

The server stub simply delegates record_iternext calls to it's rpc.

There's really no decent way to test ZEO without running to muc crazy
stuff.  I'd rather to a lame test than a really lame test, so here goes.

    >>> class FauxRPC:
    ...     storage = FakeStorage()
    ...     def call(self, meth, *args):
    ...         return getattr(self.storage, meth)(*args)
    ...     peer_protocol_version = 1

    >>> import ZEO.ServerStub
    >>> stub = ZEO.ServerStub.StorageServer(FauxRPC())
    >>> next = None
    >>> while 1:
    ...     oid, serial, data, next = stub.record_iternext(next)
    ...     print oid
    ...     if next is None:
    ...         break
    1
    2
    3
    4

"""
    
def test_suite():
    return doctest.DocTestSuite()

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

