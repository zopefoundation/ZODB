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
"""Stub for interface exposed by StorageServer"""

class StorageServer:

    def __init__(self, rpc):
        self.rpc = rpc

    def register(self, storage_name, read_only):
        self.rpc.call('register', storage_name, read_only)

    def get_info(self):
        return self.rpc.call('get_info')

    def get_size_info(self):
        return self.rpc.call('get_size_info')

    def beginZeoVerify(self):
        self.rpc.callAsync('beginZeoVerify')

    def zeoVerify(self, oid, s, sv):
        self.rpc.callAsync('zeoVerify', oid, s, sv)

    def endZeoVerify(self):
        self.rpc.callAsync('endZeoVerify')

    def new_oids(self, n=None):
        if n is None:
            return self.rpc.call('new_oids')
        else:
            return self.rpc.call('new_oids', n)

    def pack(self, t, wait=None):
        if wait is None:
            self.rpc.call('pack', t)
        else:
            self.rpc.call('pack', t, wait)

    def zeoLoad(self, oid):
        return self.rpc.call('zeoLoad', oid)

    def storea(self, oid, serial, data, version, id):
        self.rpc.callAsync('storea', oid, serial, data, version, id)

    def tpc_begin(self, id, user, descr, ext, tid, status):
        return self.rpc.call('tpc_begin', id, user, descr, ext, tid, status)

    def vote(self, trans_id):
        return self.rpc.call('vote', trans_id)

    def tpc_finish(self, id):
        return self.rpc.call('tpc_finish', id)

    def tpc_abort(self, id):
        self.rpc.callAsync('tpc_abort', id)

    def abortVersion(self, src, id):
        return self.rpc.call('abortVersion', src, id)

    def commitVersion(self, src, dest, id):
        return self.rpc.call('commitVersion', src, dest, id)

    def history(self, oid, version, length=None):
        if length is not None:
            return self.rpc.call('history', oid, version)
        else:
            return self.rpc.call('history', oid, version, length)

    def load(self, oid, version):
        return self.rpc.call('load', oid, version)

    def loadSerial(self, oid, serial):
        return self.rpc.call('loadSerial', oid, serial)

    def modifiedInVersion(self, oid):
        return self.rpc.call('modifiedInVersion', oid)

    def new_oid(self, last=None):
        if last is None:
            return self.rpc.call('new_oid')
        else:
            return self.rpc.call('new_oid', last)

    def store(self, oid, serial, data, version, trans):
        return self.rpc.call('store', oid, serial, data, version, trans)

    def transactionalUndo(self, trans_id, trans):
        return self.rpc.call('transactionalUndo', trans_id, trans)

    def undo(self, trans_id):
        return self.rpc.call('undo', trans_id)

    def undoLog(self, first, last):
        # XXX filter not allowed across RPC
        return self.rpc.call('undoLog', first, last)

    def undoInfo(self, first, last, spec):
        return self.rpc.call('undoInfo', first, last, spec)

    def versionEmpty(self, vers):
        return self.rpc.call('versionEmpty', vers)

    def versions(self, max=None):
        if max is None:
            return self.rpc.call('versions')
        else:
            return self.rpc.call('versions', max)
