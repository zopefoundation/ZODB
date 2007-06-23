##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
"""Conventience function for creating test databases

$Id$
"""

import os
import shutil
import sys
import tempfile
import time

import persistent
import transaction
from ZODB.MappingStorage import MappingStorage
from ZODB.ConflictResolution import ConflictResolvingStorage
from ZODB.DB import DB as _DB
from ZODB import POSException

def DB(name='Test', **dbargs):
    return _DB(MappingStorage(name), **dbargs)

class ConflictResolvingMappingStorage(
    MappingStorage, ConflictResolvingStorage):

    def __init__(self, name='ConflictResolvingMappingStorage'):
        MappingStorage.__init__(self, name)
        self._old = {}

    def loadSerial(self, oid, serial):
        self._lock_acquire()
        try:
            old_info = self._old[oid]
            try:
                return old_info[serial]
            except KeyError:
                raise POSException.POSKeyError(oid)
        finally:
            self._lock_release()

    def store(self, oid, serial, data, version, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        if version:
            raise POSException.Unsupported("Versions aren't supported")

        self._lock_acquire()
        try:
            if oid in self._index:
                oserial = self._index[oid][:8]
                if serial != oserial:
                    rdata = self.tryToResolveConflict(
                        oid, oserial, serial, data)
                    if rdata is None:
                        raise POSException.ConflictError(
                            oid=oid, serials=(oserial, serial), data=data)
                    else:
                        data = rdata
            self._tindex[oid] = self._tid + data
        finally:
            self._lock_release()
        return self._tid

    def _finish(self, tid, user, desc, ext):
        self._index.update(self._tindex)
        self._ltid = self._tid
        for oid, record in self._tindex.items():
            self._old.setdefault(oid, {})[self._tid] = record[8:]

def commit():
    transaction.commit()

def pack(db):
    db.pack(time.time()+1)

class P(persistent.Persistent):

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return 'P(%s)' % self.name

def setUp(test):
    test.globs['__teardown_stack__'] = []
    tmp = tempfile.mkdtemp('test')
    registerTearDown(test, lambda : rmtree(tmp))
    here = os.getcwd()
    registerTearDown(test, lambda : os.chdir(here))
    os.chdir(tmp)

if sys.platform == 'win32':    
    # On windows, we can't remove a directory of there are files upen.
    # We may need to wait a while for processes to exit.
    def rmtree(path):
        for i in range(1000):
            try:
                shutil.rmtree(path)
            except OSError:
                time.sleep(0.01)
            else:
                break

else:
    rmtree = shutil.rmtree
            
def registerTearDown(test, func):
    test.globs['__teardown_stack__'].append(func)    
    
def tearDown(test):
    for f in test.globs['__teardown_stack__']:
        f()
