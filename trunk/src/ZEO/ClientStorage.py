##############################################################################
#
# Copyright (c) 1996-1998, Digital Creations, Fredericksburg, VA, USA.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
#   o Redistributions of source code must retain the above copyright
#     notice, this list of conditions, and the disclaimer that follows.
# 
#   o Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions, and the following disclaimer in
#     the documentation and/or other materials provided with the
#     distribution.
# 
#   o Neither the name of Digital Creations nor the names of its
#     contributors may be used to endorse or promote products derived
#     from this software without specific prior written permission.
# 
# 
# THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS AND CONTRIBUTORS *AS IS*
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
# PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL
# CREATIONS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
# TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
# USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
# DAMAGE.
#
# 
# If you have questions regarding this software, contact:
#
#   Digital Creations, L.C.
#   910 Princess Ann Street
#   Fredericksburge, Virginia  22401
#
#   info@digicool.com
#
#   (540) 371-6909
#
##############################################################################
"""Network ZODB storage client
"""
__version__='$Revision: 1.2 $'[11:-2]

import struct, time, os, socket, cPickle, string, Sync, zrpc
now=time.time
from struct import pack, unpack
from ZODB import POSException, BaseStorage
from ZODB.TimeStamp import TimeStamp

TupleType=type(())

class UnrecognizedResult(POSException.StorageError):
    """A server call returned an unrecognized result
    """

class ClientStorage(BaseStorage.BaseStorage):

    def __init__(self, connection, async=0):

        if async:
            import asyncore
            def loop(timeout=30.0, use_poll=0,
                     self=self, asyncore=asyncore, loop=asyncore.loop):
                self.becomeAsync()
                asyncore.loop=loop
                loop(timeout, use_poll)
            asyncore.loop=loop

        self._call=zrpc.sync(connection)
        self.__begin='tpc_begin_sync'

        self._call._write('1')
        info=self._call('get_info')
        self._len=info.get('length',0)
        self._size=info.get('size',0)
        self.__name__=info.get('name', str(connection))
        self._supportsUndo=info.get('supportsUndo',0)
        self._supportsVersions=info.get('supportsVersions',0)

        BaseStorage.BaseStorage.__init__(self,
                                         info.get('name', str(connection)),
                                         )

    def becomeAsync(self):
        self._call=zrpc.async(self._call)
        self.__begin='tpc_begin'

    def registerDB(self, db, limit):

        def invalidate(code, args,
                       invalidate=db.invalidate,
                       limit=limit,
                       release=self._commit_lock_release,
                       ):
            if code == 'I':
                for oid, serial, version in args:
                    invalidate(oid, version=version)
            elif code == 'U':
                release()

        self._call.setOutOfBand(invalidate)
        

    def __len__(self): return self._len

    def abortVersion(self, src, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        self._lock_acquire()
        try: return self._call('abortVersion', src, self._serial)
        finally: self._lock_release()

    def close(self):
        self._lock_acquire()
        try: self._call.close()
        finally: self._lock_release()
        
    def commitVersion(self, src, dest, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        self._lock_acquire()
        try: return self._call('commitVersion', src, dest, self._serial)
        finally: self._lock_release()

    def getName(self): return self.__name__

    def getSize(self): return self._size
                  
    def history(self, oid, version, length=1):
        self._lock_acquire()
        try: return self._call('history', oid, version, length)     
        finally: self._lock_release()       

    def load(self, oid, version, _stuff=None):
        self._lock_acquire()
        try: return self._call('load', oid, version)
        finally: self._lock_release()
                    
    def modifiedInVersion(self, oid):
        self._lock_acquire()
        try: return self._call('modifiedInVersion', oid)
        finally: self._lock_release()

    def new_oid(self, last=None):
        self._lock_acquire()
        try: return self._call('new_oid')
        finally: self._lock_release()
        
    def pack(self, t, rf):
        # Note that we ignore the rf argument.  The server
        # will provide it's own implementation.
        self._lock_acquire()
        try: return self._call('pack', t)
        finally: self._lock_release()

    def store(self, oid, serial, data, version, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        self._lock_acquire()
        try: return self._call('store', oid, serial,
                               data, version, self._serial)
        finally: self._lock_release()

    def supportsUndo(self): return self._supportsUndo
    def supportsVersions(self): return self._supportsVersions
        
    def tpc_abort(self, transaction):
        self._lock_acquire()
        try:
            if transaction is not self._transaction: return
            self._call('tpc_abort', self._serial)
            self._transaction=None
            self._commit_lock_release()
        finally: self._lock_release()

    def tpc_begin(self, transaction):
        self._lock_acquire()
        try:
            if self._transaction is transaction: return

            user=transaction.user
            desc=transaction.description
            ext=transaction._extension
            t=time.time()
            t=apply(TimeStamp,(time.gmtime(t)[:5]+(t%60,)))
            self._ts=t=t.laterThan(self._ts)
            self._serial=id=`t`

            while 1:
                self._lock_release()
                self._commit_lock_acquire()
                self._lock_acquire()
                if self._call(self.__begin, id, user, desc, ext) is None:
                    break

            self._transaction=transaction
            
        finally: self._lock_release()

    def tpc_finish(self, transaction, f=None):
        self._lock_acquire()
        try:
            if transaction is not self._transaction: return
            if f is not None: f()

            self._call('tpc_finish', self._serial,
                       transaction.user,
                       transaction.description,
                       transaction._extension)

            self._transaction=None
            self._commit_lock_release()
        finally: self._lock_release()

    def undo(self, transaction_id):
        self._lock_acquire()
        try: return self._call('undo', transaction_id)
        finally: self._lock_release()

    def undoLog(self, version, first, last, filter=None):
        # Waaaa, we really need to get the filter through
        # but how can we send it over the wire?

        # I suppose we could try to run the filter in a restricted execution
        # env.

        # Maybe .... we are really going to want to pass lambdas, hm.
        
        self._lock_acquire()
        try: return self._call('undoLog', version, first, last) # Eek!
        finally: self._lock_release()

    def versionEmpty(self, version):
        self._lock_acquire()
        try: return self._call('versionEmpty', version)
        finally: self._lock_release()

    def versions(self, max=None):
        self._lock_acquire()
        try: return self._call('versionEmpty', max)
        finally: self._lock_release()

