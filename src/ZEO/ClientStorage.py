######################################################################
# Digital Creations Options License Version 0.9.0
# -----------------------------------------------
# 
# Copyright (c) 1999, Digital Creations.  All rights reserved.
# 
# This license covers Zope software delivered as "options" by Digital
# Creations.
# 
# Use in source and binary forms, with or without modification, are
# permitted provided that the following conditions are met:
# 
# 1. Redistributions are not permitted in any form.
# 
# 2. This license permits one copy of software to be used by up to five
#    developers in a single company. Use by more than five developers
#    requires additional licenses.
# 
# 3. Software may be used to operate any type of website, including
#    publicly accessible ones.
# 
# 4. Software is not fully documented, and the customer acknowledges
#    that the product can best be utilized by reading the source code.
# 
# 5. Support for software is included for 90 days in email only. Further
#    support can be purchased separately.
# 
# Disclaimer
# 
#   THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND ANY
#   EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
#   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL CREATIONS OR ITS
#   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
#   USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
#   OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
#   SUCH DAMAGE.
######################################################################
"""Network ZODB storage client
"""
__version__='$Revision: 1.6 $'[11:-2]

import struct, time, os, socket, cPickle, string, Sync, zrpc, ClientCache
import tempfile
now=time.time
from struct import pack, unpack
from ZODB import POSException, BaseStorage
from ZODB.TimeStamp import TimeStamp

TupleType=type(())

class UnrecognizedResult(POSException.StorageError):
    """A server call returned an unrecognized result
    """

class ClientStorage(BaseStorage.BaseStorage):

    def __init__(self, connection, async=0, storage='1', cache_size=20000000,
                 name=''):

        # Decide whether to use non-temporary files
        client=os.environ.get('ZEO_CLIENT','')
        if client: async=1
        
        if async:
            import asyncore
            def loop(timeout=30.0, use_poll=0,
                     self=self, asyncore=asyncore, loop=asyncore.loop):
                self.becomeAsync()
                asyncore.loop=loop
                loop(timeout, use_poll)
            asyncore.loop=loop

        self._call=zrpc.syncRPC(connection)
        self.__begin='tpc_begin_sync'

        self._call._write(str(storage))
        info=self._call('get_info')
        self._len=info.get('length',0)
        self._size=info.get('size',0)
        name=name or ("%s %s" % (info.get('name', ''), str(connection)))
        self._supportsUndo=info.get('supportsUndo',0)
        self._supportsVersions=info.get('supportsVersions',0)


        self._tfile=tempfile.TemporaryFile()
        

        self._cache=ClientCache.ClientCache(storage, cache_size, client=client)
        if async:
            for oid, (s, vs) in self._cache.open():
                self._call.queue('zeoVerify', oid, s, vs)
        else:
            for oid, (s, vs) in self._cache.open():
                self._call.send('zeoVerify', oid, s, vs)

        BaseStorage.BaseStorage.__init__(self, name)

    def becomeAsync(self):
        self._call=zrpc.asyncRPC(self._call)
        self.__begin='tpc_begin'

    def registerDB(self, db, limit):

        def invalidate(code, args,
                       dinvalidate=db.invalidate,
                       limit=limit,
                       release=self._commit_lock_release,
                       cinvalidate=self._cache.invalidate
                       ):
            if code == 'I':
                for oid, serial, version in args:
                    cinvalidate(oid, version=version)
                    dinvalidate(oid, version=version)
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
        try:
            p = self._cache.load(oid, version)
            if p is not None: return p
            p, s, v, pv, sv = self._call('zeoLoad', oid)
            self._cache.store(oid, p, s, v, pv, sv)
            if not v or not version or version != v:
                return p, s
            return pv, sv
        finally: self._lock_release()
                    
    def modifiedInVersion(self, oid):
        self._lock_acquire()
        try:
            v=self._cache.modifiedInVersion(oid)
            if v is not None: return v
            return self._call('modifiedInVersion', oid)
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
        try:
            serial=self._call('store', oid, serial,
                              data, version, self._serial)
            
            write=self._tfile.write
            write(oid+serial+pack(">HI", len(version), len(data))+version)
            write(data)

            return serial
        
        finally: self._lock_release()

    def supportsUndo(self): return self._supportsUndo
    def supportsVersions(self): return self._supportsVersions
        
    def tpc_abort(self, transaction):
        self._lock_acquire()
        try:
            if transaction is not self._transaction: return
            self._call('tpc_abort', self._serial)
            self._transaction=None
            self._tfile.seek(0)
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

            self._tfile.seek(0)

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

            tfile=self._tfile
            seek=tfile.seek
            read=tfile.read
            cache=self._cache
            size=tfile.tell()
            seek(0)
            i=0
            while i < size:
                oid=read(8)
                s=read(8)
                h=read(6)
                vlen, dlen = unpack(">HI", h)
                if vlen: v=read(vlen)
                else: v=''
                p=read(dlen)
                cache.update(oid, s, v, p)
                i=i+22+vlen+dlen

            seek(0)

            self._transaction=None
            self._commit_lock_release()
        finally: self._lock_release()

    def undo(self, transaction_id):
        self._lock_acquire()
        try:
            oids=self._call('undo', transaction_id)
            cinvalidate=self._cache.invalidate
            for oid in oids: cinvalidate(oid,'')                
            return oids
        finally: self._lock_release()


    def undoInfo(self, first, last, specification):
        self._lock_acquire()
        try:
            return self._call('undoInfo', first, last, specification)
        finally: self._lock_release()

    def undoLog(self, first, last, filter=None):
        if filter is not None: return ()
        
        self._lock_acquire()
        try: return self._call('undoLog', first, last) # Eek!
        finally: self._lock_release()

    def versionEmpty(self, version):
        self._lock_acquire()
        try: return self._call('versionEmpty', version)
        finally: self._lock_release()

    def versions(self, max=None):
        self._lock_acquire()
        try: return self._call('versionEmpty', max)
        finally: self._lock_release()

