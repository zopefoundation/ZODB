##############################################################################
# 
# Zope Public License (ZPL) Version 1.0
# -------------------------------------
# 
# Copyright (c) Digital Creations.  All rights reserved.
# 
# This license has been certified as Open Source(tm).
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
# 1. Redistributions in source code must retain the above copyright
#    notice, this list of conditions, and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions, and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
# 
# 3. Digital Creations requests that attribution be given to Zope
#    in any manner possible. Zope includes a "Powered by Zope"
#    button that is installed by default. While it is not a license
#    violation to remove this button, it is requested that the
#    attribution remain. A significant investment has been put
#    into Zope, and this effort will continue if the Zope community
#    continues to grow. This is one way to assure that growth.
# 
# 4. All advertising materials and documentation mentioning
#    features derived from or use of this software must display
#    the following acknowledgement:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    In the event that the product being advertised includes an
#    intact Zope distribution (with copyright and license included)
#    then this clause is waived.
# 
# 5. Names associated with Zope or Digital Creations must not be used to
#    endorse or promote products derived from this software without
#    prior written permission from Digital Creations.
# 
# 6. Modified redistributions of any form whatsoever must retain
#    the following acknowledgment:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    Intact (re-)distributions of any official Zope release do not
#    require an external acknowledgement.
# 
# 7. Modifications are encouraged but must be packaged separately as
#    patches to official Zope releases.  Distributions that do not
#    clearly separate the patches from the original work must be clearly
#    labeled as unofficial distributions.  Modifications which do not
#    carry the name Zope may be packaged in any form, as long as they
#    conform to all of the clauses above.
# 
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
# 
# 
# This software consists of contributions made by Digital Creations and
# many individuals on behalf of Digital Creations.  Specific
# attributions are listed in the accompanying credits file.
# 
##############################################################################
"""Network ZODB storage client
"""
__version__='$Revision: 1.15 $'[11:-2]

import struct, time, os, socket, string, Sync, zrpc, ClientCache
import tempfile, Invalidator, ExtensionClass, thread
import ThreadedAsync

now=time.time
from struct import pack, unpack
from ZODB import POSException, BaseStorage
from ZODB.TimeStamp import TimeStamp
from zLOG import LOG, PROBLEM, INFO

TupleType=type(())

class ClientStorageError(POSException.StorageError):
    """An error occured in the ZEO Client Storage"""

class UnrecognizedResult(ClientStorageError):
    """A server call returned an unrecognized result
    """

class ClientDisconnected(ClientStorageError):
    """The database storage is disconnected from the storage.
    """
    

class ClientStorage(ExtensionClass.Base, BaseStorage.BaseStorage):

    _connected=_async=0
    __begin='tpc_begin_sync'

    def __init__(self, connection, async=0, storage='1', cache_size=20000000,
                 name='', client='', debug=0, var=None):

        # Decide whether to use non-temporary files
        client=client or os.environ.get('ZEO_CLIENT','')

        self._connection=connection
        self._storage=storage
        self._debug=debug

        self._info={'length': 0, 'size': 0, 'name': 'ZEO Client',
                    'supportsUndo':0, 'supportsVersions': 0,
                    }
        
        self._call=zrpc.asyncRPC(connection, debug=debug)

        name = name or str(connection)

        self._tfile=tempfile.TemporaryFile()
        self._oids=[]
        self._serials=[]
        self._seriald={}

        ClientStorage.inheritedAttribute('__init__')(self, name)

        self.__lock_acquire=self._lock_acquire

        self._cache=ClientCache.ClientCache(
            storage, cache_size, client=client, var=var)


        ThreadedAsync.register_loop_callback(self.becomeAsync)

    def _startup(self):

        if not self._call.connect():
            # If we can't connect right away, go ahead and open the cache
            # and start a separate thread to try and reconnect.
            LOG("ClientStorage", PROBLEM, "Failed to connect to storage")
            self._cache.open()
            thread.start_new_thread(self._call.connect,(0,))

    def notifyConnected(self, s):
        LOG("ClientStorage", INFO, "Connected to storage")
        self._lock_acquire()
        try:
            # We let the connection keep coming up now that
            # we have the storage lock. This way, we know no calls
            # will be made while in the process of coming up.
            self._call.finishConnect(s)

            self._connected=1
            self._oids=[]
            self.__begin='tpc_begin_sync'
            
            self._call.message_output(str(self._storage))
            self._info.update(self._call('get_info'))

            cached=self._cache.open()
            if cached:
                self._call.sendMessage('beginZeoVerify')
                for oid, (s, vs) in cached:
                    self._call.sendMessage('zeoVerify', oid, s, vs)
                self._call.sendMessage('endZeoVerify')

        finally: self._lock_release()

        if self._async:
            import ZServer.medusa.asyncore
            self.becomeAsync(ZServer.medusa.asyncore.socket_map)

    def notifyDisconnected(self, ignored):
        LOG("ClientStorage", PROBLEM, "Disconnected from storage")
        self._connected=0
        self._transaction=None
        thread.start_new_thread(self._call.connect,(0,))
        try: self._commit_lock_release()
        except: pass

    def becomeAsync(self, map):
        self._lock_acquire()
        try:
            self._async=1
            if self._connected:
                import ZServer.PubCore.ZEvent

                self._call.setLoop(map,
                                   ZServer.PubCore.ZEvent.Wakeup)
                self.__begin='tpc_begin'
        finally: self._lock_release()

    def registerDB(self, db, limit):

        invalidator=Invalidator.Invalidator(
            db.invalidate,
            self._cache.invalidate)

        def out_of_band_hook(
            code, args,
            get_hook={
                'b': (invalidator.begin, 0),
                'i': (invalidator.invalidate, 1),
                'e': (invalidator.end, 0),
                'I': (invalidator.Invalidate, 1),
                'U': (self._commit_lock_release, 0),
                's': (self._serials.append, 1),
                'S': (self._info.update, 1),
                }.get):

            hook = get_hook(code, None)
            if hook is None: return
            hook, flag = hook
            if flag: hook(args)
            else: hook()

        self._call.setOutOfBand(out_of_band_hook)

        self._startup()

    def __len__(self): return self._info['length']

    def abortVersion(self, src, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        self._lock_acquire()
        try:
            oids=self._call('abortVersion', src, self._serial)
            invalidate=self._cache.invalidate
            for oid in oids: invalidate(oid, src)
            return oids
        finally: self._lock_release()

    def close(self):
        self._lock_acquire()
        try: self._call.closeIntensionally()
        finally: self._lock_release()
        
    def commitVersion(self, src, dest, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        self._lock_acquire()
        try:
            oids=self._call('commitVersion', src, dest, self._serial)
            invalidate=self._cache.invalidate
            if dest:
                # just invalidate our version data
                for oid in oids: invalidate(oid, src)
            else:
                # dest is '', so invalidate version and non-version
                for oid in oids: invalidate(oid, dest)
                
            return oids
        finally: self._lock_release()

    def getName(self):
        return "%s (%s)" % (
            self.__name__,
            self._connected and 'connected' or 'disconnected')

    def getSize(self): return self._info['size']
                  
    def history(self, oid, version, length=1):
        self._lock_acquire()
        try: return self._call('history', oid, version, length)     
        finally: self._lock_release()       
                  
    def loadSerial(self, oid, serial):
        self._lock_acquire()
        try: return self._call('loadSerial', oid, serial)     
        finally: self._lock_release()       

    def load(self, oid, version, _stuff=None):
        self._lock_acquire()
        try:
            cache=self._cache
            p = cache.load(oid, version)
            if p: return p
            p, s, v, pv, sv = self._call('zeoLoad', oid)
            cache.checkSize(size)
            cache.store(oid, p, s, v, pv, sv)
            if not v or not version or version != v:
                if s: return p, s
                raise KeyError, oid # no non-version data for this
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
        try:
            oids=self._oids
            if not oids:
                oids[:]=self._call('new_oids')
                oids.reverse()
                
            return oids.pop()
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
            serial=self._call.sendMessage('storea', oid, serial,
                                          data, version, self._serial)
            
            write=self._tfile.write
            write(oid+pack(">HI", len(version), len(data))+version)
            write(data)

            if self._serials:
                s=self._serials
                l=len(s)
                r=s[:l]
                del s[:l]
                d=self._seriald
                for oid, s in r: d[oid]=s
                return r

            return serial
        
        finally: self._lock_release()

    def tpc_vote(self, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        self._lock_acquire()
        try:
            self._call('vote', self._serial)

            if self._serials:
                s=self._serials
                l=len(s)
                r=s[:l]
                del s[:l]
                d=self._seriald
                for oid, s in r: d[oid]=s
                return r
        
        finally: self._lock_release()
            
        

    def supportsUndo(self): return self._info['supportsUndo']
    def supportsVersions(self): return self._info['supportsVersions']
        
    def tpc_abort(self, transaction):
        self._lock_acquire()
        try:
            if transaction is not self._transaction: return
            self._call('tpc_abort', self._serial)
            self._transaction=None
            self._tfile.seek(0)
            self._seriald.clear()
            del self._serials[:]
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
            self._seriald.clear()
            del self._serials[:]

            while 1:
                self._lock_release()
                self._commit_lock_acquire()
                self._lock_acquire()
                if not self._connected: raise ClientDisconnected()
                try: r=self._call(self.__begin, id, user, desc, ext)
                except:
                    self._commit_lock_release()
                    raise
                
                if r is None: break

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

            seriald=self._seriald
            if self._serials:
                s=self._serials
                l=len(s)
                r=s[:l]
                del s[:l]
                for oid, s in r: seriald[oid]=s

            tfile=self._tfile
            seek=tfile.seek
            read=tfile.read
            cache=self._cache
            size=tfile.tell()
            cache.checkSize(size)
            seek(0)
            i=0
            while i < size:
                oid=read(8)
                s=seriald[oid]
                h=read(6)
                vlen, dlen = unpack(">HI", h)
                if vlen: v=read(vlen)
                else: v=''
                p=read(dlen)
                if len(p) != dlen:
                    raise ClientStorageError, (
                        "Unexpected end of file in client storage "
                        "temporary file."
                        )
                cache.update(oid, s, v, p)
                i=i+14+vlen+dlen

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
        try: return self._call('versions', max)
        finally: self._lock_release()
