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
"""Network ZODB storage client
"""

__version__='$Revision: 1.38 $'[11:-2]

import struct, time, os, socket, string, Sync, zrpc, ClientCache
import tempfile, Invalidator, ExtensionClass, thread
import ThreadedAsync

now=time.time
from struct import pack, unpack
from ZODB import POSException, BaseStorage
from ZODB.TimeStamp import TimeStamp
from zLOG import LOG, PROBLEM, INFO

try: from ZODB.ConflictResolution import ResolvedSerial
except: ResolvedSerial='rs'

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

    def __init__(self, connection, storage='1', cache_size=20000000,
                 name='', client='', debug=0, var=None,
                 min_disconnect_poll=5, max_disconnect_poll=300,
                 wait_for_server_on_startup=1):

        # Decide whether to use non-temporary files
        client=client or os.environ.get('ZEO_CLIENT','')

        self._connection=connection
        self._storage=storage
        self._debug=debug
        self._wait_for_server_on_startup=wait_for_server_on_startup

        self._info={'length': 0, 'size': 0, 'name': 'ZEO Client',
                    'supportsUndo':0, 'supportsVersions': 0,
                    }
        
        self._call=zrpc.asyncRPC(connection, debug=debug,
                                 tmin=min_disconnect_poll,
                                 tmax=max_disconnect_poll)

        name = name or str(connection)

        self.closed = 0
        self._tfile=tempfile.TemporaryFile()
        self._oids=[]
        self._serials=[]
        self._seriald={}

        ClientStorage.inheritedAttribute('__init__')(self, name)

        self.__lock_acquire=self._lock_acquire

        self._cache=ClientCache.ClientCache(
            storage, cache_size, client=client, var=var)


        ThreadedAsync.register_loop_callback(self.becomeAsync)

        # IMPORTANT: Note that we aren't fully "there" yet.
        # In particular, we don't actually connect to the server
        # until we have a controlling database set with registerDB
        # below.

    def registerDB(self, db, limit):
        """Register that the storage is controlled by the given DB.
        """
        
        # Among other things, we know that our data methods won't get
        # called until after this call.

        self.invalidator = Invalidator.Invalidator(db.invalidate,
                                                   self._cache.invalidate)

        def out_of_band_hook(
            code, args,
            get_hook={
                'b': (self.invalidator.begin, 0),
                'i': (self.invalidator.invalidate, 1),
                'e': (self.invalidator.end, 0),
                'I': (self.invalidator.Invalidate, 1),
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

        # Now that we have our callback system in place, we can
        # try to connect

        self._startup()

    def _startup(self):

        if not self._call.connect(not self._wait_for_server_on_startup):

            # If we can't connect right away, go ahead and open the cache
            # and start a separate thread to try and reconnect.

            LOG("ClientStorage", PROBLEM, "Failed to connect to storage")
            self._cache.open()
            thread.start_new_thread(self._call.connect,(0,))

            # If the connect succeeds then this work will be done by
            # notifyConnected

    def notifyConnected(self, s):
        LOG("ClientStorage", INFO, "Connected to storage")
        self._lock_acquire()
        try:
            
            # We let the connection keep coming up now that
            # we have the storage lock. This way, we know no calls
            # will be made while in the process of coming up.

            self._call.finishConnect(s)

            if self.closed:
                return

            self._connected=1
            self._oids=[]

            # we do synchronous commits until we are sure that
            # we have and are ready for a main loop.

            # Hm. This is a little silly. If self._async, then
            # we will really never do a synchronous commit.
            # See below.
            self.__begin='tpc_begin_sync'
            
            self._call.message_output(str(self._storage))

            ### This seems silly. We should get the info asynchronously.
            # self._info.update(self._call('get_info'))

            cached=self._cache.open()
            ### This is a little expensive for large caches
            if cached:
                self._call.sendMessage('beginZeoVerify')
                for oid, (s, vs) in cached:
                    self._call.sendMessage('zeoVerify', oid, s, vs)
                self._call.sendMessage('endZeoVerify')

        finally: self._lock_release()

        if self._async:
            import asyncore
            self.becomeAsync(asyncore.socket_map)


    ### Is there a race condition between notifyConnected and
    ### notifyDisconnected? In Particular, what if we get
    ### notifyDisconnected in the middle of notifyConnected?
    ### The danger is that we'll proceed as if we were connected
    ### without worrying if we were, but this would happen any way if
    ### notifyDisconnected had to get the instance lock.  There's
    ### nothing to gain by getting the instance lock.

    ### Note that we *don't* have to worry about getting connected
    ### in the middle of notifyDisconnected, because *it's*
    ### responsible for starting the thread that makes the connection.

    def notifyDisconnected(self, ignored):
        LOG("ClientStorage", PROBLEM, "Disconnected from storage")
        self._connected=0
        self._transaction=None
        thread.start_new_thread(self._call.connect,(0,))
        if self._transaction is not None:
            try:
                self._commit_lock_release()
            except:
                pass

    def becomeAsync(self, map):
        self._lock_acquire()
        try:
            self._async=1
            if self._connected:
                self._call.setLoop(map, getWakeup())
                self.__begin='tpc_begin'
        finally: self._lock_release()

    def __len__(self): return self._info['length']

    def abortVersion(self, src, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        self._lock_acquire()
        try:
            oids=self._call('abortVersion', src, self._serial)
            vlen = pack(">H", len(src))
            for oid in oids:
                self._tfile.write("i%s%s%s" % (oid, vlen, src))
            return oids
        finally: self._lock_release()

    def close(self):
        self._lock_acquire()
        try:
            LOG("ClientStorage", INFO, "close")
            self._call.closeIntensionally()
            try:
                self._tfile.close()
            except os.error:
                # On Windows, this can fail if it is called more than
                # once, because it tries to delete the file each
                # time.
                pass
            self._cache.close()
            if self.invalidator is not None:
                self.invalidator.close()
                self.invalidator = None
            self.closed = 1
        finally: self._lock_release()
        
    def commitVersion(self, src, dest, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        self._lock_acquire()
        try:
            oids=self._call('commitVersion', src, dest, self._serial)
            if dest:
                vlen = pack(">H", len(src))
                # just invalidate our version data
                for oid in oids:
                    self._tfile.write("i%s%s%s" % (oid, vlen, src))
            else:
                vlen = pack(">H", len(dest))
                # dest is '', so invalidate version and non-version
                for oid in oids:
                    self._tfile.write("i%s%s%s" % (oid, vlen, dest))
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
            cache.checkSize(0)
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
        
    def pack(self, t=None, rf=None, wait=0, days=0):
        # Note that we ignore the rf argument.  The server
        # will provide it's own implementation.
        if t is None: t=time.time()
        t=t-(days*86400)
        self._lock_acquire()
        try: return self._call('pack', t, wait)
        finally: self._lock_release()

    def store(self, oid, serial, data, version, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        self._lock_acquire()
        try:
            serial=self._call.sendMessage('storea', oid, serial,
                                          data, version, self._serial)
            
            write=self._tfile.write
            buf = string.join(("s", oid,
                               pack(">HI", len(version), len(data)),
                               version, data), "")
            write(buf)

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
        self._lock_acquire()
        try:
            if transaction is not self._transaction:
                return
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
            
    def supportsUndo(self):
        return self._info['supportsUndo']
    
    def supportsVersions(self):
        return self._info['supportsVersions']
    
    def supportsTransactionalUndo(self):
        try:
            return self._info['supportsTransactionalUndo']
        except KeyError:
            return 0
        
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

            while 1:
                self._lock_release()
                self._commit_lock_acquire()
                self._lock_acquire()

                # We've got the local commit lock. Now get
                # a (tentative) transaction time stamp.
                t=time.time()
                t=apply(TimeStamp,(time.gmtime(t)[:5]+(t%60,)))
                self._ts=t=t.laterThan(self._ts)
                id=`t`
                
                try:
                    if not self._connected:
                        raise ClientDisconnected(
                            "This action is temporarily unavailable.<p>")
                    r=self._call(self.__begin, id, user, desc, ext)
                except:
                    # XXX can't seem to guarantee that the lock is held here. 
                    self._commit_lock_release()
                    raise
                
                if r is None: break

            # We have *BOTH* the local and distributed commit
            # lock, now we can actually get ready to get started.
            self._serial=id
            self._tfile.seek(0)
            self._seriald.clear()
            del self._serials[:]

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
                opcode=read(1)
                if opcode == "s":
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
                    if s==ResolvedSerial:
                        self._cache.invalidate(oid, v)
                    else:
                        self._cache.update(oid, s, v, p)
                    i=i+15+vlen+dlen
                elif opcode == "i":
                    oid=read(8)
                    h=read(2)
                    vlen=unpack(">H", h)[0]
                    v=read(vlen)
                    self._cache.invalidate(oid, v)
                    i=i+11+vlen

            seek(0)

            self._transaction=None
            self._commit_lock_release()
        finally: self._lock_release()

    def transactionalUndo(self, trans_id, trans):
        self._lock_acquire()
        try:
            if trans is not self._transaction:
                raise POSException.StorageTransactionError(self, transaction)
            oids = self._call('transactionalUndo', trans_id, self._serial)
            for oid in oids:
                # write invalidation records with no version
                self._tfile.write("i%s\000\000" % oid)
            return oids
        finally: self._lock_release()

    def undo(self, transaction_id):
        self._lock_acquire()
        try:
            oids=self._call('undo', transaction_id)
            cinvalidate=self._cache.invalidate
            for oid in oids:
                cinvalidate(oid,'')                
            return oids
        finally: self._lock_release()


    def undoInfo(self, first=0, last=-20, specification=None):
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

    def sync(self): self._call.sync()

def getWakeup(_w=[]):
    if _w: return _w[0]
    import trigger
    t=trigger.trigger().pull_trigger
    _w.append(t)
    return t
