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

__version__ = "$Revision: 1.35 $"[11:-2]

import asyncore, socket, string, sys, os
from smac import SizedMessageAsyncConnection
from ZODB import POSException
import cPickle
from cPickle import Unpickler
from ZODB.POSException import TransactionError, UndoError, VersionCommitError
from ZODB.Transaction import Transaction
import traceback
from zLOG import LOG, INFO, ERROR, TRACE, BLATHER
from ZODB.referencesf import referencesf
from thread import start_new_thread
from cStringIO import StringIO
from ZEO import trigger
from ZEO import asyncwrap
from ZEO.smac import Disconnected
from types import StringType

class StorageServerError(POSException.StorageError): pass

max_blather=120
def blather(*args):
    accum = []
    total_len = 0
    for arg in args:
        if not isinstance(arg, StringType):
            arg = str(arg)
        accum.append(arg)
        total_len = total_len + len(arg)
        if total_len >= max_blather:
            break
    m = string.join(accum)
    if len(m) > max_blather: m = m[:max_blather] + ' ...'
    LOG('ZEO Server', TRACE, m)


# We create a special fast pickler! This allows us
# to create slightly more efficient pickles and
# to create them a tad faster.
pickler=cPickle.Pickler()
pickler.fast=1 # Don't use the memo
dump=pickler.dump

class StorageServer(asyncore.dispatcher):

    def __init__(self, connection, storages):
        
        self.__storages=storages
        for n, s in storages.items():
            init_storage(s)
            # Create a waiting list to support the distributed commit lock.
            s._waiting = []

        self.__connections={}
        self.__get_connections=self.__connections.get

        self._pack_trigger = trigger.trigger()
        asyncore.dispatcher.__init__(self)

        if type(connection) is type(''):
            self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try: os.unlink(connection)
            except: pass
        else:
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            self.set_reuse_addr()

        LOG('ZEO Server', INFO, 'Listening on %s' % repr(connection))
        self.bind(connection)
        self.listen(5)

    def register_connection(self, connection, storage_id):
        storage=self.__storages.get(storage_id, None)
        if storage is None:
            LOG('ZEO Server', ERROR, "Unknown storage_id: %s" % storage_id)
            connection.close()
            return None, None
        
        connections=self.__get_connections(storage_id, None)
        if connections is None:
            self.__connections[storage_id]=connections=[]
        connections.append(connection)
        return storage, storage_id

    def unregister_connection(self, connection, storage_id):
        
        connections=self.__get_connections(storage_id, None)
        if connections: 
            n=[]
            for c in connections:
                if c is not connection:
                    n.append(c)
        
            self.__connections[storage_id]=n

    def invalidate(self, connection, storage_id, invalidated=(), info=0,
                   dump=dump):
        for c in self.__connections[storage_id]:
            if invalidated and c is not connection: 
                c.message_output('I'+dump(invalidated, 1))
            if info:
                c.message_output('S'+dump(info, 1))

    def writable(self): return 0
    
    def handle_read(self): pass
    
    def readable(self): return 1
    
    def handle_connect (self): pass
    
    def handle_accept(self):
        try:
            sock, addr = self.accept()
        except socket.error:
            sys.stderr.write('warning: accept failed\n')
        else:
            ZEOConnection(self, sock, addr)

    def log_info(self, message, type='info'):
        if type=='error': type=ERROR
        else: type=INFO
        LOG('ZEO Server', type, message)

    log=log_info

storage_methods={}
for n in (
    'get_info', 'abortVersion', 'commitVersion',
    'history', 'load', 'loadSerial',
    'modifiedInVersion', 'new_oid', 'new_oids', 'pack', 'store',
    'storea', 'tpc_abort', 'tpc_begin', 'tpc_begin_sync',
    'tpc_finish', 'undo', 'undoLog', 'undoInfo', 'versionEmpty', 'versions',
    'transactionalUndo',
    'vote', 'zeoLoad', 'zeoVerify', 'beginZeoVerify', 'endZeoVerify',
    ):
    storage_methods[n]=1
storage_method=storage_methods.has_key

def find_global(module, name,
                global_dict=globals(), silly=('__doc__',)):
    try: m=__import__(module, global_dict, global_dict, silly)
    except:
        raise StorageServerError, (
            "Couldn\'t import global module %s" % module)

    try: r=getattr(m, name)
    except:
        raise StorageServerError, (
            "Couldn\'t find global %s in module %s" % (name, module))
        
    safe=getattr(r, '__no_side_effects__', 0)
    if safe: return r

    raise StorageServerError, 'Unsafe global, %s.%s' % (module, name)

_noreturn=[]
class ZEOConnection(SizedMessageAsyncConnection):

    _transaction=None
    __storage=__storage_id=None

    def __init__(self, server, sock, addr):
        self.__server=server
        self.__invalidated=[]
        self.__closed=None
        if __debug__: debug='ZEO Server'
        else: debug=0
        SizedMessageAsyncConnection.__init__(self, sock, addr, debug=debug)
        LOG('ZEO Server', INFO, 'Connect %s %s' % (id(self), `addr`))

    def close(self):
        t=self._transaction
        if (t is not None and self.__storage is not None and
            self.__storage._transaction is t):
            self.tpc_abort(t.id)
        else:           
            self._transaction=None
            self.__invalidated=[]

        self.__server.unregister_connection(self, self.__storage_id)
        self.__closed=1
        SizedMessageAsyncConnection.close(self)
        LOG('ZEO Server', INFO, 'Close %s' % id(self))

    def message_input(self, message,
                      dump=dump, Unpickler=Unpickler, StringIO=StringIO,
                      None=None):
        if __debug__:
            if len(message) > max_blather:
                tmp = `message[:max_blather]`
            else:
                tmp = `message`
            blather('message_input', id(self), tmp)

        if self.__storage is None:
            # This is the first communication from the client
            self.__storage, self.__storage_id = (
                self.__server.register_connection(self, message))

            # Send info back asynchronously, so client need not ask
            self.message_output('S'+dump(self.get_info(), 1))
            return
            
        try:

            # Unpickle carefully.
            unpickler=Unpickler(StringIO(message))
            unpickler.find_global=find_global
            args=unpickler.load()
            
            name, args = args[0], args[1:]
            if __debug__:
                apply(blather,
                      ("call", id(self), ":", name,) + args)
                
            if not storage_method(name):
                raise 'Invalid Method Name', name
            if hasattr(self, name):
                r=apply(getattr(self, name), args)
            else:
                r=apply(getattr(self.__storage, name), args)
            if r is _noreturn: return
        except (UndoError, VersionCommitError):
            # These are normal usage errors. No need to leg them
            self.return_error(sys.exc_info()[0], sys.exc_info()[1])
            return
        except:
            LOG('ZEO Server', ERROR, 'error', error=sys.exc_info())
            self.return_error(sys.exc_info()[0], sys.exc_info()[1])
            return

        if __debug__:
            blather("%s R: %s" % (id(self), `r`))
            
        r=dump(r,1)            
        self.message_output('R'+r)

    def return_error(self, err_type, err_value, type=type, dump=dump):
        if type(err_value) is not type(self):
            err_value = err_type, err_value

        if __debug__:
            blather("%s E: %s" % (id(self), `err_value`))
                    
        try: r=dump(err_value, 1)
        except:
            # Ugh, must be an unpicklable exception
            r=StorageServerError("Couldn't pickle error %s" % `r`)
            dump('',1) # clear pickler
            r=dump(r,1)

        self.message_output('E'+r)
        

    def get_info(self):
        storage=self.__storage
        info = {
            'length': len(storage),
            'size': storage.getSize(),
            'name': storage.getName(),
            }
        for feature in ('supportsUndo',
                        'supportsVersions',
                        'supportsTransactionalUndo',):
            if hasattr(storage, feature):
                info[feature] = getattr(storage, feature)()
            else:
                info[feature] = 0
        return info

    def get_size_info(self):
        storage=self.__storage
        return {
            'length': len(storage),
            'size': storage.getSize(),
            }

    def zeoLoad(self, oid):
        storage=self.__storage
        v=storage.modifiedInVersion(oid)
        if v: pv, sv = storage.load(oid, v)
        else: pv=sv=None
        try:
            p, s = storage.load(oid,'')
        except KeyError:
            if sv:
                # Created in version, no non-version data
                p=s=None
            else:
                raise
        return p, s, v, pv, sv
            

    def beginZeoVerify(self):
        self.message_output('bN.')            
        return _noreturn

    def zeoVerify(self, oid, s, sv,
                  dump=dump):
        try: p, os, v, pv, osv = self.zeoLoad(oid)
        except: return _noreturn
        p=pv=None # free the pickles
        if os != s:
            self.message_output('i'+dump((oid, ''),1))            
        elif osv != sv:
            self.message_output('i'+dump((oid,  v),1))
            
        return _noreturn

    def endZeoVerify(self):
        self.message_output('eN.')
        return _noreturn

    def new_oids(self, n=100):
        new_oid=self.__storage.new_oid
        if n < 0: n=1
        r=range(n)
        for i in r: r[i]=new_oid()
        return r

    def pack(self, t, wait=0):
        start_new_thread(self._pack, (t,wait))
        if wait: return _noreturn

    def _pack(self, t, wait=0):
        try:
            LOG('ZEO Server', BLATHER, 'pack begin')
            self.__storage.pack(t, referencesf)
            LOG('ZEO Server', BLATHER, 'pack end')
        except:
            LOG('ZEO Server', ERROR,
                'Pack failed for %s' % self.__storage_id,
                error=sys.exc_info())
            if wait:
                self.return_error(sys.exc_info()[0], sys.exc_info()[1])
                self.__server._pack_trigger.pull_trigger()
        else:
            if wait:
                self.message_output('RN.')
                self.__server._pack_trigger.pull_trigger()
            else:
                # Broadcast new size statistics
                self.__server.invalidate(0, self.__storage_id, (),
                                         self.get_size_info())

    def abortVersion(self, src, id):
        t=self._transaction
        if t is None or id != t.id:
            raise POSException.StorageTransactionError(self, id)
        oids=self.__storage.abortVersion(src, t)
        a=self.__invalidated.append
        for oid in oids: a((oid,src))
        return oids

    def commitVersion(self, src, dest, id):
        t=self._transaction
        if t is None or id != t.id:
            raise POSException.StorageTransactionError(self, id)
        oids=self.__storage.commitVersion(src, dest, t)
        a=self.__invalidated.append
        for oid in oids:
            a((oid,dest))
            if dest: a((oid,src))
        return oids

    def storea(self, oid, serial, data, version, id,
               dump=dump):
        try:
            t=self._transaction
            if t is None or id != t.id:
                raise POSException.StorageTransactionError(self, id)
        
            newserial=self.__storage.store(oid, serial, data, version, t)
        except TransactionError, v:
            # This is a normal transaction errorm such as a conflict error
            # or a version lock or conflict error. It doen't need to be
            # logged.
            newserial=v
        except:
            # all errors need to be serialized to prevent unexpected
            # returns, which would screw up the return handling.
            # IOW, Anything that ends up here is evil enough to be logged.
            LOG('ZEO Server', ERROR, 'store error', error=sys.exc_info())
            newserial=sys.exc_info()[1]
        else:
            if serial != '\0\0\0\0\0\0\0\0':
                self.__invalidated.append((oid, version))

        try: r=dump((oid,newserial), 1)
        except:
            # We got a pickling error, must be because the
            # newserial is an unpicklable exception.
            r=StorageServerError("Couldn't pickle exception %s" % `newserial`)
            dump('',1) # clear pickler
            r=dump((oid, r),1)

        self.message_output('s'+r)
        return _noreturn

    def vote(self, id): 
        t=self._transaction
        if t is None or id != t.id:
            raise POSException.StorageTransactionError(self, id)
        return self.__storage.tpc_vote(t)

    def transactionalUndo(self, trans_id, id):
        t=self._transaction
        if t is None or id != t.id:
            raise POSException.StorageTransactionError(self, id)
        return self.__storage.transactionalUndo(trans_id, self._transaction)
        
    def undo(self, transaction_id):
        oids=self.__storage.undo(transaction_id)
        if oids:
            self.__server.invalidate(
                self, self.__storage_id, map(lambda oid: (oid,None), oids)
                )
            return oids
        return ()

    # distributed commit lock support methods

    # Only one client at a time can commit a transaction on a
    # storage.  If one client is committing a transaction, and a
    # second client sends a tpc_begin(), then second client is queued.
    # When the first transaction finishes, either by abort or commit,
    # the request from the queued client must be handled.

    # It is important that this code be robust.  If a queued
    # transaction is not restarted, the server will stop processing
    # new transactions.

    # This lock is implemented by storing the queued requests in a
    # list on the storage object.  The list contains:
    #     a callable object to resume request
    #     arguments to that object
    #     a callable object to handle errors during resume

    # XXX I am not sure that the commitlock_resume() method is
    # sufficiently paranoid.

    def commitlock_suspend(self, resume, args, onerror):
        self.__storage._waiting.append((resume, args, onerror))

    def commitlock_resume(self):
        waiting = self.__storage._waiting
        while waiting:
            resume, args, onerror = waiting.pop(0)
            try:
                if apply(resume, args):
                    break
            except Disconnected:
                # A disconnected error isn't an unexpected error.
                # There should be no need to log it, because the
                # disconnect will have generated its own log event.
                onerror()
            except:
                LOG('ZEO Server', ERROR,
                    "Unexpected error handling queued tpc_begin()",
                    error=sys.exc_info())
                onerror()

    def tpc_abort(self, id):
        t = self._transaction
        if t is None or id != t.id:
            return
        r = self.__storage.tpc_abort(t)

        self._transaction = None
        self.__invalidated = []
        self.commitlock_resume()
        
    def unlock(self):
        if self.__closed:
            return
        self.message_output('UN.')

    def tpc_begin(self, id, user, description, ext):
        t = self._transaction
        if t is not None:
            if id == t.id:
                return
            else:
                raise StorageServerError(
                    "Multiple simultaneous tpc_begin requests from the same "
                    "client."
                    )
        storage = self.__storage
        if storage._transaction is not None:
            self.commitlock_suspend(self.unlock, (), self.close)
            return 1 # Return a flag indicating a lock condition.
            
        self._transaction=t=Transaction()
        t.id=id
        t.user=user
        t.description=description
        t._extension=ext
        storage.tpc_begin(t)
        self.__invalidated=[]

    def tpc_begin_sync(self, id, user, description, ext):
        if self.__closed: return
        t=self._transaction
        if t is not None and id == t.id: return
        storage=self.__storage
        if storage._transaction is None:
            self.try_again_sync(id, user, description, ext)
        else:
            self.commitlock_suspend(self.try_again_sync,
                                    (id, user, description, ext),
                                    self.close)

        return _noreturn
        
    def try_again_sync(self, id, user, description, ext):
        storage=self.__storage
        if storage._transaction is None:
            self._transaction=t=Transaction()
            t.id=id
            t.user=user
            t.description=description
            storage.tpc_begin(t)
            self.__invalidated=[]
            self.message_output('RN.')
            
        return 1

    def tpc_finish(self, id, user, description, ext):
        t = self._transaction
        if id != t.id:
            return

        storage = self.__storage
        r = storage.tpc_finish(t)

        self._transaction = None
        if self.__invalidated:
            self.__server.invalidate(self, self.__storage_id,
                                     self.__invalidated,
                                     self.get_size_info())
            self.__invalidated = []
            
        self.commitlock_resume()

def init_storage(storage):
    if not hasattr(storage,'tpc_vote'): storage.tpc_vote=lambda *args: None

if __name__=='__main__':
    import ZODB.FileStorage
    name, port = sys.argv[1:3]
    blather(name, port)
    try:
        port='', int(port)
    except:
        pass

    d = {'1': ZODB.FileStorage.FileStorage(name)}
    StorageServer(port, d)
    asyncwrap.loop()
