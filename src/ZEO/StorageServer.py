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

__version__ = "$Revision: 1.36 $"[11:-2]

import asyncore, socket, string, sys, os
import cPickle
from cPickle import Unpickler
from cStringIO import StringIO
from thread import start_new_thread
import time
from types import StringType

from ZODB import POSException
from ZODB.POSException import TransactionError, UndoError, VersionCommitError
from ZODB.Transaction import Transaction
from ZODB.referencesf import referencesf
from ZODB.utils import U64

from ZEO import trigger
from ZEO import asyncwrap
from ZEO.smac import Disconnected, SizedMessageAsyncConnection
from ZEO.logger import zLogger, format_msg

class StorageServerError(POSException.StorageError):
    pass

# We create a special fast pickler! This allows us
# to create slightly more efficient pickles and
# to create them a tad faster.
pickler=cPickle.Pickler()
pickler.fast=1 # Don't use the memo
dump=pickler.dump

log = zLogger("ZEO Server")

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

        log.info('Listening on %s' % repr(connection))
        self.bind(connection)
        self.listen(5)

    def register_connection(self, connection, storage_id):
        storage=self.__storages.get(storage_id, None)
        if storage is None:
            log.error("Unknown storage_id: %s" % storage_id)
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
            r = self.accept()
            if r is None:
                return
            sock, addr = r
        except socket.error, err:
            log.warning("accept() failed: %s" % err)
        else:
            ZEOConnection(self, sock, addr)

    def status(self):
        """Log status information about connections and storages"""

        lines = []
        for storage_id, connections in self.__connections.items():
            s = "Storage %s has %d connections" % (storage_id,
                                                   len(connections))
            lines.append(s)
            for c in connections:
                lines.append("%s readable=%s writeable=%s" % (
                   c, c.readable(), c.writable()))
                lines.append("\t" + c.stats())
        log.info(string.join(lines, "\n"))
        return _noreturn

storage_methods={}
for n in (
    'get_info', 'abortVersion', 'commitVersion',
    'history', 'load', 'loadSerial',
    'modifiedInVersion', 'new_oid', 'new_oids', 'pack', 'store',
    'storea', 'tpc_abort', 'tpc_begin', 'tpc_begin_sync',
    'tpc_finish', 'undo', 'undoLog', 'undoInfo', 'versionEmpty', 'versions',
    'transactionalUndo',
    'vote', 'zeoLoad', 'zeoVerify', 'beginZeoVerify', 'endZeoVerify',
    'status'
    ):
    storage_methods[n]=1
storage_method=storage_methods.has_key

def find_global(module, name,
                global_dict=globals(), silly=('__doc__',)):
    try: m=__import__(module, global_dict, global_dict, silly)
    except:
        raise StorageServerError, (
            "Couldn\'t import global module %s" % module)

    try:
        r=getattr(m, name)
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
        self.status = server.status
        self.__invalidated=[]
        self.__closed=None
        if __debug__:
            debug = log
        else:
            debug = None

        if __debug__:
            # store some detailed statistics about method calls
            self._last_method = None
            self._t_begin = None
            self._t_end = None
            self._ncalls = 0
            
        SizedMessageAsyncConnection.__init__(self, sock, addr, debug=debug)
        self.logaddr = repr(addr) # form of addr suitable for logging
        log.info('Connect %s %s' % (id(self), self.logaddr))

    def stats(self):
        # This method is called via the status() command.  The stats
        # are of limited use for the current command, because the
        # actual invocation of status() will clobber the previous
        # method's statistics.
        #
        # When there are multiple connections active, a new connection
        # can always get detailed statistics about other connections.
        if __debug__:
            if self._last_method == "status":
                return "method=status begin=%s end=... ncalls=%d" % (
                    self._t_begin, self._ncalls)
            if self._t_end is not None and self._t_begin is not None:
                delta = self._t_end - self._t_begin
            else:
                delta = -1
            return "method=%s begin=%s end=%s delta=%.3f ncalls=%d" % (
                self._last_method, self._t_begin, self._t_end, delta,
                self._ncalls)
        else:
            return ""

    def __repr__(self):
        return "<ZEOConnection %s%s" % (`self.addr`,
                         # sort of messy way to add tag 'closed' to
                         # connections that are closed
                         (self.__closed is None and '>' or ' closed>'))

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
        log.info('Close %s' % id(self))

    def message_input(self, message,
                      dump=dump, Unpickler=Unpickler, StringIO=StringIO,
                      None=None):
        if __debug__:

            self._t_begin = time.time()
            self._t_end = None
            
            if len(message) > 120: # XXX need constant from logger
                tmp = `message[:120]`
            else:
                tmp = `message`
            log.trace("message_input %s" % tmp)

        if self.__storage is None:
            if __debug__:
                log.blather("register connection to %s from %s" % (message,
                                                                self.logaddr))
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
                self._last_method = name
                self._ncalls = self._ncalls + 1
                log.debug("call %s%s from %s" % (name, format_msg(args),
                                                 self.logaddr))
                
            if not storage_method(name):
                log.warning("Invalid method name: %s" % name)
                if __debug__:
                    self._t_end = time.time()
                raise 'Invalid Method Name', name
            if hasattr(self, name):
                r=apply(getattr(self, name), args)
            else:
                r=apply(getattr(self.__storage, name), args)
            if r is _noreturn:
                if __debug__:
                    log.debug("no return to %s" % self.logaddr)
                    self._t_end = time.time()
                return
        except (UndoError, VersionCommitError), err:
            if __debug__:
                log.debug("return error %s to %s" % (err, self.logaddr))
                self._t_end = time.time()
            # These are normal usage errors. No need to log them.
            self.return_error(sys.exc_info()[0], sys.exc_info()[1])
            return
        except:
            if __debug__:
                self._t_end = time.time()
            log.error("error", error=sys.exc_info())
            self.return_error(sys.exc_info()[0], sys.exc_info()[1])
            return

        if __debug__:
            log.debug("return %s to %s" % (format_msg(r), self.logaddr))
            self._t_end = time.time()
            
        r=dump(r,1)            
        self.message_output('R'+r)

    def return_error(self, err_type, err_value, type=type, dump=dump):
        if type(err_value) is not type(self):
            err_value = err_type, err_value

        if __debug__:
            log.trace("%s E: %s" % (id(self), `err_value`))
                    
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
        if __debug__:
            log.blather("zeoLoad(%s) %s" % (U64(oid), self.logaddr))
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
        if __debug__:
            log.blather("beginZeoVerify() %s" % self.logaddr)
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
        if __debug__:
            log.blather("endZeoVerify() %s" % self.logaddr)
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
            log.blather('pack begin')
            self.__storage.pack(t, referencesf)
            log.blather('pack end')
        except:
            log.error(
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
        if __debug__:
            log.blather("storea(%s, [%d], %s) %s" % (U64(oid), len(data),
                                                   U64(id), self.logaddr))
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
            log.error('store error', error=sys.exc_info())
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
        if __debug__:
            log.blather("transactionalUndo(%s, %s) %s" % (trans_id,
                                                        U64(id), self.logaddr))
        t=self._transaction
        if t is None or id != t.id:
            raise POSException.StorageTransactionError(self, id)
        return self.__storage.transactionalUndo(trans_id, self._transaction)
        
    def undo(self, transaction_id):
        if __debug__:
            log.blather("undo(%s) %s" % (transaction_id, self.logaddr))
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
        log.blather("suspend %s.  %d queued clients" % (resume.im_self,
                                            len(self.__storage._waiting)))

    def commitlock_resume(self):
        waiting = self.__storage._waiting
        while waiting:
            resume, args, onerror = waiting.pop(0)
            log.blather("resuming queued client %s, %d still queued" % (
                resume.im_self, len(waiting)))
            try:
                if apply(resume, args):
                    break
            except Disconnected:
                # A disconnected error isn't an unexpected error.
                # There should be no need to log it, because the
                # disconnect will have generated its own log event.
                onerror()
            except:
                log.error(
                    "Unexpected error handling queued tpc_begin()",
                    error=sys.exc_info())
                onerror()

    def tpc_abort(self, id):
        if __debug__:
            try:
                log.blather("tpc_abort(%s) %s" % (U64(id), self.logaddr))
            except:
                print repr(id)
                raise
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
        if __debug__:
            log.blather("tpc_begin(%s, %s, %s) %s" % (U64(id), `user`,
                                                      `description`,
                                                      self.logaddr))
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

        assert id != 't'
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
        if __debug__:
            log.blather("tpc_finish(%s) %s" % (U64(id), self.logaddr))
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
    log.trace(format_msg(name, port))
    try:
        port='', int(port)
    except:
        pass

    d = {'1': ZODB.FileStorage.FileStorage(name)}
    StorageServer(port, d)
    asyncwrap.loop()
