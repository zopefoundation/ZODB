
import asyncore, socket, string, sys, cPickle
from smac import smac
from ZODB import POSException
from ZODB.Transaction import Transaction
import traceback
from zLOG import LOG, INFO, ERROR

class StorageServerError(POSException.StorageError): pass

def blather(*args):
    LOG('ZEO Server', INFO, string.join(args))

class StorageServer(asyncore.dispatcher):

    def __init__(self, connection, storages):
        
        self.host, self.port = connection
        self.__storages=storages

        self.__connections={}
        self.__get_connections=self.__connections.get


        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)

        self.bind((self.host, self.port))

        self.listen(5)

    def register_connection(self, connection, storage_id):
        storage=self.__storages.get(storage_id, None)
        if storage is None:
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

    def invalidate(self, connection, storage_id, invalidated,
                   dumps=cPickle.dumps):
        for c in self.__connections[storage_id]:
            if c is connection: continue
            c.message_output('I'+dumps(invalidated))

    def writable(self): return 0
    
    def handle_read(self): pass
    
    def readable(self): return 1
    
    def handle_connect (self): pass
    
    def handle_accept(self):
        try:
            sock, addr = self.accept()
        except socket.error:
            sys.stderr.write('warning: accept failed\n')

        Connection(self, sock, addr)

    def log_info(self, message, type='info'):
        if type=='error': type=ERROR
        else: type=INFO
        LOG('ZEO Server', type, message)

    log=log_info

storage_methods={}
for n in ('get_info', 'abortVersion', 'commitVersion', 'history',
          'load', 'modifiedInVersion', 'new_oid', 'pack', 'store',
          'tpc_abort', 'tpc_begin', 'tpc_begin_sync', 'tpc_finish', 'undo',
          'undoLog', 'versionEmpty',
          'zeoLoad', 'zeoVerify',
          ):
    storage_methods[n]=1
storage_method=storage_methods.has_key

_noreturn=[]
class Connection(smac):

    _transaction=None
    __storage=__storage_id=None

    def __init__(self, server, sock, addr):
        smac.__init__(self, sock, addr)
        self.__server=server
        self.__invalidated=[]
        self.__closed=None

    def close(self):
        t=self._transaction
        if (t is not None and self.__storage is not None and
            self.__storage._transaction is t):
            self.tpc_abort(t.id)

        self.__server.unregister_connection(self, self.__storage_id)
        self.__closed=1
        smac.close(self)

    def message_input(self, message):
        if __debug__:
            m=`message`
            if len(m) > 60: m=m[:60]+' ...'
            blather('message_input', m)

        if self.__storage is None:
            self.__storage, self.__storage_id = (
                self.__server.register_connection(self, message))
            return
            
        rt='R'
        try:
            args=cPickle.loads(message)
            name, args = args[0], args[1:]
            if __debug__:
                m=`tuple(args)`
                if len(m) > 60: m=m[:60]+' ...'
                blather('call: %s%s' % (name, m))
                
            if not storage_method(name):
                raise 'Invalid Method Name', name
            if hasattr(self, name):
                r=apply(getattr(self, name), args)
            else:
                r=apply(getattr(self.__storage, name), args)
            if r is _noreturn: return
        except:
            LOG('ZEO Server', ERROR, 'error', error=sys.exc_info())
            t, r = sys.exc_info()[:2]
            if type(r) is not type(self): r=t,r
            rt='E'

        if __debug__:
            m=`r`
            if len(m) > 60: m=m[:60]+' ...'
            blather('%s: %s' % (rt, m))
            
        r=cPickle.dumps(r,1)
        self.message_output(rt+r)

    def get_info(self):
        storage=self.__storage
        return {
            'length': len(storage),
            'size': storage.getSize(),
            'name': storage.getName(),
            'supportsUndo': storage.supportsUndo(),
            'supportsVersions': storage.supportsVersions(),
            }

    def zeoLoad(self, oid):
        storage=self.__storage
        v=storage.modifiedInVersion(oid)
        if v: pv, sv = storage.load(oid, v)
        else: pv=sv=None
        p, s = storage.load(oid,'')
        return p, s, v, pv, sv

    def zeoVerify(self, oid, s, sv,
                  dumps=cPickle.dumps):
        try: p, os, v, pv, osv = self.zeoLoad(oid)
        except: return _noreturn
        p=pv=None # free the pickles
        if os != s:
            self.message_output('I'+dumps(((oid, os, ''),)))            
        elif osv != sv:
            self.message_output('I'+dumps(((oid, osv, v),)))
            
        return _noreturn
        

    def store(self, oid, serial, data, version, id):
        t=self._transaction
        if t is None or id != t.id:
            raise POSException.StorageTransactionError(self, id)
        newserial=self.__storage.store(oid, serial, data, version, t)
        if serial != '\0\0\0\0\0\0\0\0':
            self.__invalidated.append(oid, serial, version)
        return newserial

    def tpc_abort(self, id):
        t=self._transaction
        if t is None or id != t.id: return
        r=self.__storage.tpc_abort(t)

        storage=self.__storage
        try: waiting=storage.__waiting
        except: waiting=storage.__waiting=[]
        while waiting:
            f, args = waiting.pop(0)
            if apply(f,args): break

        self._transaction=None
        self.__invalidated=[]
        
    def unlock(self):
        if self.__closed: return
        self.message_output('UN.')

    def tpc_begin(self, id, user, description, ext):
        t=self._transaction
        if t is not None and id == t.id: return
        storage=self.__storage
        if storage._transaction is not None:
            try: waiting=storage.__waiting
            except: waiting=storage.__waiting=[]
            waiting.append(self.unlock, ())
            return 1 # Return a flag indicating a lock condition.
            
        self._transaction=t=Transaction()
        t.id=id
        t.user=user
        t.description=description
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
            try: waiting=storage.__waiting
            except: waiting=storage.__waiting=[]
            waiting.append(self.try_again_sync, (id, user, description, ext))

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
        t=self._transaction
        if id != t.id: return
        t.user=user
        t.description=description
        t.ext=ext

        storage=self.__storage
        r=storage.tpc_finish(t)
        
        try: waiting=storage.__waiting
        except: waiting=storage.__waiting=[]
        while waiting:
            f, args = waiting.pop(0)
            if apply(f,args): break

        self._transaction=None
        self.__server.invalidate(self, self.__storage_id, self.__invalidated)
        self.__invalidated=[]
        

if __name__=='__main__':
    import ZODB.FileStorage
    name, port = sys.argv[1:3]
    blather(name, port)
    try: port='',string.atoi(port)
    except: pass
    StorageServer(port, ZODB.FileStorage.FileStorage(name))
    asyncore.loop()
