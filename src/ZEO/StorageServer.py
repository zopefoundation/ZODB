
import asyncore, socket, string, sys, cPickle
from smac import smac
from ZODB import POSException
from ZODB.Transaction import Transaction
import traceback

class StorageServerError(POSException.ServerError): pass


class Server(asyncore.dispatcher):

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

storage_methods={}
for n in ('get_info', 'abortVersion', 'commitVersion', 'history',
          'load', 'modifiedInVersion', 'new_oid', 'pack', 'store',
          'tpc_abort', 'tpc_begin', 'tpc_finish', 'undo', 'undoLog',
          'versionEmpty'):
    storage_methods[n]=1
storage_method=storage_methods.has_key


class Connection(smac):

    _transaction=None
    __storage=__storage_id=None

    def __init__(self, server, sock, addr):
        smac.__init__(self, sock, addr)
        self.__server=server
        self.__storage=server.storage
        self.__invalidated=[]

    def close(self):
        self.__server.unregister_connection(self, self.__storage_id)
        smac.close(self)

    def message_input(self, message):
        if self.__storage is None:
            self.__storage, self.__storage_id = (
                self.__server.register_connection(self, message))
            return
            
        rt='R'
        try:
            args=cPickle.loads(message)
            name, args = args[0], args[1:]
            if not storage_method(name):
                raise 'Invalid Method Name', name
            if hasattr(self, name):
                r=apply(getattr(self, name), args)
            else:
                r=apply(getattr(self.__storage, name), args)
        except:
            traceback.print_exc()
            t, r = sys.exc_info()[:2]
            if type(r) is not type(self): r=t,r
            rt='E'

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

    def store(self, oid, serial, data, version, id):
        t=self._transaction
        if t is None or id != t.id:
            raise POSException.StorageTransactionError(self, id)
        newserial=self.__storage.store(oid, data, serial, version, t)
        if serial != '\0\0\0\0\0\0\0\0':
            self.__invalidated.append(oid, serial, version)
        return newserial

    def unlock(self):
        self.message_output('UN')

    def tpc_abort(self, id):
        t=self._transaction
        if t is None or id != t.id: return
        r=self.__storage.tpc_abort(t)
        for c in self.__storage.__waiting: c.unlock()
        self._transaction=None
        self.__invalidated=[]
        

    def tpc_begin(self, id, user, description, ext):
        t=self._transaction
        if t is not None and id == t.id: return
        storage=self.__storage
        if storage._transaction is not None:
            storage.__waiting.append(self)
            return 1
            
        self._transaction=t=Transaction()
        t.id=id
        t.user=user
        t.description=description
        storage.tpc_begin(t)
        storage.__waiting=[]
        self.__invalidated=[]
        

    def tpc_finish(self, id, user, description, ext):
        t=self._transaction
        if id != t.id: return
        t.user=user
        t.description=description
        r=self.__storage.tpc_finish(t)
        for c in self.__storage.__waiting: c.unlock()
        self._transaction=None
        self.__server.invalidate(self, self.__storage_id, self.__invalidated)
        self.__invalidated=[]
        

if __name__=='__main__':
    import ZODB.FileStorage
    name, port = sys.argv[1:3]
    try: port='',string.atoi(port)
    except: pass
    Server(port, ZODB.FileStorage.FileStorage(name))
    asyncore.loop()
