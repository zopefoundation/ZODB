"""But simple rpc mechanisms
"""

from cPickle import dumps, loads
from ThreadLock import allocate_lock
import socket, smac, string, struct
TupleType=type(())

class sync:
    """Synchronous rpc"""

    _outOfBand=None

    def __init__(self, connection, outOfBand=None):
        host, port = connection
        s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(host, port)
        self.__s=s
        self._outOfBand=outOfBand

    def setOutOfBand(self, f): self._outOfBand=f
        
    def __call__(self, *args):
        args=dumps(args,1)
        self._write(args)
        while 1:
            r=self._read()
            c=r[:1]
            if c=='R':
                return loads(r[1:])
            if c=='E':
                r=loads(r[1:])
                if type(r) is TupleType: raise r[0], r[1]
                raise r
            oob=self._outOfBand
            if oob is not None:
                oob(c, loads(r[1:]))
            else:
                raise UnrecognizedResult, r

    def _write(self, data, pack=struct.pack):
        send=self.__s.send
        h=pack(">i", len(data))
        l=len(h)
        while l > 0:
            sent=send(h)
            h=h[sent:]
            l=l-sent
        l=len(data)
        while l > 0:
            sent=send(data)
            data=data[sent:]
            l=l-sent

    def _read(self, _st=type(''), join=string.join, unpack=struct.unpack):
        recv=self.__s.recv

        l=4

        data=None
        while l > 0:
            d=recv(l)
            if data is None: data=d
            elif type(data) is _st: data=[data, d]
            else: data.append(d)
            l=l-len(d)
        if type(data) is not _st: data=join(data,'')

        l,=unpack(">i", data)

        data=None
        while l > 0:
            d=recv(l)
            if data is None: data=d
            elif type(data) is st: data=[data, d]
            else: data.append(d)
            l=l-len(d)
        if type(data) is not _st: data=join(data,'')

        return data
    
class async(smac.smac, sync):

    def __init__(self, connection, outOfBand=None):
        host, port = connection
        s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(host, port)
        self._outOfBand=outOfBand
        l=allocate_lock()
        self.__la=l.acquire
        self.__lr=l.release
        self.__r=None
        l.acquire()
        smac.__init__(self, s, None)
    
    def _write(self, data): self.message_output(data)

    def message_input(self, m):
        c=m[:1]
        if c in 'RE':
            self.__r=m
            self.__lr()
        else:
            oob=self._outOfBand
            if oob is not None: oob(c, loads(m[1:]))

    def _read(self):
        self.__la()
        return self.__r
        


