"""RPC protocol for ZEO based on asyncore

The basic protocol is as:
a pickled tuple containing: msgid, flags, method, args

msgid is an integer.
flags is an integer.
    The only currently defined flag is ASYNC (0x1), which means
    the client does not expect a reply.
method is a string specifying the method to invoke.
    For a reply, the method is ".reply".
args is a tuple of the argument to pass to method.

XXX need to specify a version number that describes the protocol.
allow for future revision.

XXX support multiple outstanding calls

XXX factor out common pattern of deciding what protocol to use based
on whether address is tuple or string
"""

import asyncore
import errno
import cPickle
import os
import select
import socket
import sys
import threading
import thread
import time
import traceback
import types

from cStringIO import StringIO

from ZODB import POSException
from ZEO import smac, trigger
from Exceptions import Disconnected
import zLOG
import ThreadedAsync
from Exceptions import Disconnected

REPLY = ".reply" # message name used for replies
ASYNC = 1

_label = "zrpc:%s" % os.getpid()

def new_label():
    global _label
    _label = "zrpc:%s" % os.getpid()

def log(message, level=zLOG.BLATHER, label=None, error=None):
    zLOG.LOG(label or _label, level, message, error=error)

class ZRPCError(POSException.StorageError):
    pass

class DecodingError(ZRPCError):
    """A ZRPC message could not be decoded."""

class DisconnectedError(ZRPCError, Disconnected):
    """The database storage is disconnected from the storage server."""

# Export the mainloop function from asycnore to zrpc clients
loop = asyncore.loop

def connect(addr, client=None):
    if type(addr) == types.TupleType:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    else:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(addr)
    c = Connection(s, addr, client)
    return c

class Marshaller:
    """Marshal requests and replies to second across network"""

    # It's okay to share a single Pickler as long as it's in fast
    # mode, which means that it doesn't have a memo.
    
    pickler = cPickle.Pickler()
    pickler.fast = 1
    pickle = pickler.dump

    errors = (cPickle.UnpickleableError,
              cPickle.UnpicklingError,
              cPickle.PickleError,
              cPickle.PicklingError)

    def encode(self, msgid, flags, name, args):
        """Returns an encoded message"""
        return self.pickle((msgid, flags, name, args), 1)

    def decode(self, msg):
        """Decodes msg and returns its parts"""
        unpickler = cPickle.Unpickler(StringIO(msg))
        unpickler.find_global = find_global

        try:
            return unpickler.load() # msgid, flags, name, args
        except (cPickle.UnpicklingError, IndexError), err_msg:
            log("can't decode %s" % repr(msg), level=zLOG.ERROR)
            raise DecodingError(msg)

class Delay:
    """Used to delay response to client for synchronous calls

    When a synchronous call is made and the original handler returns
    without handling the call, it returns a Delay object that prevents
    the mainloop from sending a response.
    """

    def set_sender(self, msgid, send_reply):
        self.msgid = msgid
        self.send_reply = send_reply

    def reply(self, obj):
        self.send_reply(self.msgid, obj)

class Connection(smac.SizedMessageAsyncConnection):
    """Dispatcher for RPC on object

    The connection supports synchronous calls, which expect a return,
    and asynchronous calls that do not.

    It uses the Marshaller class to handle encoding and decoding of
    method calls are arguments.

    A Connection is designed for use in a multithreaded application,
    where a synchronous call must block until a response is ready.
    The current design only allows a single synchronous call to be
    outstanding. 
    """
    __super_init = smac.SizedMessageAsyncConnection.__init__
    __super_close = smac.SizedMessageAsyncConnection.close
    __super_writable = smac.SizedMessageAsyncConnection.writable

    def __init__(self, sock, addr, obj=None):
        self.msgid = 0
        self.obj = obj
        self.marshal = Marshaller()
        self.closed = 0
        self.async = 0
        # The reply lock is used to block when a synchronous call is
        # waiting for a response
        self.__super_init(sock, addr)
        self._map = {self._fileno: self}
        self._prepare_async()
        self.__call_lock = thread.allocate_lock()
        self.__reply_lock = thread.allocate_lock()
        self.__reply_lock.acquire()
        if isinstance(obj, Handler):
            self.set_caller = 1
        else:
            self.set_caller = 0

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.addr)

    def close(self):
        if self.closed:
            return
        self.closed = 1
        self.__super_close()

    def register_object(self, obj):
        """Register obj as the true object to invoke methods on"""
        self.obj = obj

    def message_input(self, message):
        """Decoding an incoming message and dispatch it"""
        # XXX Not sure what to do with errors that reach this level.
        # Need to catch ZRPCErrors in handle_reply() and
        # handle_request() so that they get back to the client.
        try:
            msgid, flags, name, args = self.marshal.decode(message)
        except DecodingError, msg:
            return self.return_error(None, None, sys.exc_info()[0],
                                     sys.exc_info()[1])  

        if __debug__:
            log("recv msg: %s, %s, %s, %s" % (msgid, flags, name,
                                              repr(args)[:40]),
                level=zLOG.DEBUG)
        if name == REPLY:
            self.handle_reply(msgid, flags, args)
        else:
            self.handle_request(msgid, flags, name, args)

    def handle_reply(self, msgid, flags, args):
        if __debug__:
            log("recv reply: %s, %s, %s" % (msgid, flags, str(args)[:40]),
                level=zLOG.DEBUG)
        self.__reply = msgid, flags, args
        self.__reply_lock.release() # will fail if lock is unlocked

    def handle_request(self, msgid, flags, name, args):
        if __debug__:
            log("call %s%s on %s" % (name, repr(args)[:40], repr(self.obj)),
                zLOG.DEBUG)
        if not self.check_method(name):
            raise ZRPCError("Invalid method name: %s on %s" % (name,
                                                               `self.obj`))

        meth = getattr(self.obj, name)
        try:
            if self.set_caller:
                self.obj.set_caller(self)
                try:
                    ret = meth(*args)
                finally:
                    self.obj.clear_caller()
            else:
                ret = meth(*args)
        except (POSException.UndoError,
                POSException.VersionCommitError), msg:
            error = sys.exc_info()
            log("%s() raised exception: %s" % (name, msg), zLOG.ERROR, error)
            return self.return_error(msgid, flags, error[0], error[1])
        except Exception, msg:
            error = sys.exc_info()
            log("%s() raised exception: %s" % (name, msg), zLOG.ERROR, error)
            return self.return_error(msgid, flags, error[0], error[1])

        if flags & ASYNC:
            if ret is not None:
                log("async method %s returned value %s" % (name, repr(ret)),
                    zLOG.ERROR)
                raise ZRPCError("async method returned value")
        else:
            if __debug__:
                log("%s return %s" % (name, repr(ret)[:40]), zLOG.DEBUG)
            if isinstance(ret, Delay):
                ret.set_sender(msgid, self.send_reply)
            else:
                self.send_reply(msgid, ret)

    def handle_error(self):
        self.log_error()
        self.close()

    def log_error(self, msg="No error message supplied"):
        error = sys.exc_info()
        log(msg, zLOG.ERROR, error=error)
        del error

    def check_method(self, name):
        # XXX minimal security check should go here: Is name exported?
        return hasattr(self.obj, name)

    def send_reply(self, msgid, ret):
        msg = self.marshal.encode(msgid, 0, REPLY, ret)
        self.message_output(msg)
    
    def return_error(self, msgid, flags, err_type, err_value):
        if flags is None:
            self.log_error("Exception raised during decoding")
            return
        if flags & ASYNC:
            self.log_error("Asynchronous call raised exception: %s" % self)
            return
        if type(err_value) is not types.InstanceType:
            err_value = err_type, err_value

        try:
            msg = self.marshal.encode(msgid, 0, REPLY, (err_type, err_value))
        except self.marshal.errors:
            err = ZRPCError("Couldn't pickle error %s" % `err_value`)
            msg = self.marshal.encode(msgid, 0, REPLY, (ZRPCError, err))
        self.message_output(msg)
        self._do_io()

    # The next two methods are used by clients to invoke methods on
    # remote objects  

    # XXX Should revise design to allow multiple outstanding
    # synchronous calls

    def call(self, method, *args):
        self.__call_lock.acquire()
        try:
            return self._call(method, args)
        finally:
            self.__call_lock.release()

    def _call(self, method, args):
        if self.closed:
            raise DisconnectedError("This action is temporarily unavailable")
        msgid = self.msgid
        self.msgid = self.msgid + 1
        if __debug__:
            log("send msg: %d, 0, %s, ..." % (msgid, method))
        self.message_output(self.marshal.encode(msgid, 0, method, args))

        self.__reply = None
        # lock is currently held
        self._do_io(wait=1)
        # lock is held again...
        r_msgid, r_flags, r_args = self.__reply
        self.__reply_lock.acquire()
        assert r_msgid == msgid, "%s != %s: %s" % (r_msgid, msgid, r_args)

        if type(r_args) == types.TupleType \
           and type(r_args[0]) == types.ClassType \
           and issubclass(r_args[0], Exception):
            raise r_args[1] # error raised by server
        return r_args

    def callAsync(self, method, *args):
        self.__call_lock.acquire()
        try:
            self._callAsync(method, args)
        finally:
            self.__call_lock.release()

    def _callAsync(self, method, args):
        if self.closed:
            raise DisconnectedError("This action is temporarily unavailable")
        msgid = self.msgid
        self.msgid += 1
        if __debug__:
            log("send msg: %d, %d, %s, ..." % (msgid, ASYNC, method))
        self.message_output(self.marshal.encode(msgid, ASYNC, method, args))
        self._do_io()

    # handle IO, possibly in async mode

    def sync(self):
        pass # XXX what is this supposed to do?

    def _prepare_async(self):
        self._async = 0
        ThreadedAsync.register_loop_callback(self.set_async)
        # XXX If we are not in async mode, this will cause dead
        # Connections to be leaked.

    def set_async(self, map):
        # XXX do we need a lock around this?  I'm not sure there is
        # any harm to a race with _do_io().
        self._async = 1
        self.trigger = trigger.trigger()

    def is_async(self):
        return self._async
            
    def _do_io(self, wait=0): # XXX need better name
        # XXX invariant? lock must be held when calling with wait==1
        # otherwise, in non-async mode, there will be no poll

        if __debug__:
            log("_do_io(wait=%d), async=%d" % (wait, self.is_async()),
                level=zLOG.DEBUG)
        if self.is_async():
            self.trigger.pull_trigger()
            if wait:
                self.__reply_lock.acquire()
                # wait until reply...
                self.__reply_lock.release()
        else:
            if wait:
                # do loop only if lock is already acquired
                while not self.__reply_lock.acquire(0):
                    asyncore.poll(10.0, self._map)
                    if self.closed:
                        raise Disconnected()
                self.__reply_lock.release()
            else:
                asyncore.poll(0.0, self._map)

        # XXX it seems that we need to release before returning if
        # called with wait==1.  perhaps the caller need not acquire
        # upon return...

class ServerConnection(Connection):
    # XXX this is a hack
    def _do_io(self, wait=0):
        """If this is a server, there is no explicit IO to do"""
        pass

class ConnectionManager:
    """Keeps a connection up over time"""

    # XXX requires that obj implement notifyConnected and
    # notifyDisconnected.   make this optional?

    def __init__(self, addr, obj=None, debug=1, tmin=1, tmax=180):
        self.set_addr(addr)
        self.obj = obj
        self.tmin = tmin
        self.tmax = tmax
        self.debug = debug
        self.connected = 0
        self.connection = None
        # If _thread is not None, then there is a helper thread
        # attempting to connect.  _thread is protected by _connect_lock.
        self._thread = None
        self._connect_lock = threading.Lock()
        self.trigger = None
        self.async = 0
        self.closed = 0
        ThreadedAsync.register_loop_callback(self.set_async)

    def __repr__(self):
        return "<%s for %s>" % (self.__class__.__name__, self.addr)

    def set_addr(self, addr):
        "Set one or more addresses to use for server."

        # For backwards compatibility (and simplicity?) the
        # constructor accepts a single address in the addr argument --
        # a string for a Unix domain socket or a 2-tuple with a
        # hostname and port.  It can also accept a list of such addresses.

        addr_type = self._guess_type(addr)
        if addr_type is not None:
            self.addr = [(addr_type, addr)]
        else:
            self.addr = []
            for a in addr:
                addr_type = self._guess_type(a)
                if addr_type is None:
                    raise ValueError, "unknown address in list: %s" % repr(a)
                self.addr.append((addr_type, a))

    def _guess_type(self, addr):
        if isinstance(addr, types.StringType):
            return socket.AF_UNIX

        if (len(addr) == 2
            and isinstance(addr[0], types.StringType)
            and isinstance(addr[1], types.IntType)):
            return socket.AF_INET

        # not anything I know about

        return None

    def close(self):
        """Prevent ConnectionManager from opening new connections"""
        self.closed = 1
        self._connect_lock.acquire()
        try:
            if self._thread is not None:
                self._thread.join()
        finally:
            self._connect_lock.release()
        if self.connection:
            self.connection.close()

    def register_object(self, obj):
        self.obj = obj

    def set_async(self, map):
        # XXX need each connection started with async==0 to have a callback
        self.async = 1 # XXX needs to be set on the Connection
        self.trigger = trigger.trigger()

    def connect(self, sync=0):
        if self.connected == 1:
            return
        self._connect_lock.acquire()
        try:
            if self._thread is None:
                zLOG.LOG(_label, zLOG.BLATHER,
                         "starting thread to connect to server")
                self._thread = threading.Thread(target=self.__m_connect)
                self._thread.start()
            if sync:
                try:
                    self._thread.join()
                except AttributeError:
                    # probably means the thread exited quickly
                    pass
        finally:
            self._connect_lock.release()

    def attempt_connect(self):
        # XXX will _attempt_connects() take too long?  think select().
        self._attempt_connects()
        return self.connected

    def notify_closed(self, conn):
        self.connected = 0
        self.connection = None
        self.obj.notifyDisconnected()
        if not self.closed:
            self.connect()

    class Connected(Exception):
        def __init__(self, sock):
            self.sock = sock
            
    def __m_connect(self):
        # a new __connect that handles multiple addresses
        try:
            delay = self.tmin
            while not (self.closed or self._attempt_connects()):
                time.sleep(delay)
                delay *= 2
                if delay > self.tmax:
                    delay = self.tmax
        finally:
            self._thread = None

    def _attempt_connects(self):
        "Return true if any connect attempt succeeds."
        sockets = {}

        zLOG.LOG(_label, zLOG.BLATHER,
                 "attempting connection on %d sockets" % len(self.addr))
        try:
            for domain, addr in self.addr:
                if __debug__:
                    zLOG.LOG(_label, zLOG.DEBUG,
                             "attempt connection to %s" % repr(addr))
                s = socket.socket(domain, socket.SOCK_STREAM)
                s.setblocking(0)
                # XXX can still block for a while if addr requires DNS
                e = self._connect_ex(s, addr)
                if e is not None:
                    sockets[s] = addr

            # next wait until the actually connect
            while sockets:
                if self.closed:
                    for s in sockets.keys():
                        s.close()
                    return 0
                try:
                    r, w, x = select.select([], sockets.keys(), [], 1.0)
                except select.error:
                    continue
                for s in w:
                    e = self._connect_ex(s, sockets[s])
                    if e is None:
                        del sockets[s]
        except self.Connected, container:
            s = container.sock
            del sockets[s]
            # close all the other sockets
            for s in sockets.keys():
                s.close()
            return 1
        return 0

    def _connect_ex(self, s, addr):
        """Call s.connect_ex(addr) and return true if loop should continue.

        We have to handle several possible return values from
        connect_ex().  If the socket is connected and the initial ZEO
        setup works, we're done.  Report success by raising an
        exception.  Yes, the is odd, but we need to bail out of the
        select() loop in the caller and an exception is a principled
        way to do the abort.

        If the socket sonnects and the initial ZEO setup fails or the
        connect_ex() returns an error, we close the socket and ignore it.

        If connect_ex() returns EINPROGRESS, we need to try again later.
        """
        
        e = s.connect_ex(addr)
        if e == errno.EINPROGRESS:
            return 1
        elif e == 0:
            c = self._test_connection(s, addr)
            zLOG.LOG(_label, zLOG.DEBUG, "connected to %s" % repr(addr))
            if c:
                self.connected = 1
                raise self.Connected(s)
        else:
            if __debug__:
                zLOG.LOG(_label, zLOG.DEBUG,
                         "error connecting to %s: %s" % (addr,
                                                         errno.errorcode[e]))
            s.close()

    def _test_connection(self, s, addr):
        c = ManagedConnection(s, addr, self.obj, self)
        try:
            self.obj.notifyConnected(c)
            self.connection = c
            return 1
        except:
            # XXX zLOG the error
            c.close()
        return 0

class ManagedServerConnection(ServerConnection):
    """A connection that notifies its ConnectionManager of closing"""
    __super_init = Connection.__init__
    __super_close = Connection.close

    def __init__(self, sock, addr, obj, mgr):
        self.__mgr = mgr
        self.__super_init(sock, addr, obj)

    def close(self):
        self.__super_close()
        self.__mgr.close(self)

class ManagedConnection(Connection):
    """A connection that notifies its ConnectionManager of closing.

    A managed connection also defers the ThreadedAsync work to its
    manager. 
    """
    __super_init = Connection.__init__
    __super_close = Connection.close

    def __init__(self, sock, addr, obj, mgr):
        self.__mgr = mgr
        if self.__mgr.async:
            self.__async = 1
            self.trigger = self.__mgr.trigger
        else:
            self.__async = None
        self.__super_init(sock, addr, obj)

    def _prepare_async(self):
        # Don't do the register_loop_callback that the superclass does
        pass

    def is_async(self):
        if self.__async:
            return 1
        async = self.__mgr.async
        if async:
            self.__async = 1
            self.trigger = self.__mgr.trigger
        return async

    def close(self):
        self.__super_close()
        self.__mgr.notify_closed(self)

class Dispatcher(asyncore.dispatcher):
    """A server that accepts incoming RPC connections"""
    __super_init = asyncore.dispatcher.__init__

    reuse_addr = 1

    def __init__(self, addr, obj=None, factory=Connection, reuse_addr=None):  
        self.__super_init()
        self.addr = addr
        self.obj = obj
        self.factory = factory
        self.clients = []
        if reuse_addr is not None:
            self.reuse_addr = reuse_addr
        self._open_socket()

    def _open_socket(self):
        if type(self.addr) == types.TupleType:
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(self.addr)
        self.listen(5)

    def writable(self):
        return 0

    def readable(self):
        return 1

    def handle_accept(self):
        try:
            sock, addr = self.accept()
        except socket.error, msg:
            log("accepted failed: %s" % msg)
            return
        c = self.factory(sock, addr, self.obj)
        log("connect from %s: %s" % (repr(addr), c))
        self.clients.append(c)

class Handler:
    """Base class used to handle RPC caller discovery"""

    def set_caller(self, addr):
        self.__caller = addr

    def get_caller(self):
        return self.__caller

    def clear_caller(self):
        self.__caller = None

_globals = globals()
_silly = ('__doc__',)

def find_global(module, name):
    """Helper for message unpickler"""
    try:
        m = __import__(module, _globals, _globals, _silly)
    except ImportError, msg:
        raise ZRPCError("import error %s: %s" % (module, msg))

    try:
        r = getattr(m, name)
    except AttributeError:
        raise ZRPCError("module %s has no global %s" % (module, name))
        
    safe = getattr(r, '__no_side_effects__', 0)
    if safe:
        return r

    if type(r) == types.ClassType and issubclass(r, Exception):
        return r

    raise ZRPCError("Unsafe global: %s.%s" % (module, name))

