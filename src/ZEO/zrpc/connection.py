##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
import asyncore
import errno
import select
import sys
import threading
import types
import logging

import ThreadedAsync
from ZEO.zrpc import smac
from ZEO.zrpc.error import ZRPCError, DisconnectedError
from ZEO.zrpc.marshal import Marshaller, ServerMarshaller
from ZEO.zrpc.trigger import trigger
from ZEO.zrpc.log import short_repr, log
from ZODB.loglevels import BLATHER, TRACE

REPLY = ".reply" # message name used for replies
ASYNC = 1

class Delay:
    """Used to delay response to client for synchronous calls.

    When a synchronous call is made and the original handler returns
    without handling the call, it returns a Delay object that prevents
    the mainloop from sending a response.
    """

    def set_sender(self, msgid, send_reply, return_error):
        self.msgid = msgid
        self.send_reply = send_reply
        self.return_error = return_error

    def reply(self, obj):
        self.send_reply(self.msgid, obj)

    def error(self, exc_info):
        log("Error raised in delayed method", logging.ERROR, exc_info=True)
        self.return_error(self.msgid, 0, *exc_info[:2])

class MTDelay(Delay):

    def __init__(self):
        self.ready = threading.Event()

    def set_sender(self, msgid, send_reply, return_error):
        Delay.set_sender(self, msgid, send_reply, return_error)
        self.ready.set()

    def reply(self, obj):
        self.ready.wait()
        Delay.reply(self, obj)

    def error(self, exc_info):
        self.ready.wait()
        Delay.error(self, exc_info)

# PROTOCOL NEGOTIATION
#
# The code implementing protocol version 2.0.0 (which is deployed
# in the field and cannot be changed) *only* talks to peers that
# send a handshake indicating protocol version 2.0.0.  In that
# version, both the client and the server immediately send out
# their protocol handshake when a connection is established,
# without waiting for their peer, and disconnect when a different
# handshake is receive.
#
# The new protocol uses this to enable new clients to talk to
# 2.0.0 servers.  In the new protocol:
#
#    The server sends its protocol handshake to the client at once.
#
#    The client waits until it receives the server's protocol handshake
#    before sending its own handshake.  The client sends the lower of its
#    own protocol version and the server protocol version, allowing it to
#    talk to servers using later protocol versions (2.0.2 and higher) as
#    well:  the effective protocol used will be the lower of the client
#    and server protocol.  However, this changed in ZODB 3.3.1 (and
#    should have changed in ZODB 3.3) because an older server doesn't
#    support MVCC methods required by 3.3 clients.
#
# [Ugly details:  In order to treat the first received message (protocol
#  handshake) differently than all later messages, both client and server
#  start by patching their message_input() method to refer to their
#  recv_handshake() method instead.  In addition, the client has to arrange
#  to queue (delay) outgoing messages until it receives the server's
#  handshake, so that the first message the client sends to the server is
#  the client's handshake.  This multiply-special treatment of the first
#  message is delicate, and several asyncore and thread subtleties were
#  handled unsafely before ZODB 3.2.6.
# ]
#
# The ZEO modules ClientStorage and ServerStub have backwards
# compatibility code for dealing with the previous version of the
# protocol.  The client accepts the old version of some messages,
# and will not send new messages when talking to an old server.
#
# As long as the client hasn't sent its handshake, it can't send
# anything else; output messages are queued during this time.
# (Output can happen because the connection testing machinery can
# start sending requests before the handshake is received.)
#
# UPGRADING FROM ZEO 2.0.0 TO NEWER VERSIONS:
#
# Because a new client can talk to an old server, but not vice
# versa, all clients should be upgraded before upgrading any
# servers.  Protocol upgrades beyond 2.0.1 will not have this
# restriction, because clients using protocol 2.0.1 or later can
# talk to both older and newer servers.
#
# No compatibility with protocol version 1 is provided.

# Connection is abstract (it must be derived from).  ManagedServerConnection
# and ManagedClientConnection are the concrete subclasses.  They need to
# supply a handshake() method appropriate for their role in protocol
# negotiation.

class Connection(smac.SizedMessageAsyncConnection, object):
    """Dispatcher for RPC on object on both sides of socket.

    The connection supports synchronous calls, which expect a return,
    and asynchronous calls, which do not.

    It uses the Marshaller class to handle encoding and decoding of
    method calls and arguments.  Marshaller uses pickle to encode
    arbitrary Python objects.  The code here doesn't ever see the wire
    format.

    A Connection is designed for use in a multithreaded application,
    where a synchronous call must block until a response is ready.

    A socket connection between a client and a server allows either
    side to invoke methods on the other side.  The processes on each
    end of the socket use a Connection object to manage communication.

    The Connection deals with decoded RPC messages.  They are
    represented as four-tuples containing: msgid, flags, method name,
    and a tuple of method arguments.

    The msgid starts at zero and is incremented by one each time a
    method call message is sent.  Each side of the connection has a
    separate msgid state.

    When one side of the connection (the client) calls a method, it
    sends a message with a new msgid.  The other side (the server),
    replies with a message that has the same msgid, the string
    ".reply" (the global variable REPLY) as the method name, and the
    actual return value in the args position.  Note that each side of
    the Connection can initiate a call, in which case it will be the
    client for that particular call.

    The protocol also supports asynchronous calls.  The client does
    not wait for a return value for an asynchronous call.  The only
    defined flag is ASYNC.  If a method call message has the ASYNC
    flag set, the server will raise an exception.

    If a method call raises an Exception, the exception is propagated
    back to the client via the REPLY message.  The client side will
    raise any exception it receives instead of returning the value to
    the caller.
    """

    __super_init = smac.SizedMessageAsyncConnection.__init__
    __super_close = smac.SizedMessageAsyncConnection.close
    __super_setSessionKey = smac.SizedMessageAsyncConnection.setSessionKey

    # Protocol history:
    #
    # Z200 -- Original ZEO 2.0 protocol
    #
    # Z201 -- Added invalidateTransaction() to client.
    #         Renamed several client methods.
    #         Added several sever methods:
    #             lastTransaction()
    #             getAuthProtocol() and scheme-specific authentication methods
    #             getExtensionMethods().
    #             getInvalidations().
    #
    # Z303 -- named after the ZODB release 3.3
    #         Added methods for MVCC:
    #             loadBefore()
    #             loadEx()
    #         A Z303 client cannot talk to a Z201 server, because the latter
    #         doesn't support MVCC.  A Z201 client can talk to a Z303 server,
    #         but because (at least) the type of the root object changed
    #         from ZODB.PersistentMapping to persistent.mapping, the older
    #         client can't actually make progress if a Z303 client created,
    #         or ever modified, the root.

    # Protocol variables:
    # Our preferred protocol.
    current_protocol = "Z303"

    # If we're a client, an exhaustive list of the server protocols we
    # can accept.
    servers_we_can_talk_to = [current_protocol]

    # If we're a server, an exhaustive list of the client protocols we
    # can accept.
    clients_we_can_talk_to = ["Z200", "Z201", current_protocol]

    # This is pretty excruciating.  Details:
    #
    # 3.3 server 3.2 client
    #     server sends Z303 to client
    #     client computes min(Z303, Z201) == Z201 as the protocol to use
    #     client sends Z201 to server
    #     OK, because Z201 is in the server's clients_we_can_talk_to
    #
    # 3.2 server 3.3 client
    #     server sends Z201 to client
    #     client computes min(Z303, Z201) == Z201 as the protocol to use
    #     Z201 isn't in the client's servers_we_can_talk_to, so client
    #         raises exception
    #
    # 3.3 server 3.3 client
    #     server sends Z303 to client
    #     client computes min(Z303, Z303) == Z303 as the protocol to use
    #     Z303 is in the client's servers_we_can_talk_to, so client
    #         sends Z303 to server
    #     OK, because Z303 is in the server's clients_we_can_talk_to

    # Client constructor passes 'C' for tag, server constructor 'S'.  This
    # is used in log messages, and to determine whether we can speak with
    # our peer.
    def __init__(self, sock, addr, obj, tag):
        self.obj = None
        self.marshal = Marshaller()
        self.closed = False
        self.peer_protocol_version = None # set in recv_handshake()

        assert tag in "CS"
        self.tag = tag
        self.logger = logging.getLogger('ZEO.zrpc.Connection(%c)' % tag)
        if isinstance(addr, types.TupleType):
            self.log_label = "(%s:%d) " % addr
        else:
            self.log_label = "(%s) " % addr

        # Supply our own socket map, so that we don't get registered with
        # the asyncore socket map just yet.  The initial protocol messages
        # are treated very specially, and we dare not get invoked by asyncore
        # before that special-case setup is complete.  Some of that setup
        # occurs near the end of this constructor, and the rest is done by
        # a concrete subclass's handshake() method.  Unfortunately, because
        # we ultimately derive from asyncore.dispatcher, it's not possible
        # to invoke the superclass constructor without asyncore stuffing
        # us into _some_ socket map.
        ourmap = {}
        self.__super_init(sock, addr, map=ourmap)

        # A Connection either uses asyncore directly or relies on an
        # asyncore mainloop running in a separate thread.  If
        # thr_async is true, then the mainloop is running in a
        # separate thread.  If thr_async is true, then the asyncore
        # trigger (self.trigger) is used to notify that thread of
        # activity on the current thread.
        self.thr_async = False
        self.trigger = None
        self._prepare_async()

        # The singleton dict is used in synchronous mode when a method
        # needs to call into asyncore to try to force some I/O to occur.
        # The singleton dict is a socket map containing only this object.
        self._singleton = {self._fileno: self}

        # msgid_lock guards access to msgid
        self.msgid = 0
        self.msgid_lock = threading.Lock()

        # replies_cond is used to block when a synchronous call is
        # waiting for a response
        self.replies_cond = threading.Condition()
        self.replies = {}

        # waiting_for_reply is used internally to indicate whether
        # a call is in progress.  setting a session key is deferred
        # until after the call returns.
        self.waiting_for_reply = False
        self.delay_sesskey = None
        self.register_object(obj)

        # The first message we see is a protocol handshake.  message_input()
        # is temporarily replaced by recv_handshake() to treat that message
        # specially.  revc_handshake() does "del self.message_input", which
        # uncovers the normal message_input() method thereafter.
        self.message_input = self.recv_handshake

        # Server and client need to do different things for protocol
        # negotiation, and handshake() is implemented differently in each.
        self.handshake()

        # Now it's safe to register with asyncore's socket map; it was not
        # safe before message_input was replaced, or before handshake() was
        # invoked.
        # Obscure:  in Python 2.4, the base asyncore.dispatcher class grew
        # a ._map attribute, which is used instead of asyncore's global
        # socket map when ._map isn't None.  Because we passed `ourmap` to
        # the base class constructor above, in 2.4 asyncore believes we want
        # to use `ourmap` instead of the global socket map -- but we don't.
        # So we have to replace our ._map with the global socket map, and
        # update the global socket map with `ourmap`.  Replacing our ._map
        # isn't necessary before Python 2.4, but doesn't hurt then (it just
        # gives us an unused attribute in 2.3); updating the global socket
        # map is necessary regardless of Python version.
        self._map = asyncore.socket_map
        asyncore.socket_map.update(ourmap)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.addr)

    __str__ = __repr__ # Defeat asyncore's dreaded __getattr__

    def log(self, message, level=BLATHER, exc_info=False):
        self.logger.log(level, self.log_label + message, exc_info=exc_info)

    def close(self):
        if self.closed:
            return
        self._singleton.clear()
        self.closed = True
        self.close_trigger()
        self.__super_close()

    def close_trigger(self):
        # Overridden by ManagedClientConnection.
        if self.trigger is not None:
            self.trigger.close()

    def register_object(self, obj):
        """Register obj as the true object to invoke methods on."""
        self.obj = obj

    # Subclass must implement.  handshake() is called by the constructor,
    # near its end, but before self is added to asyncore's socket map.
    # When a connection is created the first message sent is a 4-byte
    # protocol version.  This allows the protocol to evolve over time, and
    # lets servers handle clients using multiple versions of the protocol.
    # In general, the server's handshake() just needs to send the server's
    # preferred protocol; the client's also needs to queue (delay) outgoing
    # messages until it sees the handshake from the server.
    def handshake(self):
        raise NotImplementedError

    # Replaces message_input() for the first message received.  Records the
    # protocol sent by the peer in `peer_protocol_version`, restores the
    # normal message_input() method, and raises an exception if the peer's
    # protocol is unacceptable.  That's all the server needs to do.  The
    # client needs to do additional work in response to the server's
    # handshake, and extends this method.
    def recv_handshake(self, proto):
        # Extended by ManagedClientConnection.
        del self.message_input  # uncover normal-case message_input()
        self.peer_protocol_version = proto

        if self.tag == 'C':
            good_protos = self.servers_we_can_talk_to
        else:
            assert self.tag == 'S'
            good_protos = self.clients_we_can_talk_to

        if proto in good_protos:
            self.log("received handshake %r" % proto, level=logging.INFO)
        else:
            self.log("bad handshake %s" % short_repr(proto),
                     level=logging.ERROR)
            raise ZRPCError("bad handshake %r" % proto)

    def message_input(self, message):
        """Decode an incoming message and dispatch it"""
        # If something goes wrong during decoding, the marshaller
        # will raise an exception.  The exception will ultimately
        # result in asycnore calling handle_error(), which will
        # close the connection.
        msgid, flags, name, args = self.marshal.decode(message)

        if __debug__:
            self.log("recv msg: %s, %s, %s, %s" % (msgid, flags, name,
                                                   short_repr(args)),
                     level=TRACE)
        if name == REPLY:
            self.handle_reply(msgid, flags, args)
        else:
            self.handle_request(msgid, flags, name, args)

    def handle_reply(self, msgid, flags, args):
        if __debug__:
            self.log("recv reply: %s, %s, %s"
                     % (msgid, flags, short_repr(args)), level=TRACE)
        self.replies_cond.acquire()
        try:
            self.replies[msgid] = flags, args
            self.replies_cond.notifyAll()
        finally:
            self.replies_cond.release()

    def handle_request(self, msgid, flags, name, args):
        if not self.check_method(name):
            msg = "Invalid method name: %s on %s" % (name, repr(self.obj))
            raise ZRPCError(msg)
        if __debug__:
            self.log("calling %s%s" % (name, short_repr(args)),
                     level=logging.DEBUG)

        meth = getattr(self.obj, name)
        try:
            self.waiting_for_reply = True
            try:
                ret = meth(*args)
            finally:
                self.waiting_for_reply = False
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception, msg:
            self.log("%s() raised exception: %s" % (name, msg), logging.INFO,
                     exc_info=True)
            error = sys.exc_info()[:2]
            return self.return_error(msgid, flags, *error)

        if flags & ASYNC:
            if ret is not None:
                raise ZRPCError("async method %s returned value %s" %
                                (name, short_repr(ret)))
        else:
            if __debug__:
                self.log("%s returns %s" % (name, short_repr(ret)),
                         logging.DEBUG)
            if isinstance(ret, Delay):
                ret.set_sender(msgid, self.send_reply, self.return_error)
            else:
                self.send_reply(msgid, ret)

        if self.delay_sesskey:
            self.__super_setSessionKey(self.delay_sesskey)
            self.delay_sesskey = None

    def handle_error(self):
        if sys.exc_info()[0] == SystemExit:
            raise sys.exc_info()
        self.log("Error caught in asyncore",
                 level=logging.ERROR, exc_info=True)
        self.close()

    def check_method(self, name):
        # TODO:  This is hardly "secure".
        if name.startswith('_'):
            return None
        return hasattr(self.obj, name)

    def send_reply(self, msgid, ret):
        # encode() can pass on a wide variety of exceptions from cPickle.
        # While a bare `except` is generally poor practice, in this case
        # it's acceptable -- we really do want to catch every exception
        # cPickle may raise.
        try:
            msg = self.marshal.encode(msgid, 0, REPLY, ret)
        except: # see above
            try:
                r = short_repr(ret)
            except:
                r = "<unreprable>"
            err = ZRPCError("Couldn't pickle return %.100s" % r)
            msg = self.marshal.encode(msgid, 0, REPLY, (ZRPCError, err))
        self.message_output(msg)
        self.poll()

    def return_error(self, msgid, flags, err_type, err_value):
        if flags & ASYNC:
            self.log("Asynchronous call raised exception: %s" % self,
                     level=logging.ERROR, exc_info=True)
            return
        if type(err_value) is not types.InstanceType:
            err_value = err_type, err_value

        # encode() can pass on a wide variety of exceptions from cPickle.
        # While a bare `except` is generally poor practice, in this case
        # it's acceptable -- we really do want to catch every exception
        # cPickle may raise.
        try:
            msg = self.marshal.encode(msgid, 0, REPLY, (err_type, err_value))
        except: # see above
            try:
                r = short_repr(err_value)
            except:
                r = "<unreprable>"
            err = ZRPCError("Couldn't pickle error %.100s" % r)
            msg = self.marshal.encode(msgid, 0, REPLY, (ZRPCError, err))
        self.message_output(msg)
        self.poll()

    def setSessionKey(self, key):
        if self.waiting_for_reply:
            self.delay_sesskey = key
        else:
            self.__super_setSessionKey(key)

    # The next two public methods (call and callAsync) are used by
    # clients to invoke methods on remote objects

    def send_call(self, method, args, flags):
        # send a message and return its msgid
        self.msgid_lock.acquire()
        try:
            msgid = self.msgid
            self.msgid = self.msgid + 1
        finally:
            self.msgid_lock.release()
        if __debug__:
            self.log("send msg: %d, %d, %s, ..." % (msgid, flags, method),
                     level=TRACE)
        buf = self.marshal.encode(msgid, flags, method, args)
        self.message_output(buf)
        return msgid

    def call(self, method, *args):
        if self.closed:
            raise DisconnectedError()
        msgid = self.send_call(method, args, 0)
        r_flags, r_args = self.wait(msgid)
        if (isinstance(r_args, types.TupleType) and len(r_args) > 1
            and type(r_args[0]) == types.ClassType
            and issubclass(r_args[0], Exception)):
            inst = r_args[1]
            raise inst # error raised by server
        else:
            return r_args

    # For testing purposes, it is useful to begin a synchronous call
    # but not block waiting for its response.  Since these methods are
    # used for testing they can assume they are not in async mode and
    # call asyncore.poll() directly to get the message out without
    # also waiting for the reply.

    def _deferred_call(self, method, *args):
        if self.closed:
            raise DisconnectedError()
        msgid = self.send_call(method, args, 0)
        asyncore.poll(0.01, self._singleton)
        return msgid

    def _deferred_wait(self, msgid):
        r_flags, r_args = self.wait(msgid)
        if (isinstance(r_args, types.TupleType)
            and type(r_args[0]) == types.ClassType
            and issubclass(r_args[0], Exception)):
            inst = r_args[1]
            raise inst # error raised by server
        else:
            return r_args

    def callAsync(self, method, *args):
        if self.closed:
            raise DisconnectedError()
        self.send_call(method, args, ASYNC)
        self.poll()

    def callAsyncNoPoll(self, method, *args):
        # Like CallAsync but doesn't poll.  This exists so that we can
        # send invalidations atomically to all clients without
        # allowing any client to sneak in a load request.
        if self.closed:
            raise DisconnectedError()
        self.send_call(method, args, ASYNC)

    # handle IO, possibly in async mode

    def _prepare_async(self):
        self.thr_async = False
        ThreadedAsync.register_loop_callback(self.set_async)
        # TODO:  If we are not in async mode, this will cause dead
        # Connections to be leaked.

    def set_async(self, map):
        self.trigger = trigger()
        self.thr_async = True

    def is_async(self):
        # Overridden by ManagedConnection
        if self.thr_async:
            return 1
        else:
            return 0

    def _pull_trigger(self, tryagain=10):
        try:
            self.trigger.pull_trigger()
        except OSError:
            self.trigger.close()
            self.trigger = trigger()
            if tryagain > 0:
                self._pull_trigger(tryagain=tryagain-1)

    def wait(self, msgid):
        """Invoke asyncore mainloop and wait for reply."""
        if __debug__:
            self.log("wait(%d), async=%d" % (msgid, self.is_async()),
                     level=TRACE)
        if self.is_async():
            self._pull_trigger()

        # Delay used when we call asyncore.poll() directly.
        # Start with a 1 msec delay, double until 1 sec.
        delay = 0.001

        self.replies_cond.acquire()
        try:
            while 1:
                if self.closed:
                    raise DisconnectedError()
                reply = self.replies.get(msgid)
                if reply is not None:
                    del self.replies[msgid]
                    if __debug__:
                        self.log("wait(%d): reply=%s" %
                                 (msgid, short_repr(reply)), level=TRACE)
                    return reply
                if self.is_async():
                    self.replies_cond.wait(10.0)
                else:
                    self.replies_cond.release()
                    try:
                        try:
                            if __debug__:
                                self.log("wait(%d): asyncore.poll(%s)" %
                                         (msgid, delay), level=TRACE)
                            asyncore.poll(delay, self._singleton)
                            if delay < 1.0:
                                delay += delay
                        except select.error, err:
                            self.log("Closing.  asyncore.poll() raised %s."
                                     % err, level=BLATHER)
                            self.close()
                    finally:
                        self.replies_cond.acquire()
        finally:
            self.replies_cond.release()

    def flush(self):
        """Invoke poll() until the output buffer is empty."""
        if __debug__:
            self.log("flush")
        while self.writable():
            self.poll()

    def poll(self):
        """Invoke asyncore mainloop to get pending message out."""
        if __debug__:
            self.log("poll(), async=%d" % self.is_async(), level=TRACE)
        if self.is_async():
            self._pull_trigger()
        else:
            asyncore.poll(0.0, self._singleton)

    def pending(self, timeout=0):
        """Invoke mainloop until any pending messages are handled."""
        if __debug__:
            self.log("pending(), async=%d" % self.is_async(), level=TRACE)
        if self.is_async():
            return
        # Inline the asyncore poll() function to know whether any input
        # was actually read.  Repeat until no input is ready.

        # Pending does reads and writes.  In the case of server
        # startup, we may need to write out zeoVerify() messages.
        # Always check for read status, but don't check for write status
        # only there is output to do.  Only continue in this loop as
        # long as there is data to read.
        r = r_in = [self._fileno]
        x_in = []
        while r and not self.closed:
            if self.writable():
                w_in = [self._fileno]
            else:
                w_in = []
            try:
                r, w, x = select.select(r_in, w_in, x_in, timeout)
            except select.error, err:
                if err[0] == errno.EINTR:
                    timeout = 0
                    continue
                else:
                    raise
            else:
                # Make sure any subsequent select does not block.  The
                # loop is only intended to make sure all incoming data is
                # returned.

                # Insecurity:  What if the server sends a lot of
                # invalidations, such that pending never finishes?  Seems
                # unlikely, but possible.
                timeout = 0
            if r:
                try:
                    self.handle_read_event()
                except asyncore.ExitNow:
                    raise
                except:
                    self.handle_error()
            if w:
                try:
                    self.handle_write_event()
                except asyncore.ExitNow:
                    raise
                except:
                    self.handle_error()

class ManagedServerConnection(Connection):
    """Server-side Connection subclass."""
    __super_init = Connection.__init__
    __super_close = Connection.close

    def __init__(self, sock, addr, obj, mgr):
        self.mgr = mgr
        self.__super_init(sock, addr, obj, 'S')
        self.marshal = ServerMarshaller()
        self.obj.notifyConnected(self)

    def handshake(self):
        # Send the server's preferred protocol to the client.
        self.message_output(self.current_protocol)

    def close(self):
        self.obj.notifyDisconnected()
        self.mgr.close_conn(self)
        self.__super_close()

class ManagedClientConnection(Connection):
    """Client-side Connection subclass."""
    __super_init = Connection.__init__
    __super_close = Connection.close
    base_message_output = Connection.message_output

    def __init__(self, sock, addr, obj, mgr):
        self.mgr = mgr

        # We can't use the base smac's message_output directly because the
        # client needs to queue outgoing messages until it's seen the
        # initial protocol handshake from the server.  So we have our own
        # message_ouput() method, and support for initial queueing.  This is
        # a delicate design, requiring an output mutex to be wholly
        # thread-safe.
        # Caution:  we must set this up before calling the base class
        # constructor, because the latter registers us with asyncore;
        # we need to guarantee that we'll queue outgoing messages before
        # asyncore learns about us.
        self.output_lock = threading.Lock()
        self.queue_output = True
        self.queued_messages = []

        self.__super_init(sock, addr, obj, tag='C')
        self.check_mgr_async()

    # Our message_ouput() queues messages until recv_handshake() gets the
    # protocol handshake from the server.
    def message_output(self, message):
        self.output_lock.acquire()
        try:
            if self.queue_output:
                self.queued_messages.append(message)
            else:
                assert not self.queued_messages
                self.base_message_output(message)
        finally:
            self.output_lock.release()

    def handshake(self):
        # The client waits to see the server's handshake.  Outgoing messages
        # are queued for the duration.  The client will send its own
        # handshake after the server's handshake is seen, in recv_handshake()
        # below.  It will then send any messages queued while waiting.
        assert self.queue_output # the constructor already set this

    def recv_handshake(self, proto):
        # The protocol to use is the older of our and the server's preferred
        # protocols.
        proto = min(proto, self.current_protocol)

        # Restore the normal message_input method, and raise an exception
        # if the protocol version is too old.
        Connection.recv_handshake(self, proto)

        # Tell the server the protocol in use, then send any messages that
        # were queued while waiting to hear the server's protocol, and stop
        # queueing messages.
        self.output_lock.acquire()
        try:
            self.base_message_output(proto)
            for message in self.queued_messages:
                self.base_message_output(message)
            self.queued_messages = []
            self.queue_output = False
        finally:
            self.output_lock.release()

    # Defer the ThreadedAsync work to the manager.

    def close_trigger(self):
        # the manager should actually close the trigger
        # TODO: what is that comment trying to say?  What 'manager'?
        del self.trigger

    def set_async(self, map):
        pass

    def _prepare_async(self):
        # Don't do the register_loop_callback that the superclass does
        pass

    def check_mgr_async(self):
        if not self.thr_async and self.mgr.thr_async:
            assert self.mgr.trigger is not None, \
                   "manager (%s) has no trigger" % self.mgr
            self.thr_async = True
            self.trigger = self.mgr.trigger
            return 1
        return 0

    def is_async(self):
        # TODO: could the check_mgr_async() be avoided on each test?
        if self.thr_async:
            return 1
        return self.check_mgr_async()

    def close(self):
        self.mgr.close_conn(self)
        self.__super_close()
