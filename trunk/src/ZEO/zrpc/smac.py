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
"""Sized Message Async Connections.

This class extends the basic asyncore layer with a record-marking
layer.  The message_output() method accepts an arbitrary sized string
as its argument.  It sends over the wire the length of the string
encoded using struct.pack('>I') and the string itself.  The receiver
passes the original string to message_input().

This layer also supports an optional message authentication code
(MAC).  If a session key is present, it uses HMAC-SHA-1 to generate a
20-byte MAC.  If a MAC is present, the high-order bit of the length
is set to 1 and the MAC immediately follows the length.
"""

import asyncore
import errno
try:
    import hmac
except ImportError:
    import _hmac as hmac
import sha
import socket
import struct
import threading
from types import StringType

from ZEO.zrpc.log import log, short_repr
from ZEO.zrpc.error import DisconnectedError
import zLOG


# Use the dictionary to make sure we get the minimum number of errno
# entries.   We expect that EWOULDBLOCK == EAGAIN on most systems --
# or that only one is actually used.

tmp_dict = {errno.EWOULDBLOCK: 0,
            errno.EAGAIN: 0,
            errno.EINTR: 0,
            }
expected_socket_read_errors = tuple(tmp_dict.keys())

tmp_dict = {errno.EAGAIN: 0,
            errno.EWOULDBLOCK: 0,
            errno.ENOBUFS: 0,
            errno.EINTR: 0,
            }
expected_socket_write_errors = tuple(tmp_dict.keys())
del tmp_dict

# We chose 60000 as the socket limit by looking at the largest strings
# that we could pass to send() without blocking.
SEND_SIZE = 60000

MAC_BIT = 0x80000000L

class SizedMessageAsyncConnection(asyncore.dispatcher):
    __super_init = asyncore.dispatcher.__init__
    __super_close = asyncore.dispatcher.close

    __closed = 1 # Marker indicating that we're closed

    socket = None # to outwit Sam's getattr

    def __init__(self, sock, addr, map=None, debug=None):
        self.addr = addr
        if debug is not None:
            self._debug = debug
        elif not hasattr(self, '_debug'):
            self._debug = __debug__
        # __input_lock protects __inp, __input_len, __state, __msg_size
        self.__input_lock = threading.Lock()
        self.__inp = None # None, a single String, or a list
        self.__input_len = 0
        # Instance variables __state, __msg_size and __has_mac work together:
        #   when __state == 0:
        #     __msg_size == 4, and the next thing read is a message size;
        #     __has_mac is set according to the MAC_BIT in the header
        #   when __state == 1:
        #     __msg_size is variable, and the next thing read is a message.
        #     __has_mac indicates if we're in MAC mode or not (and
        #               therefore, if we need to check the mac header)
        # The next thing read is always of length __msg_size.
        # The state alternates between 0 and 1.
        self.__state = 0
        self.__has_mac = True
        self.__msg_size = 4
        self.__output_lock = threading.Lock() # Protects __output
        self.__output = []
        self.__closed = 0
        # Each side of the connection sends and receives messages.  A
        # MAC is generated for each message and depends on each
        # previous MAC; the state of the MAC generator depends on the
        # history of operations it has performed.  So the MACs must be
        # generated in the same order they are verified.

        # Each side is guaranteed to receive messages in the order
        # they are sent, but there is no ordering constraint between
        # message sends and receives.  If the two sides are A and B
        # and message An indicates the nth message sent by A, then
        # A1 A2 B1 B2 and A1 B1 B2 A2 are both legitimate total
        # orderings of the messages.

        # As a result, there must be seperate MAC generators for each
        # side of the connection.  If not, the generator state would
        # be different after A1 A2 B1 B2 than it would be after
        # A1 B1 B2 A2; if the generator state was different, the MAC
        # could not be verified.
        self.__hmac_send = None
        self.__hmac_recv = None

        self.__super_init(sock, map)

    def setSessionKey(self, sesskey):
        log("set session key %r" % sesskey)
        self.__hmac_send = hmac.HMAC(sesskey, digestmod=sha)
        self.__hmac_recv = hmac.HMAC(sesskey, digestmod=sha)

    def get_addr(self):
        return self.addr

    # XXX avoid expensive getattr calls?  Can't remember exactly what
    # this comment was supposed to mean, but it has something to do
    # with the way asyncore uses getattr and uses if sock:
    def __nonzero__(self):
        return 1

    def handle_read(self):
        self.__input_lock.acquire()
        try:
            # Use a single __inp buffer and integer indexes to make this fast.
            try:
                d = self.recv(8192)
            except socket.error, err:
                if err[0] in expected_socket_read_errors:
                    return
                raise
            if not d:
                return

            input_len = self.__input_len + len(d)
            msg_size = self.__msg_size
            state = self.__state
            has_mac = self.__has_mac

            inp = self.__inp
            if msg_size > input_len:
                if inp is None:
                    self.__inp = d
                elif type(self.__inp) is StringType:
                    self.__inp = [self.__inp, d]
                else:
                    self.__inp.append(d)
                self.__input_len = input_len
                return # keep waiting for more input

            # load all previous input and d into single string inp
            if isinstance(inp, StringType):
                inp = inp + d
            elif inp is None:
                inp = d
            else:
                inp.append(d)
                inp = "".join(inp)

            offset = 0
            while (offset + msg_size) <= input_len:
                msg = inp[offset:offset + msg_size]
                offset = offset + msg_size
                if not state:
                    msg_size = struct.unpack(">I", msg)[0]
                    has_mac = msg_size & MAC_BIT
                    if has_mac:
                        msg_size ^= MAC_BIT
                        msg_size += 20
                    elif self.__hmac_send:
                        raise ValueError("Received message without MAC")
                    state = 1
                else:
                    msg_size = 4
                    state = 0
                    # XXX We call message_input() with __input_lock
                    # held!!!  And message_input() may end up calling
                    # message_output(), which has its own lock.  But
                    # message_output() cannot call message_input(), so
                    # the locking order is always consistent, which
                    # prevents deadlock.  Also, message_input() may
                    # take a long time, because it can cause an
                    # incoming call to be handled.  During all this
                    # time, the __input_lock is held.  That's a good
                    # thing, because it serializes incoming calls.
                    if has_mac:
                        mac = msg[:20]
                        msg = msg[20:]
                        if self.__hmac_recv:
                            self.__hmac_recv.update(msg)
                            _mac = self.__hmac_recv.digest()
                            if mac != _mac:
                                raise ValueError("MAC failed: %r != %r"
                                                 % (_mac, mac))
                        else:
                            log("Received MAC but no session key set")
                    elif self.__hmac_send:
                        raise ValueError("Received message without MAC")
                    self.message_input(msg)

            self.__state = state
            self.__has_mac = has_mac
            self.__msg_size = msg_size
            self.__inp = inp[offset:]
            self.__input_len = input_len - offset
        finally:
            self.__input_lock.release()

    def readable(self):
        return 1

    def writable(self):
        if len(self.__output) == 0:
            return 0
        else:
            return 1

    def handle_write(self):
        self.__output_lock.acquire()
        try:
            output = self.__output
            while output:
                # Accumulate output into a single string so that we avoid
                # multiple send() calls, but avoid accumulating too much
                # data.  If we send a very small string and have more data
                # to send, we will likely incur delays caused by the
                # unfortunate interaction between the Nagle algorithm and
                # delayed acks.  If we send a very large string, only a
                # portion of it will actually be delivered at a time.

                l = 0
                for i in range(len(output)):
                    l += len(output[i])
                    if l > SEND_SIZE:
                        break

                i += 1
                # It is very unlikely that i will be 1.
                v = "".join(output[:i])
                del output[:i]

                try:
                    n = self.send(v)
                except socket.error, err:
                    if err[0] in expected_socket_write_errors:
                        break # we couldn't write anything
                    raise
                if n < len(v):
                    output.insert(0, v[n:])
                    break # we can't write any more
        finally:
            self.__output_lock.release()

    def handle_close(self):
        self.close()

    def message_output(self, message):
        if __debug__:
            if self._debug:
                log("message_output %d bytes: %s hmac=%d" %
                    (len(message), short_repr(message),
                    self.__hmac_send and 1 or 0),
                    level=zLOG.TRACE)

        if self.__closed:
            raise DisconnectedError(
                "This action is temporarily unavailable.<p>")
        self.__output_lock.acquire()
        try:
            # do two separate appends to avoid copying the message string
            if self.__hmac_send:
                self.__output.append(struct.pack(">I", len(message) | MAC_BIT))
                self.__hmac_send.update(message)
                self.__output.append(self.__hmac_send.digest())
            else:
                self.__output.append(struct.pack(">I", len(message)))
            if len(message) <= SEND_SIZE:
                self.__output.append(message)
            else:
                for i in range(0, len(message), SEND_SIZE):
                    self.__output.append(message[i:i+SEND_SIZE])
        finally:
            self.__output_lock.release()

    def close(self):
        if not self.__closed:
            self.__closed = 1
            self.__super_close()
