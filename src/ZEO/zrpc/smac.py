##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
import socket
import struct
import threading

from ZEO.zrpc.log import log
from ZEO.zrpc.error import DisconnectedError
import ZEO.hash


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

_close_marker = object()

class SizedMessageAsyncConnection(asyncore.dispatcher):
    __super_init = asyncore.dispatcher.__init__
    __super_close = asyncore.dispatcher.close

    __closed = True # Marker indicating that we're closed

    socket = None # to outwit Sam's getattr

    def __init__(self, sock, addr, map=None):
        self.addr = addr
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
        self.__has_mac = 0
        self.__msg_size = 4
        self.__output_messages = []
        self.__output = []
        self.__closed = False
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

        # asyncore overwrites addr with the getpeername result
        # restore our value
        self.addr = addr

    def setSessionKey(self, sesskey):
        log("set session key %r" % sesskey)

        # Low-level construction is now delayed until data are sent.
        # This is to allow use of iterators that generate messages
        # only when we're ready to do I/O so that we can effeciently
        # transmit large files.  Because we delay messages, we also
        # have to delay setting the session key to retain proper
        # ordering.

        # The low-level output queue supports strings, a special close
        # marker, and iterators.  It doesn't support callbacks.  We
        # can create a allback by providing an iterator that doesn't
        # yield anything.

        # The hack fucntion below is a callback in iterator's
        # clothing. :)  It never yields anything, but is a generator
        # and thus iterator, because it contains a yield statement.

        def hack():
            self.__hmac_send = hmac.HMAC(sesskey, digestmod=ZEO.hash)
            self.__hmac_recv = hmac.HMAC(sesskey, digestmod=ZEO.hash)
            if False:
                yield ''

        self.message_output(hack())

    def get_addr(self):
        return self.addr

    # TODO: avoid expensive getattr calls?  Can't remember exactly what
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
                elif type(self.__inp) is str:
                    self.__inp = [self.__inp, d]
                else:
                    self.__inp.append(d)
                self.__input_len = input_len
                return # keep waiting for more input

            # load all previous input and d into single string inp
            if isinstance(inp, str):
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
                    # Obscure:  We call message_input() with __input_lock
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
        return True

    def writable(self):
        return bool(self.__output_messages or self.__output)

    def should_close(self):
        self.__output_messages.append(_close_marker)

    def handle_write(self):
        output = self.__output
        messages = self.__output_messages
        while output or messages:

            # Process queued messages until we have enough output
            size = sum((len(s) for s in output))
            while (size <= SEND_SIZE) and messages:
                message = messages[0]
                if message.__class__ is str:
                    size += self.__message_output(messages.pop(0), output)
                elif message is _close_marker:
                    del messages[:]
                    del output[:]
                    return self.close()
                else:
                    try:
                        message = message.next()
                    except StopIteration:
                        messages.pop(0)
                    else:
                        size += self.__message_output(message, output)


            v = "".join(output)
            del output[:]

            try:
                n = self.send(v)
            except socket.error, err:
                # Fix for https://bugs.launchpad.net/zodb/+bug/182833
                #  ensure the above mentioned "output" invariant
                output.insert(0, v)
                if err[0] in expected_socket_write_errors:
                    break # we couldn't write anything
                raise

            if n < len(v):
                output.append(v[n:])
                break # we can't write any more

    def handle_close(self):
        self.close()

    def message_output(self, message):
        if self.__closed:
            raise DisconnectedError(
                "This action is temporarily unavailable.<p>")
        self.__output_messages.append(message)

    def __message_output(self, message, output):
        # do two separate appends to avoid copying the message string
        size = 4
        if self.__hmac_send:
            output.append(struct.pack(">I", len(message) | MAC_BIT))
            self.__hmac_send.update(message)
            output.append(self.__hmac_send.digest())
            size += 20
        else:
            output.append(struct.pack(">I", len(message)))

        if len(message) <= SEND_SIZE:
            output.append(message)
        else:
            for i in range(0, len(message), SEND_SIZE):
                output.append(message[i:i+SEND_SIZE])

        return size + len(message)

    def close(self):
        if not self.__closed:
            self.__closed = True
            self.__super_close()
