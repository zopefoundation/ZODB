##############################################################################
# 
# Zope Public License (ZPL) Version 1.0
# -------------------------------------
# 
# Copyright (c) Digital Creations.  All rights reserved.
# 
# This license has been certified as Open Source(tm).
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
# 1. Redistributions in source code must retain the above copyright
#    notice, this list of conditions, and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions, and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
# 
# 3. Digital Creations requests that attribution be given to Zope
#    in any manner possible. Zope includes a "Powered by Zope"
#    button that is installed by default. While it is not a license
#    violation to remove this button, it is requested that the
#    attribution remain. A significant investment has been put
#    into Zope, and this effort will continue if the Zope community
#    continues to grow. This is one way to assure that growth.
# 
# 4. All advertising materials and documentation mentioning
#    features derived from or use of this software must display
#    the following acknowledgement:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    In the event that the product being advertised includes an
#    intact Zope distribution (with copyright and license included)
#    then this clause is waived.
# 
# 5. Names associated with Zope or Digital Creations must not be used to
#    endorse or promote products derived from this software without
#    prior written permission from Digital Creations.
# 
# 6. Modified redistributions of any form whatsoever must retain
#    the following acknowledgment:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    Intact (re-)distributions of any official Zope release do not
#    require an external acknowledgement.
# 
# 7. Modifications are encouraged but must be packaged separately as
#    patches to official Zope releases.  Distributions that do not
#    clearly separate the patches from the original work must be clearly
#    labeled as unofficial distributions.  Modifications which do not
#    carry the name Zope may be packaged in any form, as long as they
#    conform to all of the clauses above.
# 
# 
# Disclaimer
# 
#   THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND ANY
#   EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
#   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL CREATIONS OR ITS
#   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
#   USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
#   OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
#   SUCH DAMAGE.
# 
# 
# This software consists of contributions made by Digital Creations and
# many individuals on behalf of Digital Creations.  Specific
# attributions are listed in the accompanying credits file.
# 
##############################################################################
"""Sized message async connections
"""

__version__ = "$Revision: 1.12 $"[11:-2]

import asyncore, struct
from Exceptions import Disconnected
from zLOG import LOG, TRACE, ERROR, INFO, BLATHER
from types import StringType

import socket, errno

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

class SizedMessageAsyncConnection(asyncore.dispatcher):
    __super_init = asyncore.dispatcher.__init__
    __super_close = asyncore.dispatcher.close

    __closed = 1 # Marker indicating that we're closed

    socket = None # to outwit Sam's getattr

    READ_SIZE = 8096

    def __init__(self, sock, addr, map=None, debug=None):
        self.__super_init(sock, map)
        self.addr = addr
        if debug is not None:
            self._debug = debug
        elif not hasattr(self, '_debug'):
            self._debug = __debug__ and 'smac'
        self.__state = None
        self.__inp = None # None, a single String, or a list
        self.__input_len = 0
        self.__msg_size = 4
        self.__output = []
        self.__closed = None

    # XXX avoid expensive getattr calls?
    def __nonzero__(self):
        return 1

    def handle_read(self):
        # Use a single __inp buffer and integer indexes to make this
        # fast.
        try:
            d=self.recv(8096)
        except socket.error, err:
            if err[0] in expected_socket_read_errors:
                return
            raise
        if not d:
            return

        input_len = self.__input_len + len(d)
        msg_size = self.__msg_size
        state = self.__state

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
            if state is None:
                # waiting for message
                msg_size = struct.unpack(">i", msg)[0]
                state = 1
            else:
                msg_size = 4
                state = None
                self.message_input(msg)

        self.__state = state
        self.__msg_size = msg_size
        self.__inp = inp[offset:]
        self.__input_len = input_len - offset

    def readable(self):
        return 1
    
    def writable(self):
        if len(self.__output) == 0:
            return 0
        else:
            return 1

    def handle_write(self):
        output = self.__output
        while output:
            v = output[0]
            try:
                n=self.send(v)
            except socket.error, err:
                if err[0] in expected_socket_write_errors:
                    break # we couldn't write anything
                raise
            if n < len(v):
                output[0] = v[n:]
                break # we can't write any more
            else:
                del output[0]

    def handle_close(self):
        self.close()

    def message_output(self, message):
        if __debug__:
            if self._debug:
                if len(message) > 40:
                    m = message[:40]+' ...'
                else:
                    m = message
                LOG(self._debug, TRACE, 'message_output %s' % `m`)

        if self.__closed is not None:
            raise Disconnected, (
                "This action is temporarily unavailable."
                "<p>"
                )
        # do two separate appends to avoid copying the message string
        self.__output.append(struct.pack(">i", len(message)))
        self.__output.append(message)

    def close(self):
        if self.__closed is None:
            self.__closed = 1
            self.__super_close()
