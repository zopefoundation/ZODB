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
"""Sized message async connections
"""

__version__ = "$Revision: 1.16 $"[11:-2]

import asyncore, string, struct, zLOG, sys, Acquisition
import socket, errno
from logger import zLogger

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

class SizedMessageAsyncConnection(Acquisition.Explicit, asyncore.dispatcher):

    __append=None # Marker indicating that we're closed

    socket=None # to outwit Sam's getattr

    def __init__(self, sock, addr, map=None, debug=None):
        SizedMessageAsyncConnection.inheritedAttribute(
            '__init__')(self, sock, map)
        self.addr=addr
        if debug is None and __debug__:
            self._debug = zLogger("smac")
        else:
            self._debug = debug
        self.__state=None
        self.__inp=None
        self.__inpl=0
        self.__l=4
        self.__output=output=[]
        self.__append=output.append
        self.__pop=output.pop

    def handle_read(self,
                    join=string.join, StringType=type(''), _type=type,
                    _None=None):

        try:
            d=self.recv(8096)
        except socket.error, err:
            if err[0] in expected_socket_read_errors:
                return
            raise
        if not d: return

        inp=self.__inp
        if inp is _None:
            inp=d
        elif _type(inp) is StringType:
            inp=[inp,d]
        else:
            inp.append(d)

        inpl=self.__inpl+len(d)
        l=self.__l
            
        while 1:

            if l <= inpl:
                # Woo hoo, we have enough data
                if _type(inp) is not StringType: inp=join(inp,'')
                d=inp[:l]
                inp=inp[l:]
                inpl=inpl-l                
                if self.__state is _None:
                    # waiting for message
                    l=struct.unpack(">i",d)[0]
                    self.__state=1
                else:
                    l=4
                    self.__state=_None
                    self.message_input(d)
            else:
                break # not enough data
                
        self.__l=l
        self.__inp=inp
        self.__inpl=inpl

    def readable(self): return 1
    def writable(self): return not not self.__output

    def handle_write(self):
        output=self.__output
        while output:
            v=output[0]
            try:
                n=self.send(v)
            except socket.error, err:
                if err[0] in expected_socket_write_errors:
                    break # we couldn't write anything
                raise
            if n < len(v):
                output[0]=v[n:]
                break # we can't write any more
            else:
                del output[0]
                #break # waaa


    def handle_close(self):
        self.close()

    def message_output(self, message,
                       pack=struct.pack, len=len):
        if self._debug is not None:
            if len(message) > 40:
                m = message[:40]+' ...'
            else:
                m = message
            self._debug.trace('message_output %s' % `m`)

        append=self.__append
        if append is None:
            raise Disconnected("This action is temporarily unavailable.<p>")
        
        append(pack(">i",len(message))+message)

    def close(self):
        if self.__append is not None:
            self.__append=None
            SizedMessageAsyncConnection.inheritedAttribute('close')(self)

class Disconnected(Exception):
    """The client has become disconnected from the server
    """
    
