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

__version__ = "$Revision: 1.9 $"[11:-2]

import asyncore, string, struct, zLOG, sys, Acquisition
from zLOG import LOG, TRACE, ERROR, INFO

class SizedMessageAsyncConnection(Acquisition.Explicit, asyncore.dispatcher):

    __append=None # Marker indicating that we're closed

    socket=None # to outwit Sam's getattr

    def __init__(self, sock, addr, map=None, debug=None):
        SizedMessageAsyncConnection.inheritedAttribute(
            '__init__')(self, sock, map)
        self.addr=addr
        if debug is not None:
            self._debug=debug
        elif not hasattr(self, '_debug'):
            self._debug=__debug__ and 'smac'
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

        d=self.recv(8096)
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
            n=self.send(v)
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
        if self._debug:
            if len(message) > 40: m=message[:40]+' ...'
            else: m=message
            LOG(self._debug, TRACE, 'message_output %s' % `m`)

        append=self.__append
        if append is None:
            raise Disconnected, (
                "This action is temporarily unavailable."
                "<p>"
                )
        
        append(pack(">i",len(message))+message)

    def log_info(self, message, type='info'):
        if type=='error': type=ERROR
        else: type=INFO
        LOG('ZEO', type, message)

    log=log_info

    def close(self):
        if self.__append is not None:
            self.__append=None
            SizedMessageAsyncConnection.inheritedAttribute('close')(self)

class Disconnected(Exception):
    """The client has become disconnected from the server
    """
    
