######################################################################
# Digital Creations Options License Version 0.9.0
# -----------------------------------------------
# 
# Copyright (c) 1999, Digital Creations.  All rights reserved.
# 
# This license covers Zope software delivered as "options" by Digital
# Creations.
# 
# Use in source and binary forms, with or without modification, are
# permitted provided that the following conditions are met:
# 
# 1. Redistributions are not permitted in any form.
# 
# 2. This license permits one copy of software to be used by up to five
#    developers in a single company. Use by more than five developers
#    requires additional licenses.
# 
# 3. Software may be used to operate any type of website, including
#    publicly accessible ones.
# 
# 4. Software is not fully documented, and the customer acknowledges
#    that the product can best be utilized by reading the source code.
# 
# 5. Support for software is included for 90 days in email only. Further
#    support can be purchased separately.
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
######################################################################
"""Sized message async connections
"""

__version__ = "$Id: smac.py,v 1.4 1999/11/16 15:42:00 petrilli Exp $"[11:-2]

import asyncore, string, struct, zLOG
from zLOG import LOG, INFO, ERROR

class smac(asyncore.dispatcher):

    def __init__(self, sock, addr):
        asyncore.dispatcher.__init__(self, sock)
        self.addr=addr
        self.__state=None
        self.__inp=None
        self.__l=4
        self.__output=output=[]
        self.__append=output.append
        self.__pop=output.pop

    def handle_read(self,
                    join=string.join, StringType=type('')):
        l=self.__l
        d=self.recv(l)
        inp=self.__inp
        if inp is None:
            inp=d
        elif type(inp) is StringType:
            inp=[inp,d]
        else:
            inp.append(d)

        l=l-len(d)
        if l <= 0:
            if type(inp) is not StringType: inp=join(inp,'')
            if self.__state is None:
                # waiting for message
                self.__l=struct.unpack(">i",inp)[0]
                self.__state=1
                self.__inp=None
            else:
                self.__inp=None
                self.__l=4
                self.__state=None
                self.message_input(inp)
        else:
            self.__l=l
            self.__inp=inp

    def readable(self): return 1
    def writable(self): return not not self.__output

    def handle_write(self):
        output=self.__output
        if output:
            v=output[0]
            n=self.send(v)
            if n < len(v):
                output[0]=v[n:]
            else:
                del output[0]

    def handle_close(self): self.close()

    def message_output(self, message,
                       pack=struct.pack, len=len):
        if __debug__:
            if len(message) > 40: m=message[:40]+' ...'
            else: m=message
            LOG('smax', INFO, 'message_output %s' % `m`)
        self.__append(pack(">i",len(message))+message)

    def log_info(self, message, type='info'):
        if type=='error': type=ERROR
        else: type=INFO
        LOG('ZEO Server', type, message)

    log=log_info
