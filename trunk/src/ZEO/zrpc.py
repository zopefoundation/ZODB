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
"""Simple rpc mechanisms
"""

__version__ = "$Revision: 1.20 $"[11:-2]

from cPickle import loads
import cPickle
from thread import allocate_lock
from smac import SizedMessageAsyncConnection
import socket, string, struct, asyncore, sys, time, select
from zLOG import LOG, TRACE, DEBUG, INFO
from ZEO import asyncwrap

from errno import EINTR
TupleType=type(())

# We create a special fast pickler! This allows us
# to create slightly more efficient pickles and
# to create them a tad faster.
pickler=cPickle.Pickler()
pickler.fast=1 # Don't use the memo
dump=pickler.dump

class UnUnPickleableError(Exception):
    "Couldn't unpickle a remote exception"

class asyncRPC(SizedMessageAsyncConnection):

    # Flag indicating whether a main loop is running. If one isn't running,
    # then we'll have to provide our own main loop at times.
    __haveMainLoop=0  
    def __Wakeup(*args): pass

    def __init__(self, connection, outOfBand=None, tmin=5, tmax=300, debug=0):
        self._connection=connection
        self._outOfBand=outOfBand
        self._tmin, self._tmax = tmin, tmax
        self._debug=debug
        self.__closed = 0

        l=allocate_lock() # Response lock used to wait for call results
        self.__la=l.acquire
        self.__lr=l.release
        self.__r=None
        l.acquire()

        l=allocate_lock() # Response lock used to wait for call results
        self.__call_la=l.acquire
        self.__call_lr=l.release

    def connect(self, tryonce=1, log_type='client'):
        t=self._tmin
        connection = self._connection
        debug=self._debug
        while self.__closed == 0:
            if log_type: LOG(log_type, INFO,
                             'Trying to connect to server: %s' % `connection`)
            try:
                if type(connection) is type(''):
                    s=socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                else:
                    s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(connection)    
            except Exception, err:
                if debug:
                    LOG(debug, DEBUG, "Failed to connect to server: %s" % err)
                if tryonce: return 0
                time.sleep(t)
                t=t*2
                if t > self._tmax: t=self._tmax
            else:
                if debug:
                    LOG(debug, DEBUG, "Connected to server")
                    
                # Make sure the result lock is set, se we don't
                # get an old result (e.g. the exception that
                # we generated on close).
                self.__r=None
                self.__la(0)
                
                self.aq_parent.notifyConnected(s)
                return 1

    def finishConnect(self, s):
        if self.__haveMainLoop:
            map = None # use the main loop map
        else:
            map = {} # provide a dummy map
        SizedMessageAsyncConnection.__init__(self, s, '', map)

    # we are our own socket map!
    def keys(self): return (self._fileno,)
    def values(self): return (self,)
    def items(self): return ((self._fileno,self),)
    def __len__(self): return 1
    def __getitem__(self, key):
        if key==self._fileno: return self
        raise KeyError, key

    def sync(self):
        if self.__haveMainLoop: return # in async mode

        # Ick, I have to do my own select loop, which sucks
        while 1:
            try: r, w, e = select.select([self._fileno],[],[],0.0)
            except select.error, v:
                if v[0] != EINTR: raise
            if r: asyncwrap.poll(0.0, self)
            else: break

    def readLoop(self):
        la=self.__la
        while not la(0):
            asyncwrap.poll(60.0, self)
        self.__lr()

    def setLoop(self, map=None, Wakeup=lambda : None):
        if map is None: self.__haveMainLoop=0
        else:
            self.add_channel(map) # asyncore registration
            self.__haveMainLoop=1

        self.__Wakeup=Wakeup

         
    def __call__(self, *args):
        self.__call_la()
        try:
            self._last_args=args=dump(args,1)
            self.message_output(args)

            if self.__haveMainLoop:
                self.__Wakeup() # Wakeup the main loop
            else: self.readLoop()

            while 1:
                r=self._read()
                c=r[:1]
                if c=='R':
                    if r=='RN.': return None # Common case!
                    return loads(r[1:])
                
                # If c == 'E', an error occured on the server.  In
                # this case, the return value is a pickled exception.
                # Unpickle it and raise it on the client side.  The
                # traceback for this exception ends at this method,
                # but the real error occurred somewhere in the server
                # code.  To diagnose the error, look for the real
                # traceback in the server's zLOG output.
                if c=='E':
                    try: r=loads(r[1:])
                    except:
                        raise UnUnPickleableError(r[1:])
                    if type(r) is TupleType:
                        raise r[0], r[1] # see server log for real traceback
                    raise r 
                oob=self._outOfBand
                if oob is not None:
                    r=r[1:]
                    if r=='N.': r=None # Common case!
                    else: r=loads(r)
                    oob(c, r)
                else:
                    raise UnrecognizedResult, r
        finally:
            self._last_args=''
            self.__call_lr()

    def sendMessage(self, *args):
        self.message_output(dump(args,1))
        if self.__haveMainLoop:
            self.__Wakeup() # Wake up the main loop
        else: asyncwrap.poll(0.0, self)

    def setOutOfBand(self, f):
        """Define a call-back function for handling out-of-band communication

        Normal communications from the server consists of call returns
        and exception returns. The server may also send asynchronous
        messages to the client. For the client to recieve these
        messages, it must register an out-of-band callback
        function. The function will be called with a single-character
        message code and a message argument.
        """

        self._outOfBand=f

    def message_input(self, m):
        if self._debug:
            if len(m) > 60:
                md = repr(m[:60]) + ' ...'
            else:
                md = repr(m)
            LOG(self._debug, TRACE, 'message_input %s' % md)

        c=m[:1]
        if c in 'RE':
            self.__r=m
            try: self.__lr()
            except:
                # Eek, this should never happen. We're messed up.
                # we'd better close the connection.
                self.close()
                raise
        else:
            oob=self._outOfBand
            if oob is not None:
                m=m[1:]
                if m=='N.': m=None
                else: m=loads(m)
                oob(c, m)

    def _read(self):
        self.__la()
        return self.__r

    def closeIntensionally(self):
        if self.__haveMainLoop:
            # We aren't willing to close until told to by the main loop.
            # So we'll tell the main loop to tell us. :)
            self.__Wakeup(lambda self=self: self.close()) 
        else:
            self.close()
            self._outOfBand = None
            self.__closed = 1
        
    def close(self):
        asyncRPC.inheritedAttribute('close')(self)
        self.aq_parent.notifyDisconnected(self)
        # causes read call to raise last exception, which should be
        # the socket error that caused asyncore to close the socket.
        self.__r = 'E' + dump(sys.exc_info()[:2], 1)
        try:
            self.__lr()
        except:
            pass
