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
"""Handy standard storage machinery
"""
__version__='$Revision: 1.6 $'[11:-2]

import time, bpthread, UndoLogCompatible
from POSException import UndoError
from TimeStamp import TimeStamp
z64='\0'*8

class BaseStorage(UndoLogCompatible.UndoLogCompatible):
    _transaction=None
    _serial=z64

    def __init__(self, name, base=None):
        
        self.__name__=name

        # Allocate locks:
        l=bpthread.allocate_lock()
        self._lock_acquire=l.acquire
        self._lock_release=l.release
        l=bpthread.allocate_lock()
        self._commit_lock_acquire=l.acquire
        self._commit_lock_release=l.release

        t=time.time()
        t=self._ts=apply(TimeStamp,(time.gmtime(t)[:5]+(t%60,)))
        self._serial=`t`
        if base is None: self._oid='\0\0\0\0\0\0\0\0'
        else:            self._oid=base._oid

    def abortVersion(self, src, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        return []

    def close(self): pass

    commitVersion=abortVersion

    def getName(self): return self.__name__
    def getSize(self): return len(self)*300 # WAG!
    def history(self, oid, version, length=1): pass
                    
    def modifiedInVersion(self, oid): return ''

    def new_oid(self, last=None):
        if last is None:
            self._lock_acquire()
            try:
                last=self._oid
                d=ord(last[-1])
                if d < 255: last=last[:-1]+chr(d+1)
                else:       last=self.new_oid(last[:-1])
                self._oid=last
                return last
            finally: self._lock_release()
        else:
            d=ord(last[-1])
            if d < 255: return last[:-1]+chr(d+1)+'\0'*(8-len(last))
            else:       return self.new_oid(last[:-1])

    def registerDB(self, db, limit): pass # we don't care

    def supportsUndo(self): return 0
    def supportsVersions(self): return 0
        
    def tpc_abort(self, transaction):
        self._lock_acquire()
        try:
            if transaction is not self._transaction: return
            self._clear_temp()
            self._transaction=None
            self._commit_lock_release()
        finally: self._lock_release()

    def tpc_begin(self, transaction):
        self._lock_acquire()
        try:
            if self._transaction is transaction: return
            self._lock_release()
            self._commit_lock_acquire()
            self._lock_acquire()
            self._transaction=transaction
            self._clear_temp()

            user=transaction.user
            desc=transaction.description
            ext=transaction._extension
            if ext: ext=dumps(ext,1)
            else: ext=""
            self._ude=user, desc, ext

            t=time.time()
            t=apply(TimeStamp,(time.gmtime(t)[:5]+(t%60,)))
            self._ts=t=t.laterThan(self._ts)
            self._serial=`t`

            self._begin(self._serial, user, desc, ext)
            
        finally: self._lock_release()

    def _begin(self, tid, u, d, e):
        pass

    def tpc_vote(self, transaction): pass

    def tpc_finish(self, transaction, f=None):
        self._lock_acquire()
        try:
            if transaction is not self._transaction: return
            if f is not None: f()

            u,d,e=self._ude
            self._finish(self._serial, u, d, e)
            self._clear_temp()
        finally:
            self._ude=None
            self._transaction=None
            self._commit_lock_release()
            self._lock_release()

    def _finish(self, tid, u, d, e):
        pass

    def undo(self, transaction_id):
        raise UndoError, 'non-undoable transaction'

    def undoLog(self, first, last, filter=None): return ()

    def versionEmpty(self, version): return 1

    def versions(self, max=None): return ()

    def pack(self, t, referencesf): pass

