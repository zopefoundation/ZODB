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
# Do this portably in the face of checking out with -kv
import string
__version__ = string.split('$Revision: 1.12 $')[-2:][0]

import ThreadLock, bpthread
import time, UndoLogCompatible
import POSException
from TimeStamp import TimeStamp
z64='\0'*8

class BaseStorage(UndoLogCompatible.UndoLogCompatible):
    _transaction=None # Transaction that is being committed
    _serial=z64       # Transaction serial number
    _tstatus=' '      # Transaction status, used for copying data

    def __init__(self, name, base=None):
        
        self.__name__=name

        # Allocate locks:
        l=ThreadLock.allocate_lock()
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

    def commitVersion(self, src, dest, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        return []

    def close(self): pass

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
            self._abort()
            self._clear_temp()
            self._transaction=None
            self._commit_lock_release()
        finally: self._lock_release()

    def _abort(self):
        """Subclasses should rededine this to supply abort actions"""
        pass

    def tpc_begin(self, transaction, tid=None, status=' '):
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

            if tid is None:
                t=time.time()
                t=apply(TimeStamp,(time.gmtime(t)[:5]+(t%60,)))
                self._ts=t=t.laterThan(self._ts)
                self._serial=`t`
            else:
                self._ts=TimeStamp(tid)
                self._serial=tid

            self._tstatus=status

            self._begin(self._serial, user, desc, ext)
            
        finally: self._lock_release()

    def _begin(self, tid, u, d, e):
        """Subclasses should rededine this to supply
        transaction start actions"""
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
        """Subclasses should rededine this to supply commit actions"""
        pass

    def undo(self, transaction_id):
        raise POSException.UndoError, 'non-undoable transaction'

    def undoLog(self, first, last, filter=None): return ()

    def versionEmpty(self, version): return 1

    def versions(self, max=None): return ()

    def pack(self, t, referencesf): pass

    def loadSerial(self, oid, serial):
        raise POSException.Unsupported, (
            "Retrieval of historical revisions is not supported")

    def copyTransactionsFrom(self, other, verbose=0):
        """Copy transactions from another storage.

        This is typically used for converting data from one storage to another.
        """
        _ts=None
        ok=1
        preindex={}; preget=preindex.get   # waaaa
        for transaction in other.iterator():
            
            tid=transaction.tid
            if _ts is None:
                _ts=TimeStamp(tid)
            else:
                t=TimeStamp(tid)
                if t <= _ts:
                    if ok: print ('Time stamps out of order %s, %s' % (_ts, t))
                    ok=0
                    _ts=t.laterThan(_ts)
                    tid=`_ts`
                else:
                    _ts = t
                    if not ok:
                        print ('Time stamps back in order %s' % (t))
                        ok=1

            if verbose: print _ts
            
            self.tpc_begin(transaction, tid, transaction.status)
            for r in transaction:
                oid=r.oid
                if verbose: print `oid`, r.version, len(r.data)
                pre=preget(oid, None)
                s=self.store(oid, pre, r.data, r.version, transaction)
                preindex[oid]=s
                
            self.tpc_vote(transaction)
            self.tpc_finish(transaction)
