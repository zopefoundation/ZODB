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
"""Very Simple Mapping ZODB storage

The Mapping storage provides an extremely simple storage
implementation that doesn't provide undo or version support.

It is meant to illustrate the simplest possible storage.

The Mapping storage uses a single data structure to map
object ids to data.

The Demo storage serves two purposes:

  - Provide an example implementation of a full storage without
    distracting storage details,

  - Provide a volatile storage that is useful for giving demonstrations.

The demo strorage can have a "base" storage that is used in a
read-only fashion. The base storage must not not to contain version
data.

There are three main data structures:

  _data -- Transaction logging information necessary for undo

      This is a mapping from transaction id to transaction, where
      a transaction is simply a 4-tuple:

        packed, user, description, extension_data, records

      where extension_data is a dictionary or None and records are the
      actual records in chronological order. Packed is a flag
      indicating whethe the transaction has been packed or not

  _index -- A mapping from oid to record

  _vindex -- A mapping from version name to version data

      where version data is a mapping from oid to record

A record is a tuple:

  oid, serial, pre, vdata, p, 

where:

     oid -- object id

     serial -- object serial number

     pre -- The previous record for this object (or None)

     vdata -- version data

        None if not a version, ortherwise:
           version, non-version-record

     p -- the pickle data or None

The pickle data will be None for a record for an object created in
an aborted version.

It is instructive to watch what happens to the internal data structures
as changes are made.  Foe example, in Zope, you can create an external
method::

  import Zope

  def info(RESPONSE):
      RESPONSE['Content-type']= 'text/plain'

      return Zope.DB._storage._splat()

and call it to minotor the storage.

"""
__version__='$Revision: 1.2 $'[11:-2]

import base64, POSException, BTree, BaseStorage, time, string, utils
from TimeStamp import TimeStamp

class MappingStorage(BaseStorage.BaseStorage):

    def __init__(self, name='Mapping Storage'):

        BaseStorage.BaseStorage.__init__(self, name)

        self._index={}
        self._tindex=[]

    def __len__(self):
        return len(self._index)
        
    def getSize(self):
        s=32
        index=self._index
        for oid in index.keys():
            p=index[oid]
            s=s+56+len(p)
            
        return s

    def load(self, oid, version):
        self._lock_acquire()
        try:
            p=self._index[oid]
            return p[8:], p[:8] # pickle, serial
        finally: self._lock_release()

    def store(self, oid, serial, data, version, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        if version:
            raise POSException.Unsupported, "Versions aren't supported"

        self._lock_acquire()
        try:
            old=self._index.get(oid, None)
            if old is not None:
                oserial=old[:8]
                if serial != oserial: raise POSException.ConflictError
                
            serial=self._serial
            self._tindex.append((oid,serial+data))
        finally: self._lock_release()

        return serial

    def _clear_temp(self):
        self._tindex=[]

    def _finish(self, tid, user, desc, ext):

        index=self._index
        for oid, p in self._tindex: index[oid]=p

    def pack(self, t, referencesf):
        
        self._lock_acquire()
        try:    
            # Build an index of *only* those objects reachable
            # from the root.
            index=self._index
            rootl=['\0\0\0\0\0\0\0\0']
            pop=rootl.pop
            pindex={}
            referenced=pindex.has_key
            while rootl:
                oid=pop()
                if referenced(oid): continue
    
                # Scan non-version pickle for references
                r=index[oid]
                pindex[oid]=r
                p=r[8:]
                referencesf(p, rootl)

            # Now delete any unreferenced entries:
            for oid in index.keys():
                if not referenced(oid): del index[oid]
    
        finally: self._lock_release()

    def _splat(self):
        """Spit out a string showing state.
        """
        o=[]
        o.append('Index:')
        index=self._index
        keys=index.keys()
        keys.sort()
        for oid in keys:
            r=index[oid]
            o.append('  %s: %s, %s' %
                     (utils.u64(oid),TimeStamp(r[:8]),`r[8:]`))
            
        return string.join(o,'\n')
