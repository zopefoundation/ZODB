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
__version__='$Revision: 1.6 $'[11:-2]

import POSException, BaseStorage, string, utils
from TimeStamp import TimeStamp

class MappingStorage(BaseStorage.BaseStorage):

    def __init__(self, name='Mapping Storage'):

        BaseStorage.BaseStorage.__init__(self, name)

        self._index={}
        self._tindex=[]

        # Note:
        # If you subclass this and use a persistent mapping facility
        # (e.g. a dbm file), you will need to get the maximum key and
        # save it as self._oid.  See dbmStorage.

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
            if self._index.has_key(oid):
                old=self._index[oid]
                oserial=old[:8]
                if serial != oserial:
                    raise POSException.ConflictError(serials=(oserial, serial))
                
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
