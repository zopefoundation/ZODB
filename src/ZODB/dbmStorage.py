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
"""Very Simple dbm-based ZODB storage

This storage provides for use of dbm files as storages that
don't support versions or Undo.  This may be useful when implementing
objects like hit counters that don't need or want to participate
in undo or versions.
"""
__version__='$Revision: 1.5 $'[11:-2]

from MappingStorage import MappingStorage
from BaseStorage import BaseStorage
import anydbm, os

class anydbmStorage(MappingStorage):

    def __init__(self, filename, flag='r', mode=0666):


        BaseStorage.__init__(self, filename)
        self._index=anydbm.open(filename, flag, mode)
        self._tindex=[]
        keys=self._index.keys()
        if keys: self._oid=max(keys)

    def getSize(self):
        # This is a little iffy, since we aren't entirely sure what the file is
        self._lock_acquire()
        try:
            try:
                return (os.stat(self.__name__+'.data')[6] +
                        os.stat(self.__name__+'.dir')[6]
                        )
            except:
                try: return os.stat(self.__name__)[6]
                except: return 0
        finally: self._lock_release()

class gdbmStorage(anydbmStorage):

    def __init__(self, filename, flag='r', mode=0666):

        BaseStorage.__init__(self, filename)
        import gdbm
        self._index=index=gdbm.open(filename, flag[:1]+'f', mode)
        self._tindex=[]

        m='\0\0\0\0\0\0\0\0'
        oid=index.firstkey()
        while oid != None:
            m=max(m, oid)
            oid=index.nextkey(oid)

        self._oid=m

    def getSize(self):
        self._lock_acquire()
        try: return os.stat(self.__name__)[6]
        finally: self._lock_release()

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

            deleted=[]
            oid=index.firstkey()
            while oid != None:
                if not referenced(oid): deleted.append(oid)
                oid=index.nextkey(oid)

            pindex=referenced=None

            for oid in deleted: del index[oid]

            index.sync()
            index.reorganize()

        finally: self._lock_release()


    def _finish(self, tid, user, desc, ext):

        index=self._index
        for oid, p in self._tindex: index[oid]=p
        index.sync()
