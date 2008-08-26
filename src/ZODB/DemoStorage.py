##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Demo ZODB storage

The Demo storage serves two purposes:

  - Provide an example implementation of a full storage without
    distracting storage details,

  - Provide a volatile storage that is useful for giving demonstrations.

The demo storage can have a "base" storage that is used in a
read-only fashion. The base storage must not contain version
data.

There are three main data structures:

  _data -- Transaction logging information necessary for undo

      This is a mapping from transaction id to transaction, where
      a transaction is simply a 5-tuple:

        packed, user, description, extension_data, records

      where extension_data is a dictionary or None and records are the
      actual records in chronological order. Packed is a flag
      indicating whethe the transaction has been packed or not

  _index -- A mapping from oid to record

  _vindex -- A mapping from version name to version data

      where version data is a mapping from oid to record

A record is a tuple:

  oid, pre, vdata, p, tid

where:

     oid -- object id

     pre -- The previous record for this object (or None)

     vdata -- version data

        None if not a version, ortherwise:
           version, non-version-record

     p -- the pickle data or None

     tid -- the transaction id that wrote the record

The pickle data will be None for a record for an object created in
an aborted version.

It is instructive to watch what happens to the internal data structures
as changes are made.  For example, in Zope, you can create an external
method::

  import Zope2

  def info(RESPONSE):
      RESPONSE['Content-type']= 'text/plain'

      return Zope2.DB._storage._splat()

and call it to monitor the storage.

"""

import cPickle
import base64, time

import ZODB.BaseStorage
import ZODB.interfaces
import zope.interface
from ZODB import POSException
from ZODB.utils import z64, oid_repr
from persistent.TimeStamp import TimeStamp
from BTrees import OOBTree


class DemoStorage(ZODB.BaseStorage.BaseStorage):
    """Demo storage

    Demo storages provide useful storages for writing tests because
    they store their data in memory and throw away their data
    (implicitly) when they are closed.

    They were originally designed to allow demonstrations using base
    data provided on a CD.  They can optionally wrap an *unchanging*
    base storage.  It is critical that the base storage does not
    change. Using a changing base storage is not just unsupported, it
    is known not to work and can even lead to serious errors and even
    core dumps.
    
    """
    
    zope.interface.implements(ZODB.interfaces.IStorageIteration)

    def __init__(self, name='Demo Storage', base=None, quota=None):
        ZODB.BaseStorage.BaseStorage.__init__(self, name, base)

        # We use a BTree because the items are sorted!
        self._data = OOBTree.OOBTree()
        self._index = {}
        self._vindex = {}
        self._base = base
        self._size = 0
        self._quota = quota
        self._ltid = None
        self._clear_temp()

        try:
            versions = base.versions
        except AttributeError:
            pass
        else:
            if base.versions():
                raise POSException.StorageError(
                    "Demo base storage has version data")

    # When DemoStorage needs to create a new oid, and there is a base
    # storage, it must use that storage's new_oid() method.  Else
    # DemoStorage may end up assigning "new" oids that are already in use
    # by the base storage, leading to a variety of "impossible" problems.
    def new_oid(self):
        if self._base is None:
            return ZODB.BaseStorage.BaseStorage.new_oid(self)
        else:
            return self._base.new_oid()

    def __len__(self):
        base=self._base
        return (base and len(base) or 0) + len(self._index)

    def getSize(self):
        s=100
        for tid, (p, u, d, e, t) in self._data.items():
            s=s+16+24+12+4+16+len(u)+16+len(d)+16+len(e)+16
            for oid, pre, vdata, p, tid in t:
                s=s+16+24+24+4+4+(p and (16+len(p)) or 4)
                if vdata: s=s+12+16+len(vdata[0])+4

        s=s+16*len(self._index)

        for v in self._vindex.values():
            s=s+32+16*len(v)

        self._size=s
        return s

    def abortVersion(self, src, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)
        if not src:
            raise POSException.VersionCommitError("Invalid version")

        self._lock_acquire()
        try:
            v = self._vindex.get(src, None)
            if not v:
                return

            oids = []
            for r in v.values():
                oid, pre, (version, nv), p, tid = r
                oids.append(oid)
                if nv:
                    oid, pre, vdata, p, tid = nv
                    self._tindex.append([oid, r, None, p, self._tid])
                else:
                    # effectively, delete the thing
                    self._tindex.append([oid, r, None, None, self._tid])

            return self._tid, oids

        finally: self._lock_release()

    def commitVersion(self, src, dest, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        if not src:
            raise POSException.VersionCommitError("Invalid source version")
        if src == dest:
            raise POSException.VersionCommitError(
                "Can't commit to same version: %s" % repr(src))

        self._lock_acquire()
        try:
            v = self._vindex.get(src)
            if v is None:
                return

            newserial = self._tid
            tindex = self._tindex
            oids = []
            for r in v.values():
                oid, pre, vdata, p, tid = r
                assert vdata is not None
                oids.append(oid)
                if dest:
                    new_vdata = dest, vdata[1]
                else:
                    new_vdata = None
                tindex.append([oid, r, new_vdata, p, self._tid])

            return self._tid, oids

        finally:
            self._lock_release()

    def load(self, oid, version):
        self._lock_acquire()
        try:
            try:
                oid, pre, vdata, p, tid = self._index[oid]
            except KeyError:
                if self._base:
                    return self._base.load(oid, version)
                raise KeyError(oid)

            if vdata:
                oversion, nv = vdata
                if oversion != version:
                    if nv:
                        # Return the current txn's tid with the non-version
                        # data.
                        p = nv[3]
                    else:
                        raise KeyError(oid)

            if p is None:
                raise KeyError(oid)

            return p, tid
        finally: self._lock_release()

    def modifiedInVersion(self, oid):
        self._lock_acquire()
        try:
            try:
                oid, pre, vdata, p, tid = self._index[oid]
                if vdata: return vdata[0]
                return ''
            except: return ''
        finally: self._lock_release()

    def store(self, oid, serial, data, version, transaction):
        if transaction is not self._transaction:
            raise POSException.StorageTransactionError(self, transaction)

        self._lock_acquire()
        try:
            old = self._index.get(oid, None)
            if old is None:
                # Hm, nothing here, check the base version:
                if self._base:
                    try:
                        p, tid = self._base.load(oid, '')
                    except KeyError:
                        pass
                    else:
                        old = oid, None, None, p, tid

            nv=None
            if old:
                oid, pre, vdata, p, tid = old

                if vdata:
                    if vdata[0] != version:
                        raise POSException.VersionLockError(oid)

                    nv=vdata[1]
                else:
                    nv=old

                if serial != tid:
                    raise POSException.ConflictError(
                        oid=oid, serials=(tid, serial), data=data)

            r = [oid, old, version and (version, nv) or None, data, self._tid]
            self._tindex.append(r)

            s=self._tsize
            s=s+72+(data and (16+len(data)) or 4)
            if version: s=s+32+len(version)

            if self._quota is not None and s > self._quota:
                raise POSException.StorageError(
                    '''<b>Quota Exceeded</b><br>
                    The maximum quota for this demonstration storage
                    has been exceeded.<br>Have a nice day.''')

        finally: self._lock_release()
        return self._tid

    def supportsVersions(self):
        return 1

    def _clear_temp(self):
        self._tindex = []
        self._tsize = self._size + 160

    def lastTransaction(self):
        return self._ltid

    def _begin(self, tid, u, d, e):
        self._tsize = self._size + 120 + len(u) + len(d) + len(e)

    def _finish(self, tid, user, desc, ext):
        if not self._tindex:
            # No data, so we don't update anything.
            return

        self._size = self._tsize

        self._data[tid] = None, user, desc, ext, tuple(self._tindex)
        for r in self._tindex:
            oid, pre, vdata, p, tid = r
            old = self._index.get(oid)
            # If the object had version data, remove the version data.
            if old is not None:
                oldvdata = old[2]
                if oldvdata:
                    v = self._vindex[oldvdata[0]]
                    del v[oid]
                    if not v:
                        # If the version info is now empty, remove it.
                        del self._vindex[oldvdata[0]]

            self._index[oid] = r

            # If there is version data, then udpate self._vindex, too.
            if vdata:
                version = vdata[0]
                v = self._vindex.get(version)
                if v is None:
                    v = self._vindex[version] = {}
                v[oid] = r
        self._ltid = self._tid

    def undoLog(self, first, last, filter=None):
        if last < 0:  # abs(last) is an upper bound on the # to return
            last = first - last
        self._lock_acquire()
        try:
            transactions = self._data.items()
            pos = len(transactions)
            r = []
            i = 0
            while i < last and pos:
                pos -= 1
                tid, (p, u, d, e, t) = transactions[pos]
                # Bug alert:  why do we skip this if the transaction
                # has been packed?
                if p:
                    continue
                d = {'id': base64.encodestring(tid)[:-1],
                     'time': TimeStamp(tid).timeTime(),
                     'user_name': u, 'description': d}
                if e:
                    d.update(cPickle.loads(e))
                if filter is None or filter(d):
                    if i >= first:
                        r.append(d)
                    i += 1
            return r
        finally:
            self._lock_release()

    def versionEmpty(self, version):
        return not self._vindex.get(version, None)

    def versions(self, max=None):
        r = []
        for v in self._vindex.keys():
            if self.versionEmpty(v):
                continue
            r.append(v)
            if max is not None and len(r) >= max:
                break
        return r

    def _build_indexes(self, stop='\377\377\377\377\377\377\377\377'):
        # Rebuild index structures from transaction data
        index = {}
        vindex = {}
        for tid, (p, u, d, e, t) in self._data.items():
            if tid >= stop:
                break
            for r in t:
                oid, pre, vdata, p, tid = r
                old=index.get(oid, None)

                if old is not None:
                    oldvdata=old[2]
                    if oldvdata:
                        v=vindex[oldvdata[0]]
                        del v[oid]
                        if not v: del vindex[oldvdata[0]]

                index[oid]=r

                if vdata:
                    version=vdata[0]
                    v=vindex.get(version, None)
                    if v is None: v=vindex[version]={}
                    vindex[vdata[0]][oid]=r

        return index, vindex

    def pack(self, t, referencesf):
        # Packing is hard, at least when undo is supported.
        # Even for a simple storage like this one, packing
        # is pretty complex.

        self._lock_acquire()
        try:

            stop=`TimeStamp(*time.gmtime(t)[:5]+(t%60,))`

            # Build indexes up to the pack time:
            index, vindex = self._build_indexes(stop)


            # TODO:  This packing algorithm is flawed. It ignores
            # references from non-current records after the pack
            # time.

            # Now build an index of *only* those objects reachable
            # from the root.
            rootl = [z64]
            pindex = {}
            while rootl:
                oid = rootl.pop()
                if oid in pindex:
                    continue

                # Scan non-version pickle for references
                r = index.get(oid, None)
                if r is None:
                    if self._base:
                        p, s = self._base.load(oid, '')
                        referencesf(p, rootl)
                else:
                    pindex[oid] = r
                    oid, pre, vdata, p, tid = r
                    referencesf(p, rootl)
                    if vdata:
                        nv = vdata[1]
                        if nv:
                            oid, pre, vdata, p, tid = nv
                            referencesf(p, rootl)

            # Now we're ready to do the actual packing.
            # We'll simply edit the transaction data in place.
            # We'll defer deleting transactions till the end
            # to avoid messing up the BTree items.
            deleted = []
            for tid, (p, u, d, e, records) in self._data.items():
                if tid >= stop:
                    break
                o = []
                for r in records:
                    c = pindex.get(r[0])
                    if c is None:
                        # GC this record, no longer referenced
                        continue
                    if c == r:
                        # This is the most recent revision.
                        o.append(r)
                    else:
                        # This record is not the indexed record,
                        # so it may not be current. Let's see.
                        vdata = r[3]
                        if vdata:
                            # Version record are current *only* if they
                            # are indexed
                            continue
                        else:
                            # OK, this isn't a version record, so it may be the
                            # non-version record for the indexed record.
                            vdata = c[3]
                            if vdata:
                                if vdata[1] != r:
                                    # This record is not the non-version
                                    # record for the indexed record
                                    continue
                            else:
                                # The indexed record is not a version record,
                                # so this record can not be the non-version
                                # record for it.
                                continue
                        o.append(r)

                if o:
                    if len(o) != len(records):
                        self._data[tid] = 1, u, d, e, tuple(o) # Reset data
                else:
                    deleted.append(tid)

            # Now delete empty transactions
            for tid in deleted:
                del self._data[tid]

            # Now reset previous pointers for "current" records:
            for r in pindex.values():
                r[1] = None # Previous record
                if r[2] and r[2][1]: # vdata
                    # If this record contains version data and
                    # non-version data, then clear it out.
                    r[2][1][2] = None

            # Finally, rebuild indexes from transaction data:
            self._index, self._vindex = self._build_indexes()

        finally:
            self._lock_release()
        self.getSize()

    def _splat(self):
        """Spit out a string showing state.
        """
        o=[]

        o.append('Transactions:')
        for tid, (p, u, d, e, t) in self._data.items():
            o.append("  %s %s" % (TimeStamp(tid), p))
            for r in t:
                oid, pre, vdata, p, tid = r
                oid = oid_repr(oid)
                tid = oid_repr(tid)
##                if serial is not None: serial=str(TimeStamp(serial))
                pre=id(pre)
                if vdata and vdata[1]: vdata=vdata[0], id(vdata[1])
                if p: p=''
                o.append('    %s: %s' %
                         (id(r), `(oid, pre, vdata, p, tid)`))

        o.append('\nIndex:')
        items=self._index.items()
        items.sort()
        for oid, r in items:
            if r: r=id(r)
            o.append('  %s: %s' % (oid_repr(oid), r))

        o.append('\nVersion Index:')
        items=self._vindex.items()
        items.sort()
        for version, v in items:
            o.append('  '+version)
            vitems=v.items()
            vitems.sort()
            for oid, r in vitems:
                if r: r=id(r)
                o.append('    %s: %s' % (oid_repr(oid), r))

        return '\n'.join(o)

    def cleanup(self):
        if self._base is not None:
            self._base.cleanup()

    def close(self):
        if self._base is not None:
            self._base.close()

    def iterator(self, start=None, end=None):
        # First iterate over the base storage
        if self._base is not None:
            for transaction in self._base.iterator(start, end):
                yield transaction
        # Then iterate over our local transactions
        for tid, transaction in self._data.items():
            if tid >= start and tid <= end:
                yield TransactionRecord(tid, transaction)


class TransactionRecord(ZODB.BaseStorage.TransactionRecord):
    
    def __init__(self, tid, transaction):
        packed, user, description, extension, records = transaction
        super(TransactionRecord, self).__init__(
            tid, packed, user, description, extension)
        self.records = transaction

    def __iter__(self):
        for record in self.records:
            oid, prev, version, data, tid = record
            yield ZODB.BaseStorage.DataRecord(oid, tid, data, version, prev)
