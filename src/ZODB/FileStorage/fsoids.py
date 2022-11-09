##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
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
from __future__ import print_function

import ZODB.FileStorage
from ZODB.serialize import get_refs
from ZODB.TimeStamp import TimeStamp
from ZODB.utils import get_pickle_metadata
from ZODB.utils import oid_repr
from ZODB.utils import p64
from ZODB.utils import tid_repr


# Extract module.class string from pickle.


def get_class(pickle):
    return "%s.%s" % get_pickle_metadata(pickle)

# Shorten a string for display.


def shorten(s, size=50):
    if len(s) <= size:
        return s
    # Stick ... in the middle.
    navail = size - 5
    nleading = navail // 2
    ntrailing = size - nleading
    if isinstance(s, bytes):
        sep = b" ... "
    else:
        sep = " ... "
    return s[:nleading] + sep + s[-ntrailing:]


class Tracer(object):
    """Trace all occurrences of a set of oids in a FileStorage.

    Create passing a path to an existing FileStorage.
    Call register_oids(oid, ...) one or more times to specify which oids to
    investigate.
    Call run() to do the analysis.  This isn't swift -- it has to read
    every byte in the database, in order to find all references.
    Call report() to display the results.
    """

    def __init__(self, path):
        import os
        if not os.path.isfile(path):
            raise ValueError("must specify an existing FileStorage")
        self.path = path
        # Map an interesting tid to (status, user, description, pos).
        self.tid2info = {}
        # List of messages.  Each is a tuple of the form
        #     (oid, tid, string)
        # The order in the tuple is important, because it defines the
        # sort order for grouping.
        self.msgs = []
        # The set of interesting oids, specified by register_oid() calls.
        # Maps oid to # of revisions.
        self.oids = {}
        # Maps interesting oid to its module.class name.  If a creation
        # record for an interesting oid is never seen, it won't appear
        # in this mapping.
        self.oid2name = {}

    def register_oids(self, *oids):
        """
        Declare that oids (0 or more) are "interesting".

        An oid can be given as a native 8-byte string, or as an
        integer.

        Info will be gathered about all appearances of this oid in the
        entire database, including references.
        """
        for oid in oids:
            if isinstance(oid, bytes):
                assert len(oid) == 8
            else:
                oid = p64(oid)
            self.oids[oid] = 0  # 0 revisions seen so far

    def _msg(self, oid, tid, *args):
        self.msgs.append((oid, tid, ' '.join(map(str, args))))
        self._produced_msg = True

    def report(self):
        """Show all msgs, grouped by oid and sub-grouped by tid."""

        msgs = self.msgs
        oids = self.oids
        oid2name = self.oid2name
        # First determine which oids weren't seen at all, and synthesize msgs
        # for them.
        NOT_SEEN = "this oid was not defined (no data record for it found)"
        for oid in oids:
            if oid not in oid2name:
                msgs.append((oid, None, NOT_SEEN))

        msgs.sort()  # oids are primary key, tids secondary
        current_oid = current_tid = None
        for oid, tid, msg in msgs:
            if oid != current_oid:
                nrev = oids[oid]
                revision = "revision" + (nrev != 1 and 's' or '')
                name = oid2name.get(oid, "<unknown>")
                print("oid", oid_repr(oid), name, nrev, revision)
                current_oid = oid
                current_tid = None
                if msg is NOT_SEEN:
                    assert tid is None
                    print("   ", msg)
                    continue
            if tid != current_tid:
                current_tid = tid
                status, user, description, pos = self.tid2info[tid]
                print("    tid %s offset=%d %s" % (tid_repr(tid),
                                                   pos,
                                                   TimeStamp(tid)))
                print("        tid user=%r" % shorten(user))
                print("        tid description=%r" % shorten(description))
            print("       ", msg)

    # Do the analysis.
    def run(self):
        """Find all occurrences of the registered oids in the database."""

        # Maps oid of a reference to its module.class name.
        self._ref2name = {}
        for txn in ZODB.FileStorage.FileIterator(self.path):
            self._check_trec(txn)

    # Process next transaction record.
    def _check_trec(self, txn):
        # txn has members tid, status, user, description,
        # _extension, _pos, _tend, _file, _tpos
        self._produced_msg = False
        # Map and list for save data records for current transaction.
        self._records_map = {}
        self._records = []
        for drec in txn:
            self._save_references(drec)
        for drec in self._records:
            self._check_drec(drec)
        if self._produced_msg:
            # Copy txn info for later output.
            self.tid2info[txn.tid] = (txn.status, txn.user, txn.description,
                                      txn._tpos)

    def _save_references(self, drec):
        # drec has members oid, tid, data, data_txn
        tid, oid, pick, pos = drec.tid, drec.oid, drec.data, drec.pos
        if pick:
            if oid in self.oids:
                klass = get_class(pick)
                self._msg(oid, tid, "new revision", klass, "at", pos)
                self.oids[oid] += 1
                self.oid2name[oid] = self._ref2name[oid] = klass
            self._records_map[oid] = drec
            self._records.append(drec)
        elif oid in self.oids:
            self._msg(oid, tid, "creation undo at", pos)

    # Process next data record.  If a message is produced, self._produced_msg
    # will be set True.
    def _check_drec(self, drec):
        # drec has members oid, tid, data, data_txn
        tid, oid, pick, pos = drec.tid, drec.oid, drec.data, drec.pos
        ref2name = self._ref2name
        ref2name_get = ref2name.get
        records_map_get = self._records_map.get
        if pick:
            oid_in_oids = oid in self.oids
            for ref, klass in get_refs(pick):
                if ref in self.oids:
                    oidclass = ref2name_get(oid, None)
                    if oidclass is None:
                        ref2name[oid] = oidclass = get_class(pick)
                    self._msg(ref, tid, "referenced by", oid_repr(oid),
                              oidclass, "at", pos)

                if oid_in_oids:
                    if klass is None:
                        klass = ref2name_get(ref, None)
                        if klass is None:
                            r = records_map_get(ref, None)
                            # For save memory we only save references
                            # seen in one transaction with interesting
                            # objects changes. So in some circumstances
                            # we may still got "<unknown>" class name.
                            if r is None:
                                klass = "<unknown>"
                            else:
                                ref2name[ref] = klass = get_class(r.data)
                    elif isinstance(klass, tuple):
                        ref2name[ref] = klass = "%s.%s" % klass
                    else:
                        klass = "%s.%s" % (klass.__module__, klass.__name__)

                    self._msg(oid, tid, "references", oid_repr(ref), klass,
                              "at", pos)
