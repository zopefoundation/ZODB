##############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
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

import ZODB.FileStorage
from ZODB.FileStorage.fsdump import get_pickle_metadata
from ZODB.utils import U64, p64, oid_repr, tid_repr, get_refs
from ZODB.TimeStamp import TimeStamp

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
    return s[:nleading] + " ... " + s[-ntrailing:]

class Tracer(object):
    """Trace all occurrences of a set of oids in a FileStorage.

    Create passing a path to an existing FileStorage.
    Call register_oid() one or more times to specify which oids to
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

    def register_oid(self, oid):
        """
        Declare that an oid is "interesting".

        The oid can be given as a native 8-byte string, or as an
        integer.

        Info will be gathered about all appearances of this oid in the
        entire database, including references.
        """
        if isinstance(oid, str):
            assert len(oid) == 8
        else:
            oid = p64(oid)
        self.oids[oid] = 0

    def _msg(self, oid, tid, *args):
        args = map(str, args)
        self.msgs.append( (oid, tid, ' '.join(args)) )

    def report(self):
        """Show all msgs, grouped by oid and sub-grouped by tid."""

        msgs = self.msgs
        oids = self.oids
        oid2name = self.oid2name
        # First determine which oids weren't seen at all, and synthesize msgs
        # for them.
        NOT_SEEN = "this oid was neither defined nor referenced"
        for oid in oids:
            if oid not in oid2name:
                msgs.append( (oid, None, NOT_SEEN) )

        msgs.sort() # oids are primary key, tids secondary
        current_oid = current_tid = None
        for oid, tid, msg in msgs:
            if oid != current_oid:
                nrev = oids[oid]
                revision = "revision" + (nrev != 1 and 's' or '')
                name = oid2name.get(oid, "<unknown>")
                print "oid", oid_repr(oid), name, nrev, revision
                current_oid = oid
                current_tid = None
                if msg is NOT_SEEN:
                    assert tid is None
                    print "   ", msg
                    continue
            if tid != current_tid:
                current_tid = tid
                status, user, description, pos = self.tid2info[tid]
                print "    tid %s offset=%d %s" % (tid_repr(tid),
                                                   pos,
                                                   TimeStamp(tid))
                print "        tid user=%r" % shorten(user)
                print "        tid description=%r" % shorten(description)
            print "       ", msg

    # Do the analysis.
    def run(self):
        """Find all occurrences of the registered oids in the database."""

        for txn in ZODB.FileStorage.FileIterator(self.path):
            self._check_trec(txn)

    # Process next transaction record.
    def _check_trec(self, txn):
        # txn has members tid, status, user, description,
        # _extension, _pos, _tend, _file, _tpos
        interesting = False
        for drec in txn:
            if self._check_drec(drec):
                interesting = True
        if interesting:
            self.tid2info[txn.tid] = (txn.status, txn.user, txn.description,
                                      txn._tpos)

    # Process next data record.  Return true iff a message is produced (so
    # the caller can know whether to save information about the tid the
    # data record belongs to).
    def _check_drec(self, drec):
        # drec has members oid, tid, version, data, data_txn
        result = False
        tid, oid, pick, pos = drec.tid, drec.oid, drec.data, drec.pos
        if pick:
            oidclass = None
            if oid in self.oids:
                oidclass = get_class(pick)
                self._msg(oid, tid, "new revision", oidclass,
                          "at", drec.pos)
                result = True
                self.oids[oid] += 1
                self.oid2name[oid] = oidclass

            for ref, klass in get_refs(pick):
                if klass is None:
                    klass = '<unknown>'
                elif isinstance(klass, tuple):
                    klass = "%s.%s" % klass

                if ref in self.oids:
                    if oidclass is None:
                        oidclass = get_class(pick)
                    self._msg(ref, tid, "referenced by", oid_repr(oid),
                              oidclass, "at", pos)
                    result = True

                if oid in self.oids:
                    self._msg(oid, tid, "references", oid_repr(ref), klass,
                              "at", pos)
                    result = True

        elif oid in self.oids:
            # Or maybe it's a version abort.
            self._msg(oid, tid, "creation undo at", pos)
            result = True

        return result
