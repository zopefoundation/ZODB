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
"""Simple script for repairing damaged FileStorage files.

Usage: %s [-f] input output

Recover data from a FileStorage data file, skipping over damaged
data. Any damaged data will be lost. This could lead to useless output
of critical data were lost.

Options:

    -f
       Force output to output file even if it exists

    -v level

       Set the verbosity level:

         0 -- Show progress indicator (default)

         1 -- Show transaction times and sizes

         2 -- Show transaction times and sizes, and
              show object (record) ids, versions, and sizes.

    -p

       Copy partial transactions. If a data record in the middle of a
       transaction is bad, the data up to the bad data are packed. The
       output record is marked as packed. If this option is not used,
       transaction with any bad data are skipped.

    -P t

       Pack data to t seconds in the past. Note that is the "-p"
       option is used, then t should be 0.


Important note: The ZODB package must be imporatble.  You may need
                to adjust PYTHONPATH accordingly.

"""

# Algorithm:
#
#     position to start of input
#     while 1:
#         if end of file: break
#          try: copy_transaction
#          except:
#                 scan for transaction
#                 continue

import sys, os

if __name__ == '__main__' and len(sys.argv) < 3:
    print __doc__ % sys.argv[0]

def die(mess=''):
    if not mess: mess="%s: %s" % sys.exc_info()[:2]
    print mess+'\n'
    sys.exit(1)

try: import ZODB
except ImportError:
    if os.path.exists('ZODB'): sys.path.append('.')
    elif os.path.exists('FileStorage.py'):  sys.path.append('..')
    import ZODB


import getopt, ZODB.FileStorage, struct, time
from struct import unpack
from ZODB.utils import t32, p64, u64
from persistent.TimeStamp import TimeStamp
from cPickle import loads
from ZODB.FileStorage import RecordIterator

class ErrorFound(Exception): pass

def error(mess, *args):
    raise ErrorFound(mess % args)

def read_txn_header(f, pos, file_size, outp, ltid):
    # Read the transaction record
    f.seek(pos)
    h = f.read(23)
    if len(h) < 23:
        raise EOFError

    tid, stl, status, ul, dl, el = unpack(">8s8scHHH",h)
    if el < 0: el=t32-el

    tl = u64(stl)

    if pos + (tl + 8) > file_size:
        error("bad transaction length at %s", pos)

    if tl < (23 + ul + dl + el):
        error("invalid transaction length, %s, at %s", tl, pos)

    if ltid and tid < ltid:
        error("time-stamp reducation %s < %s, at %s", u64(tid), u64(ltid), pos)

    if status == "c":
        truncate(f, pos, file_size, output)
        raise EOFError

    if status not in " up":
        error("invalid status, %r, at %s", status, pos)

    tpos = pos
    tend = tpos + tl

    if status == "u":
        # Undone transaction, skip it
        f.seek(tend)
        h = f.read(8)
        if h != stl:
            error("inconsistent transaction length at %s", pos)
        pos = tend + 8
        return pos, None, tid

    pos = tpos+(23+ul+dl+el)
    user = f.read(ul)
    description = f.read(dl)
    if el:
        try: e=loads(f.read(el))
        except: e={}
    else: e={}

    result = RecordIterator(tid, status, user, description, e, pos, tend,
                            f, tpos)
    pos = tend

    # Read the (intentionally redundant) transaction length
    f.seek(pos)
    h = f.read(8)
    if h != stl:
        error("redundant transaction length check failed at %s", pos)
    pos += 8

    return pos, result, tid

def truncate(f, pos, file_size, outp):
    """Copy data from pos to end of f to a .trNNN file."""

    i = 0
    while 1:
        trname = outp + ".tr%d" % i
        if os.path.exists(trname):
            i += 1
    tr = open(trname, "wb")
    copy(f, tr, file_size - pos)
    f.seek(pos)
    tr.close()

def copy(src, dst, n):
    while n:
        buf = src.read(8096)
        if not buf:
            break
        if len(buf) > n:
            buf = buf[:n]
        dst.write(buf)
        n -= len(buf)

def scan(f, pos):
    """Return a potential transaction location following pos in f.

    This routine scans forward from pos looking for the last data
    record in a transaction.  A period '.' always occurs at the end of
    a pickle, and an 8-byte transaction length follows the last
    pickle.  If a period is followed by a plausible 8-byte transaction
    length, assume that we have found the end of a transaction.

    The caller should try to verify that the returned location is
    actually a transaction header.
    """
    while 1:
        f.seek(pos)
        data = f.read(8096)
        if not data:
            return 0

        s = 0
        while 1:
            l = data.find(".", s)
            if l < 0:
                pos += len(data)
                break
            # If we are less than 8 bytes from the end of the
            # string, we need to read more data.
            s = l + 1
            if s > len(data) - 8:
                pos += l
                break
            tl = u64(data[s:s+8])
            if tl < pos:
                return pos + s + 8

def iprogress(i):
    if i % 2:
        print ".",
    else:
        print (i/2) % 10,
    sys.stdout.flush()

def progress(p):
    for i in range(p):
        iprogress(i)

def main():
    try:
        opts, (inp, outp) = getopt.getopt(sys.argv[1:], "fv:pP:")
    except getopt.error:
        die()
        print __doc__ % argv[0]

    force = partial = verbose = 0
    pack = None
    for opt, v in opts:
        if opt == "-v":
            verbose = int(v)
        elif opt == "-p":
            partial = 1
        elif opt == "-f":
            force = 1
        elif opt == "-P":
            pack = time.time() - float(v)

    recover(inp, outp, verbose, partial, force, pack)

def recover(inp, outp, verbose=0, partial=0, force=0, pack=0):
    print "Recovering", inp, "into", outp

    if os.path.exists(outp) and not force:
        die("%s exists" % outp)

    f = open(inp, "rb")
    if f.read(4) != ZODB.FileStorage.packed_version:
        die("input is not a file storage")

    f.seek(0,2)
    file_size = f.tell()

    ofs = ZODB.FileStorage.FileStorage(outp, create=1)
    _ts = None
    ok = 1
    prog1 = 0
    undone = 0

    pos = 4L
    ltid = None
    while pos:
        try:
            npos, txn, tid = read_txn_header(f, pos, file_size, outp, ltid)
        except EOFError:
            break
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception, err:
            print "error reading txn header:", err
            if not verbose:
                progress(prog1)
            pos = scan(f, pos)
            if verbose > 1:
                print "looking for valid txn header at", pos
            continue
        ltid = tid

        if txn is None:
            undone = undone + npos - pos
            pos = npos
            continue
        else:
            pos = npos

        tid = txn.tid

        if _ts is None:
            _ts = TimeStamp(tid)
        else:
            t = TimeStamp(tid)
            if t <= _ts:
                if ok:
                    print ("Time stamps out of order %s, %s" % (_ts, t))
                ok = 0
                _ts = t.laterThan(_ts)
                tid = `_ts`
            else:
                _ts = t
                if not ok:
                    print ("Time stamps back in order %s" % (t))
                    ok = 1

        ofs.tpc_begin(txn, tid, txn.status)

        if verbose:
            print "begin", pos, _ts,
            if verbose > 1:
                print
            sys.stdout.flush()

        nrec = 0
        try:
            for r in txn:
                if verbose > 1:
                    if r.data is None:
                        l = "bp"
                    else:
                        l = len(r.data)

                    print "%7d %s %s" % (u64(r.oid), l, r.version)
                ofs.restore(r.oid, r.serial, r.data, r.version, r.data_txn,
                            txn)
                nrec += 1
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception, err:
            if partial and nrec:
                ofs._status = "p"
                ofs.tpc_vote(txn)
                ofs.tpc_finish(txn)
                if verbose:
                    print "partial"
            else:
                ofs.tpc_abort(txn)
            print "error copying transaction:", err
            if not verbose:
                progress(prog1)
            pos = scan(f, pos)
            if verbose > 1:
                print "looking for valid txn header at", pos
        else:
            ofs.tpc_vote(txn)
            ofs.tpc_finish(txn)
            if verbose:
                print "finish"
                sys.stdout.flush()

        if not verbose:
            prog = pos * 20l / file_size
            while prog > prog1:
                prog1 = prog1 + 1
                iprogress(prog1)


    bad = file_size - undone - ofs._pos

    print "\n%s bytes removed during recovery" % bad
    if undone:
        print "%s bytes of undone transaction data were skipped" % undone

    if pack is not None:
        print "Packing ..."
        from ZODB.referencesf import referencesf
        ofs.pack(pack, referencesf)

    ofs.close()

if __name__ == "__main__":
    main()
