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


"""Simple script for repairing damaged FileStorage files.

Usage: %s [-f] input output

Recover data from a FileStorage data file, skipping over damaged
data. Any damaged data will be lost. This could lead to useless output
of critical data were lost.

Options:

    -f
       Force output to putput file even if it exists

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

    
Important note: The ZODB package must be imporable.  You may need
                to adjust the Python path accordingly.

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
from ZODB.utils import t32, p64, U64
from ZODB.TimeStamp import TimeStamp
from cPickle import loads
from ZODB.FileStorage import RecordIterator

class EOF(Exception): pass
class ErrorFound(Exception): pass

def error(mess, *args):
    raise ErrorFound(mess % args)

def read_transaction_header(file, pos, file_size):
    # Read the transaction record
    seek=file.seek
    read=file.read

    seek(pos)
    h=read(23)
    if len(h) < 23: raise EOF

    tid, stl, status, ul, dl, el = unpack(">8s8scHHH",h)
    if el < 0: el=t32-el

    tl=U64(stl)

    if status=='c': raise EOF

    if pos+(tl+8) > file_size:
        error("bad transaction length at %s", pos)

    if status not in ' up':
        error('invalid status, %s, at %s', status, pos)

    if tl < (23+ul+dl+el):
        error('invalid transaction length, %s, at %s', tl, pos)

    tpos=pos
    tend=tpos+tl

    if status=='u':
        # Undone transaction, skip it
        seek(tend)
        h=read(8)
        if h != stl: error('inconsistent transaction length at %s', pos)
        pos=tend+8
        return pos, None

    pos=tpos+(23+ul+dl+el)
    user=read(ul)
    description=read(dl)
    if el:
        try: e=loads(read(el))
        except: e={}
    else: e={}

    result=RecordIterator(
        tid, status, user, description, e,
        pos, (tend, file, seek, read,
              tpos,
              )
        )

    pos=tend

    # Read the (intentionally redundant) transaction length
    seek(pos)
    h=read(8)
    if h != stl:
        error("redundant transaction length check failed at %s", pos)
    pos=pos+8

    return pos, result

def scan(file, pos, file_size):
    seek=file.seek
    read=file.read
    while 1:
        seek(pos)
        data=read(8096)
        if not data: return 0

        s=0
        while 1:
            l=data.find('.', s)
            if l < 0:
                pos=pos+8096
                break
            if l > 8080:
                pos = pos + l
                break
            s=l+1
            tl=U64(data[s:s+8])
            if tl < pos:
                return pos + s + 8

def iprogress(i):
    if i%2: print '.',
    else: print (i/2)%10,
    sys.stdout.flush()

def progress(p):
    for i in range(p): iprogress(i) 

def recover(argv=sys.argv):

    try:
        opts, (inp, outp) = getopt.getopt(argv[1:], 'fv:pP:')
        force = partial = verbose = 0
        pack = None
        for opt, v in opts:
            if opt == '-v': verbose = int(v)
            elif opt == '-p': partial=1
            elif opt == '-f': force=1
            elif opt == '-P': pack=time.time()-float(v)

        
        force = filter(lambda opt: opt[0]=='-f', opts)
        partial = filter(lambda opt: opt[0]=='-p', opts)
        verbose = filter(lambda opt: opt[0]=='-v', opts)
        verbose = verbose and int(verbose[0][1]) or 0
        print 'Recovering', inp, 'into', outp
    except:
        die()
        print __doc__ % argv[0]
        

    if os.path.exists(outp) and not force:
        die("%s exists" % outp)

    file=open(inp, "rb")
    seek=file.seek
    read=file.read
    if read(4) != ZODB.FileStorage.packed_version:
        die("input is not a file storage")

    seek(0,2)
    file_size=file.tell()

    ofs=ZODB.FileStorage.FileStorage(outp, create=1)
    _ts=None
    ok=1
    prog1=0
    preindex={}; preget=preindex.get   # waaaa
    undone=0

    pos=4
    while pos:

        try:
            npos, transaction = read_transaction_header(file, pos, file_size)
        except EOF:
            break
        except:
            print "\n%s: %s\n" % sys.exc_info()[:2]
            if not verbose: progress(prog1)
            pos = scan(file, pos, file_size)
            continue

        if transaction is None:
            undone = undone + npos - pos
            pos=npos
            continue
        else:
            pos=npos

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

        if verbose:
            print 'begin', 
            if verbose > 1: print
            sys.stdout.flush()

        ofs.tpc_begin(transaction, tid, transaction.status)

        if verbose:
            print 'begin', pos, _ts,
            if verbose > 1: print
            sys.stdout.flush()

        nrec=0
        try:
            for r in transaction:
                oid=r.oid
                if verbose > 1: print U64(oid), r.version, len(r.data)
                pre=preget(oid, None)
                s=ofs.store(oid, pre, r.data, r.version, transaction)
                preindex[oid]=s
                nrec=nrec+1
        except:
            if partial and nrec:
                ofs._status='p'
                ofs.tpc_vote(transaction)
                ofs.tpc_finish(transaction)
                if verbose: print 'partial'
            else:
                ofs.tpc_abort(transaction)
            print "\n%s: %s\n" % sys.exc_info()[:2]
            if not verbose: progress(prog1)
            pos = scan(file, pos, file_size)
        else:
            ofs.tpc_vote(transaction)
            ofs.tpc_finish(transaction)
            if verbose:
                print 'finish'
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
                

if __name__=='__main__': recover()

