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
usage="""Test speed of a ZODB storage

Options:

    -d file    The data file to use as input.
               The default is this script.

    -n n       The number of repititions

    -s module  A module that defines a 'Storage'
               attribute, which is an open storage.
               If not specified, a FileStorage will ne
               used.

    -z         Test compressing data

    -D         Run in debug mode

    -L         Test loads as well as stores by minimizing
               the cache after eachrun

    -M         Output means only

    -C         Run with a persistent client cache

    -U         Run ZEO using a Unix domain socket

    -t n       Number of concurrent threads to run.
"""

import asyncore
import sys, os, getopt, string, time
##sys.path.insert(0, os.getcwd())

import ZODB, ZODB.FileStorage
import Persistence
import ZEO.ClientStorage, ZEO.StorageServer
from ZEO.tests import forker
from ZODB.POSException import ConflictError

class P(Persistence.Persistent):
    pass

fs_name = "zeo-speed.fs"

class ZEOExit(asyncore.file_dispatcher):
    """Used to exit ZEO.StorageServer when run is done"""
    def writable(self):
        return 0
    def readable(self):
        return 1
    def handle_read(self):
        buf = self.recv(4)
        assert buf == "done"
        self.delete_fs()
        os._exit(0)
    def handle_close(self):
        print "Parent process exited unexpectedly"
        self.delete_fs()
        os._exit(0)
    def delete_fs(self):
        os.unlink(fs_name)
        os.unlink(fs_name + ".lock")
        os.unlink(fs_name + ".tmp")

def work(db, results, nrep, compress, data, detailed, minimize, threadno=None):
    for j in range(nrep):
        for r in 1, 10, 100, 1000:
            t = time.time()
            conflicts = 0

            jar = db.open()
            while 1:
                try:
                    get_transaction().begin()
                    rt = jar.root()
                    key = 's%s' % r
                    if rt.has_key(key):
                        p = rt[key]
                    else:
                        rt[key] = p =P()
                    for i in range(r):
                        v = getattr(p, str(i), P())
                        if compress is not None:
                            v.d = compress(data)
                        else:
                            v.d = data
                        setattr(p, str(i), v)
                    get_transaction().commit()
                except ConflictError:
                    conflicts = conflicts + 1
                else:
                    break
            jar.close()

            t = time.time() - t
            if detailed:
                if threadno is None:
                    print "%s\t%s\t%.4f\t%d" % (j, r, t, conflicts)
                else:
                    print "%s\t%s\t%.4f\t%d\t%d" % (j, r, t, conflicts,
                                                    threadno)
            results[r].append((t, conflicts))
            rt=d=p=v=None # release all references
            if minimize:
                time.sleep(3)
                jar.cacheMinimize(3)

def main(args):
    opts, args = getopt.getopt(args, 'zd:n:Ds:LMt:U')
    s = None
    compress = None
    data=sys.argv[0]
    nrep=5
    minimize=0
    detailed=1
    cache = None
    domain = 'AF_INET'
    threads = 1
    for o, v in opts:
        if o=='-n': nrep = int(v)
        elif o=='-d': data = v
        elif o=='-s': s = v
        elif o=='-z':
            import zlib
            compress = zlib.compress
        elif o=='-L':
            minimize=1
        elif o=='-M':
            detailed=0
        elif o=='-D':
            global debug
            os.environ['STUPID_LOG_FILE']=''
            os.environ['STUPID_LOG_SEVERITY']='-999'
            debug = 1
        elif o == '-C':
            cache = 'speed'
        elif o == '-U':
            domain = 'AF_UNIX'
        elif o == '-t':
            threads = int(v)

    zeo_pipe = None
    if s:
        s = __import__(s, globals(), globals(), ('__doc__',))
        s = s.Storage
        server = None
    else:
        fs = ZODB.FileStorage.FileStorage(fs_name, create=1)
        s, server, pid = forker.start_zeo(fs, domain=domain)

    data=open(data).read()
    db=ZODB.DB(s,
               # disable cache deactivation
               cache_size=4000,
               cache_deactivate_after=6000,)

    print "Beginning work..."
    results={1:[], 10:[], 100:[], 1000:[]}
    if threads > 1:
        import threading
        l = []
        for i in range(threads):
            t = threading.Thread(target=work,
                                 args=(db, results, nrep, compress, data,
                                       detailed, minimize, i))
            l.append(t)
        for t in l:
            t.start()
        for t in l:
            t.join()

    else:
        work(db, results, nrep, compress, data, detailed, minimize)

    if server is not None:
        server.close()
        os.waitpid(pid, 0)

    if detailed:
        print '-'*24
    print "num\tmean\tmin\tmax"
    for r in 1, 10, 100, 1000:
        times = []
        for time, conf in results[r]:
            times.append(time)
        t = mean(times)
        print "%d\t%.4f\t%.4f\t%.4f" % (r, t, min(times), max(times))

def mean(l):
    tot = 0
    for v in l:
        tot = tot + v
    return tot / len(l)

##def compress(s):
##    c = zlib.compressobj()
##    o = c.compress(s)
##    return o + c.flush()

if __name__=='__main__':
    main(sys.argv[1:])
