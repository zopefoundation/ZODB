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
"""

import asyncore  
import sys, os, getopt, string, time
##sys.path.insert(0, os.getcwd())

import ZODB, ZODB.FileStorage
import Persistence
import ZEO.ClientStorage, ZEO.StorageServer

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

def main(args):
    opts, args = getopt.getopt(args, 'zd:n:Ds:LM')
    s = None
    compress = None
    data=sys.argv[0]
    nrep=5
    minimize=0
    detailed=1
    cache = None
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

    zeo_pipe = None
    if s:
        s = __import__(s, globals(), globals(), ('__doc__',))
        s = s.Storage
    else:
        rd, wr = os.pipe()
        pid = os.fork()
        if pid:
            # in the child, run the storage server
            os.close(wr)
            import asyncore
            ZEOExit(rd)
            fs = ZODB.FileStorage.FileStorage(fs_name, create=1)
            serv = ZEO.StorageServer.StorageServer(('', 1975), {'1':fs})
            asyncore.loop()
        else:
            os.close(rd)
            zeo_pipe = wr
            s = ZEO.ClientStorage.ClientStorage(('', 1975), debug=0,
                                                client=cache)
            if hasattr(s, 'is_connected'):
                while not s.is_connected():
                    time.sleep(0.1)
            else:
                time.sleep(1.0)

    data=open(data).read()
    db=ZODB.DB(s,
               # disable cache deactivation
               cache_size=4000,
               cache_deactivate_after=6000,)
    db.open().root()

    results={1:0, 10:0, 100:0, 1000:0}
    for j in range(nrep):
        for r in 1, 10, 100, 1000:
            t = time.time()
            
            jar = db.open()
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
            jar.close()
            
            t = time.time() - t
            if detailed:
                print "%s\t%s\t%.4f" % (j, r, t)
            results[r] = results[r] + t
            rt=d=p=v=None # release all references
            if minimize:
                time.sleep(3)
                jar.cacheMinimize(3)

    if zeo_pipe:
        os.write(zeo_pipe, "done")

    if detailed:
        print '-'*24
    for r in 1, 10, 100, 1000:
        t=results[r]/nrep
        print "mean:\t%s\t%.4f\t%.4f (s/o)" % (r, t, t/r)
    
##def compress(s):
##    c = zlib.compressobj()
##    o = c.compress(s)
##    return o + c.flush()    

if __name__=='__main__':
    main(sys.argv[1:])
