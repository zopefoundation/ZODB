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

"""Start the server storage.
"""

__version__ = "$Revision: 1.8 $"[11:-2]

import sys, os, getopt, string

def directory(p, n=1):
    d=p
    while n:
        d=os.path.split(d)[0]
        if not d or d=='.': d=os.getcwd()
        n=n-1
        
    return d

def get_storage(m, n):
    p=sys.path
    d, m = os.path.split(m)
    if d: p=[d]+p
    import imp
    if m[-3:]=='.py': m=m[:-3]
    im=imp.find_module(m,p)
    im=imp.load_module(m, im[0], im[1], im[2])
    return getattr(im, n)


def main(argv):
    me=argv[0]
    sys.path[:]==filter(None, sys.path)
    sys.path.insert(0, directory(me, 2))

    args=[]
    last=''
    for a in argv[1:]:
        if (a[:1] != '-' and string.find(a, '=') > 0
            and last != '-S' # lame, sorry
            ):
            a=string.split(a,'=')
            os.environ[a[0]]=string.join(a[1:],'=')
            continue
        args.append(a)
        last=a

    INSTANCE_HOME=os.environ.get('INSTANCE_HOME', directory(me, 4))

    zeo_pid=os.environ.get('ZEO_SERVER_PID',
                           os.path.join(INSTANCE_HOME, 'var', 'ZEO_SERVER.pid')
                           )

    opts, args = getopt.getopt(args, 'p:Dh:U:sS:')

    
    
    fs=os.path.join(INSTANCE_HOME, 'var', 'Data.fs')

    usage="""%s [options] [filename]

    where options are:

       -D -- Run in debug mode

       -U -- Unix-domain socket file to listen on

       -p port -- port to listen on

       -h adddress -- host address to listen on

       -s -- Don't use zdeamon

       -S storage_name=module_path:attr_name -- A storage specification

          where:

            storage_name -- is the storage name used in the ZEO protocol.
               This is the name that you give as the optional
               'storage' keyword argument to the ClientStorage constructor.

            module_path -- This is the path to a Python module
               that defines the storage object(s) to be served.
               The module path should ommit the prefix (e.g. '.py').

            attr_name -- This is the name to which the storage object
              is assigned in the module.

    if no file name is specified, then %s is used.
    """ % (me, fs)

    port=None
    debug=0
    host=''
    unix=None
    prefix=''
    Z=1
    for o, v in opts:
        if o=='-p': port=string.atoi(v)
        elif o=='-h': host=v
        elif o=='-U': unix=v
        elif o=='-D': debug=1
        elif o=='-s': Z=0

    try:
        from ZServer.medusa import asyncore
        sys.modules['asyncore']=asyncore
    except: pass

    if port is None and unix is None:
        print usage
        print 'No port specified.'
        sys.exit(1)

    if args:
        if len(args) > 1:
            print usage
            print 'Unrecognizd arguments: ', string.join(args[1:])
            sys.exit(1)
        fs=args[0]

    __builtins__.__debug__=debug
    if debug: os.environ['Z_DEBUG_MODE']='1'

    if Z:
        try: import posix
        except: pass
        else:
            import zdaemon
            zdaemon.run(sys.argv, '')

    import ZEO.StorageServer, asyncore, zLOG

    storages={}
    for o, v in opts:
        if o=='-S':
            n, m = string.split(v,'=')
            if string.find(m,':'):
                # we got an attribute name
                m, a = string.split(m,':')
            else:
                # attribute name must be same as storage name
                a=n
            storages[n]=get_storage(m,a)

    if not storages:
        import ZODB.FileStorage
        storages['1']=ZODB.FileStorage.FileStorage(fs)

    items=storages.items()
    items.sort()
    for kv in items:
        zLOG.LOG('ZEO Server', zLOG.INFO, 'Serving %s:\t%s' % kv)

    if not unix: unix=host, port

    ZEO.StorageServer.StorageServer(unix, storages)

    open(zeo_pid,'w').write("%s %s" % (os.getppid(), os.getpid()))
    
    asyncore.loop()


if __name__=='__main__': main(sys.argv)
