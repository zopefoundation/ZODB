# Start the server storage

import sys, os, getopt, string

def directory(p, n=1):
    d=p
    while n:
        d=os.path.split(d)[0]
        if not d or d=='.': d=os.getcwd()
        n=n-1
        
    return d
    

def main(argv):
    me=argv[0]
    sys.path[:]==filter(None, sys.path)
    sys.path.insert(0, directory(me, 2))

    args=[]
    for a in argv[1:]:
        if string.find(a, '=') > 0:
            a=string.split(a,'=')
            os.environ[a[0]]=string.join(a[1:],'=')
            continue
        args.append(a)

    INSTANCE_HOME=os.environ.get('INSTANCE_HOME', directory(me, 4))

    zeo_pid=os.environ.get('ZEO_SERVER_PID',
                           os.path.join(INSTANCE_HOME, 'var', 'ZEO_SERVER.pid')
                           )

    opts, args = getopt.getopt(args, 'p:Dh:')
    
    fs=os.path.join(INSTANCE_HOME, 'var', 'Data.fs')

    usage="""%s -p port [options] [filename]

    where options are:

       -D -- Run in debug mode

       -h -- host address to listen on

    if no file name is specified, then %s is used.
    """ % (me, fs)

    port=None
    debug=0
    host=''
    for o, v in opts:
        if o=='-p': port=string.atoi(v)
        elif o=='-h': host=v
        elif o=='-D': debug=1

    if port is None:
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

    try: import posix
    except: pass
    else:
        import zdaemon
        zdaemon.run(sys.argv, '')

    import ZEO.StorageServer, ZODB.FileStorage, asyncore, zLOG

    zLOG.LOG('ZEO Server', zLOG.INFO, 'Serving %s' % fs)

    ZEO.StorageServer.StorageServer(
        (host,port),
        {
            '1': ZODB.FileStorage.FileStorage(fs)
            },
        )

    open(zeo_pid,'w').write("%s %s" % (os.getpid(), os.getppid()))
    
    asyncore.loop()


if __name__=='__main__': main(sys.argv)
