# Start the server storage

import sys, os, getopt, string

def directory(p):
    d=os.path.split(p)[0]
    if not d or d=='.': return os.getcwd()
    return d
    

def main(argv):
    me=argv[0]
    sys.path[:]==filter(None, sys.path)
    sys.path.insert(0, directory(directory(me)))

    args=[]
    for a in argv[1:]:
        if string.find(a, '=') > 0:
            a=string.split(a,'=')
            os.environ[a[0]]=string.join(a[1:],'=')
            continue
        args.append(a)

    opts, args = getopt.getopt(args, 'p:Dh:')

    fs=directory(directory(directory(directory(me))))+'/var/Data.fs'

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

    import ZEO.StorageServer, ZODB.FileStorage, asyncore

    print 'Serving', fs

    ZEO.StorageServer.StorageServer(
        (host,port),
        {
            '1': ZODB.FileStorage.FileStorage(fs)
            },
        )
    asyncore.loop()


if __name__=='__main__': main(sys.argv)
