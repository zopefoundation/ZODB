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
"""
  
import sys, os, getopt, string, time
sys.path.insert(0, os.getcwd())

import ZODB, ZODB.FileStorage
import Persistence

class P(Persistence.Persistent): pass

def main(args):

    opts, args = getopt.getopt(args, 'zd:n:Ds:LM')
    z=s=None
    data=sys.argv[0]
    nrep=5
    minimize=0
    detailed=1
    for o, v in opts:
        if o=='-n': nrep=string.atoi(v)
        elif o=='-d': data=v
        elif o=='-s': s=v
        elif o=='-z':
            global zlib
            import zlib
            z=compress
        elif o=='-L':
            minimize=1
        elif o=='-M':
            detailed=0
        elif o=='-D':
            global debug
            os.environ['STUPID_LOG_FILE']=''
            os.environ['STUPID_LOG_SEVERITY']='-999'
            __builtins__.__debug__=1


    if s:
        s=__import__(s, globals(), globals(), ('__doc__',))
        s=s.Storage
    else:
        s=ZODB.FileStorage.FileStorage('zeo_speed.fs', create=1)

    data=open(data).read()
    db=ZODB.DB(s,
               # disable cache deactivation
               cache_size=4000,
               cache_deactivate_after=6000,)

    results={1:0, 10:0, 100:0, 1000:0}
    for j in range(nrep):
        for r in 1, 10, 100, 1000:
            t=time.time()
            jar=db.open()
            get_transaction().begin()
            rt=jar.root()
            key='s%s' % r
            if rt.has_key(key): p=rt[key]
            else: rt[key]=p=P()
            for i in range(r):
                if z is not None: d=z(data)
                else: d=data
                v=getattr(p, str(i), P())
                v.d=d
                setattr(p,str(i),v)
            get_transaction().commit()
            jar.close()
            t=time.time()-t
            if detailed:
                sys.stderr.write("%s\t%s\t%.4f\n" % (j, r, t))
                sys.stdout.flush()
            results[r]=results[r]+t
            rt=d=p=v=None # release all references
            if minimize:
                time.sleep(3)
                jar.cacheMinimize(3)

    if detailed: print '-'*24
    for r in 1, 10, 100, 1000:
        t=results[r]/nrep
        sys.stderr.write("mean:\t%s\t%.4f\t%.4f (s/o)\n" % (r, t, t/r))


            
    
def compress(s):
    c=zlib.compressobj()
    o=c.compress(s)
    return o+c.flush()    

if __name__=='__main__': main(sys.argv[1:])
