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
"""
  
import sys, os, getopt, string, time
sys.path.insert(0, os.getcwd())

import ZODB, ZODB.FileStorage
import Persistence

class P(Persistence.Persistent): pass

def main(args):

    opts, args = getopt.getopt(args, 'zd:n:Ds:')
    z=s=None
    data=sys.argv[0]
    nrep=5
    for o, v in opts:
        if o=='-n': nrep=string.atoi(v)
        elif o=='-d': data=v
        elif o=='-s': s=v
        elif o=='-z':
            global zlib
            import zlib
            z=compress
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
    db=ZODB.DB(s)

    results={}
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
            sys.stderr.write("%s %s %s\n" % (j, r, time.time()-t))
            sys.stdout.flush()
    
    
def compress(s):
    c=zlib.compressobj()
    o=c.compress(s)
    return o+c.flush()    

if __name__=='__main__': main(sys.argv[1:])
