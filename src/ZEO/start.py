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
"""Start the server storage.

$Id: start.py,v 1.45 2002/09/23 01:52:50 gvanrossum Exp $
"""
from __future__ import nested_scopes

import sys, os, getopt
import types

def directory(p, n=1):
    d = p
    while n:
        d = os.path.split(d)[0]
        if not d or d == '.':
            d = os.getcwd()
        n -= 1
    return d

def get_storage(m, n, cache={}):
    p = sys.path
    d, m = os.path.split(m)
    if m.endswith('.py'):
        m = m[:-3]
    im = cache.get((d, m))
    if im is None:
        if d:
            p = [d] + p
        import imp
        im = imp.find_module(m, p)
        im = imp.load_module(m, *im)
        cache[(d, m)] = im
    return getattr(im, n)

def set_uid(arg):
    """Try to set uid and gid based on -u argument.

    This will only work if this script is run by root.
    """
    try:
        import pwd
    except ImportError:
        LOG('ZEO/start.py', INFO, ("Can't set uid to %s."
                                "pwd module is not available." % arg))
        return
    try:
        gid = None
        try:
            arg = int(arg)
        except: # conversion could raise all sorts of errors
            uid = pwd.getpwnam(arg)[2]
            gid = pwd.getpwnam(arg)[3]
        else:
            uid = pwd.getpwuid(arg)[2]
            gid = pwd.getpwuid(arg)[3]
        if gid is not None:
            try:
                os.setgid(gid)
            except OSError:
                pass
        try:
            os.setuid(uid)
        except OSError:
            pass
    except KeyError:
        LOG('ZEO/start.py', ERROR, ("can't find uid %s" % arg))

def setup_signals(storages):
    try:
        import signal
    except ImportError:
        return

    try:
        xfsz = signal.SIFXFSZ
    except AttributeError:
        pass
    else:
        signal.signal(xfsz, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, lambda sig, frame: shutdown(storages))
    signal.signal(signal.SIGINT, lambda sig, frame: shutdown(storages, 0))
    try:
        signal.signal(signal.SIGHUP, rotate_logs_handler)
    except:
        pass

def main(argv):
    me = argv[0]
    sys.path.insert(0, directory(me, 2))

    global LOG, INFO, ERROR
    from zLOG import LOG, INFO, ERROR, PANIC
    from ZEO.util import Environment
    env = Environment(me)

    # XXX hack for profiling support
    global unix, storages, zeo_pid, asyncore

    args = []
    last = ''
    for a in argv[1:]:
        if (a[:1] != '-' and a.find('=') > 0 and last != '-S'): # lame, sorry
            a = a.split("=")
            os.environ[a[0]] = "=".join(a[1:])
            continue
        args.append(a)
        last = a

    usage="""%s [options] [filename]

    where options are:

       -D -- Run in debug mode

       -d -- Set STUPD_LOG_SEVERITY to -300

       -U -- Unix-domain socket file to listen on

       -u username or uid number

         The username to run the ZEO server as. You may want to run
         the ZEO server as 'nobody' or some other user with limited
         resouces. The only works under Unix, and if ZServer is
         started by root.

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
               The module path should omit the prefix (e.g. '.py').

            attr_name -- This is the name to which the storage object
              is assigned in the module.

       -P file -- Run under profile and dump output to file.  Implies the
          -s flag.

    if no file name is specified, then %s is used.
    """ % (me, env.fs)

    try:
        opts, args = getopt.getopt(args, 'p:Dh:U:sS:u:P:d')
    except getopt.error, msg:
        print usage
        print msg
        sys.exit(1)

    port = None
    debug = 0
    host = ''
    unix =None
    Z = 1
    UID = 'nobody'
    prof = None
    detailed = 0
    fs = None
    for o, v in opts:
        if o =='-p':
            port = int(v)
        elif o =='-h':
            host = v
        elif o =='-U':
            unix = v
        elif o =='-u':
            UID = v
        elif o =='-D':
            debug = 1
        elif o =='-d':
            detailed = 1
        elif o =='-s':
            Z = 0
        elif o =='-P':
            prof = v

    if prof:
        Z = 0

    if port is None and unix is None:
        print usage
        print 'No port specified.'
        sys.exit(1)

    if args:
        if len(args) > 1:
            print usage
            print 'Unrecognizd arguments: ', " ".join(args[1:])
            sys.exit(1)
        fs = args[0]

    if debug:
        os.environ['Z_DEBUG_MODE'] = '1'
    if detailed:
        os.environ['STUPID_LOG_SEVERITY'] = '-300'

    set_uid(UID)

    if Z:
        try:
            import posix
        except:
            pass
        else:
            import zdaemon
            zdaemon.run(sys.argv, '')

    try:

        import ZEO.StorageServer, asyncore

        storages = {}
        for o, v in opts:
            if o == '-S':
                n, m = v.split("=", 1)
                if m.find(":") >= 0:
                    # we got an attribute name
                    m, a = m.split(':')
                else:
                    # attribute name must be same as storage name
                    a=n
                storages[n]=get_storage(m,a)

        if not storages:
            from ZODB.FileStorage import FileStorage
            storages['1'] = FileStorage(fs or env.fs)

        # Try to set up a signal handler
        setup_signals(storages)

        items = storages.items()
        items.sort()
        for kv in items:
            LOG('ZEO/start.py', INFO, 'Serving %s:\t%s' % kv)

        if not unix:
            unix = host, port

        ZEO.StorageServer.StorageServer(unix, storages)

        try:
            ppid, pid = os.getppid(), os.getpid()
        except:
            pass # getpid not supported
        else:
            open(env.zeo_pid,'w').write("%s %s" % (ppid, pid))

    except:
        # Log startup exception and tell zdaemon not to restart us.
        info = sys.exc_info()
        try:
            LOG("ZEO/start.py", PANIC, "Startup exception", error=info)
        except:
            pass

        import traceback
        traceback.print_exception(*info)

        sys.exit(0)

    try:
        asyncore.loop()
    except SystemExit:
        raise
    except:
        info = sys.exc_info()
        try:
            LOG("ZEO/start.py", PANIC, "Unexpected error", error=info)
        except:
            pass
        import traceback
        traceback.print_exception(*info)
        sys.exit(1)

def rotate_logs():
    import zLOG
    # There hasn't been a clear way to reinitialize the MinimalLogger.
    # I'll checkin the public initialize() method soon, but also try some
    # other strategies for older Zope installs :-(.
    init = getattr(zLOG, 'initialize', None)
    if init is not None:
        init()
        return
    # This will work if the minimal logger is in use, but not if some
    # other logger is active.
    import zLOG.MinimalLogger
    zLOG.MinimalLogger._log.initialize()

def rotate_logs_handler(signum, frame):
    rotate_logs()

    import signal
    signal.signal(signal.SIGHUP, rotate_logs_handler)

def shutdown(storages, die=1):
    LOG("ZEO/start.py", INFO, "Received signal")
    import asyncore

    # Do this twice, in case we got some more connections
    # while going through the loop.  This is really sort of
    # unnecessary, since we now use so_reuseaddr.
    for ignored in 1,2:
        for socket in asyncore.socket_map.values():
            try:
                socket.close()
            except:
                pass

    for storage in storages.values():
        try:
            storage.close()
        finally:
            pass

    try:
        s = die and "shutdown" or "restart"
        LOG('ZEO/start.py', INFO, "Shutting down (%s)" % s)
    except:
        pass

    if die:
        sys.exit(0)
    else:
        sys.exit(1)

if __name__=='__main__':
    main(sys.argv)
