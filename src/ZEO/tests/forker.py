"""Library for forking storage server and connecting client storage"""

import asyncore
import os
import profile
import random
import socket
import sys
import types
import ZEO.ClientStorage, ZEO.StorageServer

PROFILE = 0

def get_port():
    """Return a port that is not in use.

    Checks if a port is in use by trying to connect to it.  Assumes it
    is not in use if connect raises an exception.

    Raises RuntimeError after 10 tries.
    """
    for i in range(10):
        port = random.randrange(20000, 30000)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect(('localhost', port))
        except socket.error:
            # XXX check value of error?
            return port
    raise RuntimeError, "Can't find port"

if os.name == "nt":

    def start_zeo_server(storage_name, args, port=None):
        """Start a ZEO server in a separate process.

        Returns the ZEO port, the test server port, and the pid.
        """
        import ZEO.tests.winserver
        port = get_port()
        script = ZEO.tests.winserver.__file__
        if script.endswith('.pyc'):
            script = script[:-1]
        args = (sys.executable, script, str(port), storage_name) + args
        d = os.environ.copy()
        d['PYTHONPATH'] = os.pathsep.join(sys.path)
        pid = os.spawnve(os.P_NOWAIT, sys.executable, args, d)
        return ('localhost', port), ('localhost', port + 1), pid

else:

    class ZEOServerExit(asyncore.file_dispatcher):
        """Used to exit ZEO.StorageServer when run is done"""

        def writable(self):
            return 0

        def readable(self):
            return 1

        def handle_read(self):
            buf = self.recv(4)
            if buf:
                assert buf == "done"
                asyncore.socket_map.clear()

        def handle_close(self):
            asyncore.socket_map.clear()

    class ZEOClientExit:
        """Used by client to cause server to exit"""
        def __init__(self, pipe):
            self.pipe = pipe

        def close(self):
            os.write(self.pipe, "done")

    def start_zeo_server(storage, addr):
        rd, wr = os.pipe()
        pid = os.fork()
        if pid == 0:
            if PROFILE:
                p = profile.Profile()
                p.runctx("run_server(storage, addr, rd, wr)", globals(),
                         locals())
                p.dump_stats("stats.s.%d" % os.getpid())
            else:
                run_server(storage, addr, rd, wr)
            os._exit(0)
        else:
            os.close(rd)
            return pid, ZEOClientExit(wr)

    def run_server(storage, addr, rd, wr):
        # in the child, run the storage server
        os.close(wr)
        ZEOServerExit(rd)
        serv = ZEO.StorageServer.StorageServer(addr, {'1':storage})
        asyncore.loop()
        storage.close()
        if isinstance(addr, types.StringType):
            os.unlink(addr)

    def start_zeo(storage, cache=None, cleanup=None, domain="AF_INET",
                  storage_id="1", cache_size=20000000):
        """Setup ZEO client-server for storage.

        Returns a ClientStorage instance and a ZEOClientExit instance.

        XXX Don't know if os.pipe() will work on Windows.
        """

        if domain == "AF_INET":
            addr = '', get_port()
        elif domain == "AF_UNIX":
            import tempfile
            addr = tempfile.mktemp()
        else:
            raise ValueError, "bad domain: %s" % domain

        pid, exit = start_zeo_server(storage, addr)
        s = ZEO.ClientStorage.ClientStorage(addr, storage_id,
                                            debug=1, client=cache,
                                            cache_size=cache_size,
                                            min_disconnect_poll=0.5)
        return s, exit, pid

