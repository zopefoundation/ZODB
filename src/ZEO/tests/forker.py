"""Library for forking storage server and connecting client storage"""

import asyncore
import os
import sys
import ThreadedAsync
import ZEO.ClientStorage, ZEO.StorageServer

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
        # in the child, run the storage server
        try:
            os.close(wr)
            ZEOServerExit(rd)
            serv = ZEO.StorageServer.StorageServer(addr, {'1':storage})
            asyncore.loop()
            storage.close()
            if domain == "AF_UNIX":
                os.unlink(addr)
            if cleanup:
                cleanup()
        finally:
            os._exit(0)
    else:
        os.close(rd)
        return pid, ZEOClientExit(wr)

def start_zeo(storage, cache=None, cleanup=None, domain="AF_INET"):
    """Setup ZEO client-server for storage.

    Returns a ClientStorage instance and a ZEOClientExit instance.

    XXX Don't know if os.pipe() will work on Windows.
    """

    if domain == "AF_INET":
        import random
        addr = '', random.randrange(2000, 3000)
    elif domain == "AF_UNIX":
        import tempfile
        addr = tempfile.mktemp()
    else:
        raise ValueError, "bad domain: %s" % domain

    pid, exit = start_zeo_server(storage, addr)
    s = ZEO.ClientStorage.ClientStorage(addr, debug=1, client=cache)
    return s, exit, pid

