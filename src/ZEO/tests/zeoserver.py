##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Helper file used to launch a ZEO server cross platform"""

import asyncore
import errno
import getopt
import logging
import os
import signal
import socket
import sys
import threading
import time
import ZEO.runzeo
import ZEO.zrpc.connection

def cleanup(storage):
    # FileStorage and the Berkeley storages have this method, which deletes
    # all files and directories used by the storage.  This prevents @-files
    # from clogging up /tmp
    try:
        storage.cleanup()
    except AttributeError:
        pass

logger = logging.getLogger('ZEO.tests.zeoserver')

def log(label, msg, *args):
    message = "(%s) %s" % (label, msg)
    logger.debug(message, *args)


class ZEOTestServer(asyncore.dispatcher):
    """A server for killing the whole process at the end of a test.

    The first time we connect to this server, we write an ack character down
    the socket.  The other end should block on a recv() of the socket so it
    can guarantee the server has started up before continuing on.

    The second connect to the port immediately exits the process, via
    os._exit(), without writing data on the socket.  It does close and clean
    up the storage first.  The other end will get the empty string from its
    recv() which will be enough to tell it that the server has exited.

    I think this should prevent us from ever getting a legitimate addr-in-use
    error.
    """
    __super_init = asyncore.dispatcher.__init__

    def __init__(self, addr, server, keep):
        self.__super_init()
        self._server = server
        self._sockets = [self]
        self._keep = keep
        # Count down to zero, the number of connects
        self._count = 1
        self._label ='%d @ %s' % (os.getpid(), addr)
        if isinstance(addr, str):
            self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        else:
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        # Some ZEO tests attempt a quick start of the server using the same
        # port so we have to set the reuse flag.
        self.set_reuse_addr()
        try:
            self.bind(addr)
        except:
            # We really want to see these exceptions
            import traceback
            traceback.print_exc()
            raise
        self.listen(5)
        self.log('bound and listening')

    def log(self, msg, *args):
        log(self._label, msg, *args)

    def handle_accept(self):
        sock, addr = self.accept()
        self.log('in handle_accept()')
        # When we're done with everything, close the storage.  Do not write
        # the ack character until the storage is finished closing.
        if self._count <= 0:
            self.log('closing the storage')
            self._server.close_server()
            if not self._keep:
                for storage in self._server.storages.values():
                    cleanup(storage)
            self.log('exiting')
            # Close all the other sockets so that we don't have to wait
            # for os._exit() to get to it before starting the next
            # server process.
            for s in self._sockets:
                s.close()
            # Now explicitly close the socket returned from accept(),
            # since it didn't go through the wrapper.
            sock.close()
            os._exit(0)
        self.log('continuing')
        sock.send('X')
        self._count -= 1

    def register_socket(self, sock):
        # Register a socket to be closed when server shutsdown.
        self._sockets.append(sock)

class Suicide(threading.Thread):
    def __init__(self, addr):
        threading.Thread.__init__(self)
        self._adminaddr = addr

    def run(self):
        # If this process doesn't exit in 330 seconds, commit suicide.
        # The client threads in the ConcurrentUpdate tests will run for
        # as long as 300 seconds.  Set this timeout to 330 to minimize
        # chance that the server gives up before the clients.
        time.sleep(999)
        log(str(os.getpid()), "suicide thread invoking shutdown")

        # If the server hasn't shut down yet, the client may not be
        # able to connect to it.  If so, try to kill the process to
        # force it to shutdown.
        if hasattr(os, "kill"):
            os.kill(pid, signal.SIGTERM)
            time.sleep(5)
            os.kill(pid, signal.SIGKILL)
        else:
            from ZEO.tests.forker import shutdown_zeo_server
            # Nott:  If the -k option was given to zeoserver, then the
            # process will go away but the temp files won't get
            # cleaned up.
            shutdown_zeo_server(self._adminaddr)


def main():
    global pid
    pid = os.getpid()
    label = str(pid)
    log(label, "starting")

    # We don't do much sanity checking of the arguments, since if we get it
    # wrong, it's a bug in the test suite.
    keep = 0
    configfile = None
    suicide = True
    # Parse the arguments and let getopt.error percolate
    opts, args = getopt.getopt(sys.argv[1:], 'dkSC:v:')
    for opt, arg in opts:
        if opt == '-k':
            keep = 1
        if opt == '-d':
            ZEO.zrpc.connection.debug_zrpc = True
        elif opt == '-C':
            configfile = arg
        elif opt == '-S':
            suicide = False
        elif opt == '-v':
            ZEO.zrpc.connection.Connection.current_protocol = arg

    zo = ZEO.runzeo.ZEOOptions()
    zo.realize(["-C", configfile])
    addr = zo.address

    if zo.auth_protocol == "plaintext":
        __import__('ZEO.tests.auth_plaintext')

    if isinstance(addr, tuple):
        test_addr = addr[0], addr[1]+1
    else:
        test_addr = addr + '-test'
    log(label, 'creating the storage server')
    storage = zo.storages[0].open()
    mon_addr = None
    if zo.monitor_address:
        mon_addr = zo.monitor_address
    server = ZEO.runzeo.create_server({"1": storage}, zo)

    try:
        log(label, 'creating the test server, keep: %s', keep)
        t = ZEOTestServer(test_addr, server, keep)
    except socket.error, e:
        if e[0] != errno.EADDRINUSE:
            raise
        log(label, 'addr in use, closing and exiting')
        storage.close()
        cleanup(storage)
        sys.exit(2)

    t.register_socket(server.dispatcher)
    if suicide:
        # Create daemon suicide thread
        d = Suicide(test_addr)
        d.setDaemon(1)
        d.start()
    # Loop for socket events
    log(label, 'entering asyncore loop')
    asyncore.loop()


if __name__ == '__main__':
    import warnings
    warnings.simplefilter('ignore')
    main()
