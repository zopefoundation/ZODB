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
"""Library for forking storage server and connecting client storage"""

import os
import random
import sys
import time
import errno
import socket
import subprocess
import logging
import StringIO
import tempfile
import logging
import ZODB.tests.util
import zope.testing.setupstack

logger = logging.getLogger('ZEO.tests.forker')

class ZEOConfig:
    """Class to generate ZEO configuration file. """

    def __init__(self, addr):
        self.address = addr
        self.read_only = None
        self.invalidation_queue_size = None
        self.invalidation_age = None
        self.monitor_address = None
        self.transaction_timeout = None
        self.authentication_protocol = None
        self.authentication_database = None
        self.authentication_realm = None

    def dump(self, f):
        print >> f, "<zeo>"
        print >> f, "address %s:%s" % self.address
        if self.read_only is not None:
            print >> f, "read-only", self.read_only and "true" or "false"
        if self.invalidation_queue_size is not None:
            print >> f, "invalidation-queue-size", self.invalidation_queue_size
        if self.invalidation_age is not None:
            print >> f, "invalidation-age", self.invalidation_age
        if self.monitor_address is not None:
            print >> f, "monitor-address %s:%s" % self.monitor_address
        if self.transaction_timeout is not None:
            print >> f, "transaction-timeout", self.transaction_timeout
        if self.authentication_protocol is not None:
            print >> f, "authentication-protocol", self.authentication_protocol
        if self.authentication_database is not None:
            print >> f, "authentication-database", self.authentication_database
        if self.authentication_realm is not None:
            print >> f, "authentication-realm", self.authentication_realm
        print >> f, "</zeo>"

        print >> f, """
        <eventlog>
          level INFO
          <logfile>
             path server-%s.log
          </logfile>
        </eventlog>
        """ % self.address[1]

    def __str__(self):
        f = StringIO.StringIO()
        self.dump(f)
        return f.getvalue()


def encode_format(fmt):
    # The list of replacements mirrors
    # ZConfig.components.logger.handlers._control_char_rewrites
    for xform in (("\n", r"\n"), ("\t", r"\t"), ("\b", r"\b"),
                  ("\f", r"\f"), ("\r", r"\r")):
        fmt = fmt.replace(*xform)
    return fmt


def start_zeo_server(storage_conf=None, zeo_conf=None, port=None, keep=False,
                     path='Data.fs', protocol=None, blob_dir=None,
                     suicide=True):
    """Start a ZEO server in a separate process.

    Takes two positional arguments a string containing the storage conf
    and a ZEOConfig object.

    Returns the ZEO address, the test server address, the pid, and the path
    to the config file.
    """

    if not storage_conf:
        storage_conf = '<filestorage>\npath %s\n</filestorage>' % path
        if blob_dir:
            storage_conf = '<blobstorage>\nblob-dir %s\n%s\n</blobstorage>' % (
                blob_dir, storage_conf)

    if port is None:
        raise AssertionError("The port wasn't specified")

    if zeo_conf is None or isinstance(zeo_conf, dict):
        z = ZEOConfig(('localhost', port))
        if zeo_conf:
            z.__dict__.update(zeo_conf)
        zeo_conf = z

    # Store the config info in a temp file.
    tmpfile = tempfile.mktemp(".conf", dir=os.getcwd())
    fp = open(tmpfile, 'w')
    zeo_conf.dump(fp)
    fp.write(storage_conf)
    fp.close()

    # Find the zeoserver script
    import ZEO.tests.zeoserver
    script = ZEO.tests.zeoserver.__file__
    if script.endswith('.pyc'):
        script = script[:-1]

    # Create a list of arguments, which we'll tuplify below
    qa = _quote_arg
    args = [qa(sys.executable), qa(script), '-C', qa(tmpfile)]
    if keep:
        args.append("-k")
    if not suicide:
        args.append("-S")
    if protocol:
        args.extend(["-v", protocol])

    d = os.environ.copy()
    d['PYTHONPATH'] = os.pathsep.join(sys.path)

    if sys.platform.startswith('win'):
        pid = os.spawnve(os.P_NOWAIT, sys.executable, tuple(args), d)
    else:
        pid = subprocess.Popen(args, env=d, close_fds=True).pid

    adminaddr = ('localhost', port + 1)
    # We need to wait until the server starts, but not forever.
    # 30 seconds is a somewhat arbitrary upper bound.  A BDBStorage
    # takes a long time to open -- more than 10 seconds on occasion.
    for i in range(120):
        time.sleep(0.25)
        try:
            logger.debug('connect %s', i)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(adminaddr)
            ack = s.recv(1024)
            s.close()
            logging.debug('acked: %s' % ack)
            break
        except socket.error, e:
            if e[0] not in (errno.ECONNREFUSED, errno.ECONNRESET):
                raise
            s.close()
    else:
        logging.debug('boo hoo')
        raise
    return ('localhost', port), adminaddr, pid, tmpfile


if sys.platform[:3].lower() == "win":
    def _quote_arg(s):
        return '"%s"' % s
else:
    def _quote_arg(s):
        return s


def shutdown_zeo_server(adminaddr):
    # Do this in a loop to guard against the possibility that the
    # client failed to connect to the adminaddr earlier.  That really
    # only requires two iterations, but do a third for pure
    # superstition.
    for i in range(3):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(.3)
        try:
            s.connect(adminaddr)
        except socket.timeout:
            # On FreeBSD 5.3 the connection just timed out
            if i > 0:
                break
            raise
        except socket.error, e:
            if (e[0] == errno.ECONNREFUSED
                or
                # MAC OS X uses EINVAL when connecting to a port
                # that isn't being listened on.
                (sys.platform == 'darwin' and e[0] == errno.EINVAL)
                ) and i > 0:
                break
            raise
        try:
            ack = s.recv(1024)
        except socket.error, e:
            ack = 'no ack received'
        logger.debug('shutdown_zeo_server(): acked: %s' % ack)
        s.close()

def get_port(test=None):
    """Return a port that is not in use.

    Checks if a port is in use by trying to connect to it.  Assumes it
    is not in use if connect raises an exception. We actually look for
    2 consective free ports because most of the clients of this
    function will use the returned port and the next one.

    Raises RuntimeError after 10 tries.
    """

    if test is not None:
        return get_port2(test)

    for i in range(10):
        port = random.randrange(20000, 30000)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            try:
                s.connect(('localhost', port))
            except socket.error:
                pass  # Perhaps we should check value of error too.
            else:
                continue

            try:
                s1.connect(('localhost', port+1))
            except socket.error:
                pass  # Perhaps we should check value of error too.
            else:
                continue

            return port

        finally:
            s.close()
            s1.close()
    raise RuntimeError("Can't find port")

def get_port2(test):
    for i in range(10):
        while 1:
            port = random.randrange(20000, 30000)
            if port%3 == 0:
                break

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(('localhost', port+2))
        except socket.error, e:
            if e[0] != errno.EADDRINUSE:
                raise
            continue

        if not (can_connect(port) or can_connect(port+1)):
            zope.testing.setupstack.register(test, s.close)
            return port

        s.close()

    raise RuntimeError("Can't find port")

def can_connect(port):
    c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        c.connect(('localhost', port))
    except socket.error:
        return False  # Perhaps we should check value of error too.
    else:
        c.close()
        return True

def setUp(test):
    ZODB.tests.util.setUp(test)

    servers = {}

    def start_server(storage_conf=None, zeo_conf=None, port=None, keep=False,
                     addr=None, path='Data.fs', protocol=None, blob_dir=None):
        """Start a ZEO server.

        Return the server and admin addresses.
        """
        if port is None:
            if addr is None:
                port = get_port2(test)
            else:
                port = addr[1]
        elif addr is not None:
            raise TypeError("Can't specify port and addr")
        addr, adminaddr, pid, config_path = start_zeo_server(
            storage_conf, zeo_conf, port, keep, path, protocol, blob_dir)
        os.remove(config_path)
        servers[adminaddr] = pid
        return addr, adminaddr

    test.globs['start_server'] = start_server

    def get_port():
        return get_port2(test)

    test.globs['get_port'] = get_port

    def stop_server(adminaddr):
        pid = servers.pop(adminaddr)
        shutdown_zeo_server(adminaddr)
        os.waitpid(pid, 0)

    test.globs['stop_server'] = stop_server

    def cleanup_servers():
        for adminaddr in list(servers):
            stop_server(adminaddr)

    zope.testing.setupstack.register(test, cleanup_servers)

    test.globs['wait_connected'] = wait_connected
    test.globs['wait_disconnected'] = wait_disconnected


def wait_until(label, func, timeout=30, onfail=None):
    giveup = time.time() + timeout
    while not func():
        if time.time() > giveup:
            if onfail is None:
                raise AssertionError("Timed out waiting for: ", label)
            else:
                return onfail()
        time.sleep(0.01)

def wait_connected(storage):
    wait_until("storage is connected", storage.is_connected)

def wait_disconnected(storage):
    wait_until("storage is disconnected",
               lambda : not storage.is_connected())
