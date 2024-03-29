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

import os
import struct
import sys
import threading
import time
from binascii import hexlify
from binascii import unhexlify
from io import BytesIO
from tempfile import mkstemp

from persistent.timestamp import TimeStamp

from ZODB._compat import Unpickler
from ZODB._compat import ascii_bytes


__all__ = ['z64',
           'p64',
           'u64',
           'U64',
           'cp',
           'maxtid',
           'newTid',
           'oid_repr',
           'serial_repr',
           'tid_repr',
           'readable_tid_repr',
           'get_pickle_metadata',
           'locked',
           ]


def as_bytes(obj):
    if isinstance(obj, bytes):
        # invoking str on a bytes object gives its repr()
        return obj
    return str(obj).encode("ascii")


def as_text(bytes):
    return bytes.decode("ascii")


def byte_ord(byte):
    return byte  # elements of bytes are already ints


def byte_chr(int):
    return bytes((int,))


z64 = b'\0' * 8
maxtid = b'\x7f\xff\xff\xff\xff\xff\xff\xff'

assert sys.hexversion >= 0x02030000

# The distinction between ints and longs is blurred,
# so u64() are U64() really the same.

_OID_STRUCT = struct.Struct('>Q')
_OID_PACK = _OID_STRUCT.pack
_OID_UNPACK = _OID_STRUCT.unpack


def p64(v):
    """Pack an integer or long into a 8-byte string."""
    try:
        return _OID_PACK(v)
    except struct.error as e:
        raise ValueError(*(e.args + (v,)))


def u64(v):
    """Unpack an 8-byte string into a 64-bit long integer."""
    try:
        return _OID_UNPACK(v)[0]
    except struct.error as e:
        raise ValueError(*(e.args + (v,)))


U64 = u64


def cp(f1, f2, length=None, bufsize=64 * 1024):
    """Copy all data from one file to another.

    It copies the data from the current position of the input file (f1)
    appending it to the current position of the output file (f2).

    It copies at most 'length' bytes. If 'length' isn't given, it copies
    until the end of the input file.
    """
    read = f1.read
    write = f2.write
    n = bufsize

    if length is None:
        old_pos = f1.tell()
        f1.seek(0, 2)
        length = f1.tell()
        f1.seek(old_pos)

    while length > 0:
        if n > length:
            n = length
        data = read(n)
        if not data:
            break
        write(data)
        length -= len(data)


def newTid(old):
    t = time.time()
    ts = TimeStamp(*time.gmtime(t)[:5]+(t % 60,))
    if old is not None:
        ts = ts.laterThan(TimeStamp(old))
    return ts.raw()


def oid_repr(oid):
    if isinstance(oid, bytes) and len(oid) == 8:
        # Convert to hex and strip leading zeroes.
        as_hex = hexlify(oid).lstrip(b'0')
        # Ensure two characters per input byte.
        if len(as_hex) & 1:
            as_hex = b'0' + as_hex
        elif as_hex == b'':
            as_hex = b'00'
        return '0x' + as_hex.decode()
    else:
        return repr(oid)


def repr_to_oid(repr):
    repr = ascii_bytes(repr)
    if repr.startswith(b"0x"):
        repr = repr[2:]
    as_bin = unhexlify(repr)
    as_bin = b"\x00"*(8-len(as_bin)) + as_bin
    return as_bin


serial_repr = oid_repr
tid_repr = serial_repr

# For example, produce
#     '0x03441422948b4399 2002-04-14 20:50:34.815000'
# for 8-byte string tid b'\x03D\x14"\x94\x8bC\x99'.


def readable_tid_repr(tid):
    result = tid_repr(tid)
    if isinstance(tid, bytes) and len(tid) == 8:
        result = "{} {}".format(result, TimeStamp(tid))
    return result

# Given a ZODB pickle, return pair of strings (module_name, class_name).
# Do this without importing the module or class object.
# See ZODB/serialize.py's module docstring for the only docs that exist about
# ZODB pickle format.  If the code here gets smarter, please update those
# docs to be at least as smart.  The code here doesn't appear to make sense
# for what serialize.py calls formats 5 and 6.


def get_pickle_metadata(data):
    # Returns a 2-tuple of strings.

    # ZODB's data records contain two pickles.  The first is the class
    # of the object, the second is the object.  We're only trying to
    # pick apart the first here, to extract the module and class names.
    if data[0] == 0x80:  # protocol marker, protocol > 1
        data = data[2:]
    if data.startswith(b'(c'):   # pickle MARK GLOBAL opcode sequence
        global_prefix = 2
    elif data.startswith(b'c'):  # pickle GLOBAL opcode
        global_prefix = 1
    else:
        global_prefix = 0

    if global_prefix:
        # Formats 1 and 2.
        # Don't actually unpickle a class, because it will attempt to
        # load the class.  Just break open the pickle and get the
        # module and class from it.  The module and class names are given by
        # newline-terminated strings following the GLOBAL opcode.
        modname, classname, rest = data.split(b'\n', 2)
        modname = modname[global_prefix:]   # strip GLOBAL opcode
        return modname.decode(), classname.decode()

    # Else there are a bunch of other possible formats.
    f = BytesIO(data)
    u = Unpickler(f)
    try:
        class_info = u.load()
    except Exception:
        return '', ''
    if isinstance(class_info, tuple):
        if isinstance(class_info[0], tuple):
            # Formats 3 and 4.
            modname, classname = class_info[0]
        else:
            # Formats 5 and 6 (probably) end up here.
            modname, classname = class_info
    else:
        # This isn't a known format.
        modname = repr(class_info)
        classname = ''
    return modname, classname


def mktemp(dir=None, prefix='tmp'):
    """Create a temp file, known by name, in a semi-secure manner."""
    handle, filename = mkstemp(dir=dir, prefix=prefix)
    os.close(handle)
    return filename


def check_precondition(precondition):
    if not precondition():
        raise AssertionError(
            "Failed precondition: ",
            precondition.__doc__.strip())


class Locked:

    def __init__(self, func, inst=None, class_=None, preconditions=()):
        self.__func__ = func
        self.__self__ = inst
        self.__self_class__ = class_
        self.preconditions = preconditions

    def __get__(self, inst, class_):
        return self.__class__(
            self.__func__, inst, class_, self.preconditions)

    def __call__(self, *args, **kw):
        inst = self.__self__
        if inst is None:
            inst = args[0]
        func = self.__func__.__get__(self.__self__, self.__self_class__)

        with inst._lock:
            for precondition in self.preconditions:
                if not precondition(inst):
                    raise AssertionError(
                        "Failed precondition: ",
                        precondition.__doc__.strip())

            return func(*args, **kw)


class locked:

    def __init__(self, *preconditions):
        self.preconditions = preconditions

    def __get__(self, inst, class_):
        # We didn't get any preconditions, so we have a single "precondition",
        # which is actually the function to call.
        func, = self.preconditions
        return Locked(func, inst, class_)

    def __call__(self, func):
        return Locked(func, preconditions=self.preconditions)


if os.environ.get('DEBUG_LOCKING'):  # pragma: no cover
    class Lock:

        lock_class = threading.Lock

        def __init__(self):
            self._lock = self.lock_class()

        def pr(self, name, a=None, kw=None):
            f = sys._getframe(2)
            if f.f_code.co_filename.endswith('ZODB/utils.py'):
                f = sys._getframe(3)
            f = '{}:{}'.format(f.f_code.co_filename, f.f_lineno)
            print(id(self), self._lock, threading.get_ident(), f, name,
                  a if a else '', kw if kw else '')

        def acquire(self, *a, **kw):
            self.pr('acquire', a, kw)
            return self._lock.acquire(*a, **kw)

        def release(self):
            self.pr('release')
            return self._lock.release()

        def __enter__(self):
            self.pr('acquire')
            return self._lock.acquire()

        def __exit__(self, *ignored):
            self.pr('release')
            return self._lock.release()

    class RLock(Lock):

        lock_class = threading.RLock

    class Condition(Lock):

        lock_class = threading.Condition

        def wait(self, *a, **kw):
            self.pr('wait', a, kw)
            return self._lock.wait(*a, **kw)

        def wait_for(self, *a, **kw):
            self.pr('wait_for', a, kw)
            return self._lock.wait_for(*a, **kw)

        def notify(self, *a, **kw):
            self.pr('notify', a, kw)
            return self._lock.notify(*a, **kw)

        def notify_all(self):
            self.pr('notify_all')
            return self._lock.notify_all()

        notifyAll = notify_all

else:

    from threading import Condition, Lock, RLock  # noqa: F401 import unused

import ZODB.POSException  # noqa: E402 module level import not at top of file


def load_current(storage, oid, version=''):
    """Load the most recent revision of an object by calling loadBefore

    Starting in ZODB 5, it's no longer necessary for storages to
    provide a load method.

    This function is mainly intended to facilitate transitioning from
    load to loadBefore.  It's mainly useful for tests that are meant
    to test storages, but do so by calling load on the storages.

    This function will likely become unnecessary and be deprecated
    some time in the future.
    """
    assert not version
    r = storage.loadBefore(oid, maxtid)
    if r is None:
        raise ZODB.POSException.POSKeyError(oid)
    assert r[2] is None
    return r[:2]


def at2before(at):  # -> before
    """at2before converts `at` TID to corresponding `before`."""
    return p64(u64(at) + 1)


def before2at(before):  # -> at
    """before2at converts `before` TID to corresponding `at`."""
    return p64(u64(before) - 1)
