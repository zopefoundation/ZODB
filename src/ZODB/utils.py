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
import time
import warnings
from binascii import hexlify, unhexlify
from struct import pack, unpack
from tempfile import mkstemp

from persistent.TimeStamp import TimeStamp

from ZODB._compat import Unpickler
from ZODB._compat import BytesIO
from ZODB._compat import ascii_bytes


__all__ = ['z64',
           'p64',
           'u64',
           'U64',
           'cp',
           'newTid',
           'oid_repr',
           'serial_repr',
           'tid_repr',
           'positive_id',
           'readable_tid_repr',
           'DEPRECATED_ARGUMENT',
           'deprecated37',
           'deprecated38',
           'get_pickle_metadata',
           'locked',
          ]

# A unique marker to give as the default value for a deprecated argument.
# The method should then do a
#
#     if that_arg is not DEPRECATED_ARGUMENT:
#         complain
#
# dance.
DEPRECATED_ARGUMENT = object()

# Raise DeprecationWarning, noting that the deprecated thing will go
# away in ZODB 3.7.  Point to the caller of our caller (i.e., at the
# code using the deprecated thing).
def deprecated37(msg):
    warnings.warn("This will be removed in ZODB 3.7:\n%s" % msg,
                  DeprecationWarning, stacklevel=3)

# Raise DeprecationWarning, noting that the deprecated thing will go
# away in ZODB 3.8.  Point to the caller of our caller (i.e., at the
# code using the deprecated thing).
def deprecated38(msg):
    warnings.warn("This will be removed in ZODB 3.8:\n%s" % msg,
                  DeprecationWarning, stacklevel=3)


if sys.version_info[0] < 3:
    def as_bytes(obj):
        "Convert obj into bytes"
        return str(obj)

    def as_text(bytes):
        "Convert bytes into string"
        return bytes

    # Convert an element of a bytes object into an int
    byte_ord = ord
    byte_chr = chr

else:
    def as_bytes(obj):
        if isinstance(obj, bytes):
            # invoking str on a bytes object gives its repr()
            return obj
        return str(obj).encode("ascii")

    def as_text(bytes):
        return bytes.decode("ascii")

    def byte_ord(byte):
        return byte # elements of bytes are already ints

    def byte_chr(int):
        return bytes((int,))

z64 = b'\0' * 8

assert sys.hexversion >= 0x02030000

# The distinction between ints and longs is blurred in Python 2.2,
# so u64() are U64() really the same.

def p64(v):
    """Pack an integer or long into a 8-byte string"""
    return pack(">Q", v)

def u64(v):
    """Unpack an 8-byte string into a 64-bit long integer."""
    return unpack(">Q", v)[0]

U64 = u64


def cp(f1, f2, length=None):
    """Copy all data from one file to another.

    It copies the data from the current position of the input file (f1)
    appending it to the current position of the output file (f2).

    It copies at most 'length' bytes. If 'length' isn't given, it copies
    until the end of the input file.
    """
    read = f1.read
    write = f2.write
    n = 8192

    if length is None:
        old_pos = f1.tell()
        f1.seek(0,2)
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
    ts = TimeStamp(*time.gmtime(t)[:5]+(t%60,))
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
    if isinstance(tid, str) and len(tid) == 8:
        result = "%s %s" % (result, TimeStamp(tid))
    return result

# Addresses can "look negative" on some boxes, some of the time.  If you
# feed a "negative address" to an %x format, Python 2.3 displays it as
# unsigned, but produces a FutureWarning, because Python 2.4 will display
# it as signed.  So when you want to prodce an address, use positive_id() to
# obtain it.
# _ADDRESS_MASK is 2**(number_of_bits_in_a_native_pointer).  Adding this to
# a negative address gives a positive int with the same hex representation as
# the significant bits in the original.

_ADDRESS_MASK = 256 ** struct.calcsize('P')
def positive_id(obj):
    """Return id(obj) as a non-negative integer."""

    result = id(obj)
    if result < 0:
        result += _ADDRESS_MASK
        assert result > 0
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
    if data[0] in (0x80,    # Py3k indexes bytes -> int
                   b'\x80'  # Python2 indexes bytes -> bytes
                  ): # protocol marker, protocol > 1
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
    except Exception as err:
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

class Locked(object):

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

        inst._lock_acquire()
        try:
            for precondition in self.preconditions:
                if not precondition(inst):
                    raise AssertionError(
                        "Failed precondition: ",
                        precondition.__doc__.strip())

            return func(*args, **kw)
        finally:
            inst._lock_release()

class locked(object):

    def __init__(self, *preconditions):
        self.preconditions = preconditions

    def __get__(self, inst, class_):
        # We didn't get any preconditions, so we have a single "precondition",
        # which is actually the function to call.
        func, = self.preconditions
        return Locked(func, inst, class_)

    def __call__(self, func):
        return Locked(func, preconditions=self.preconditions)

