##############################################################################
#
# Copyright (c) 2011 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
__all__ = ('TimeStamp',)

import datetime
import math
import struct
import sys


if sys.version_info < (2, 6):
    _RAWTYPE = str
else:
    _RAWTYPE = bytes

def _makeOctets(s):
    if sys.version_info < (2, 6,):
        return str(s)
    if sys.version_info < (3,):
        return bytes(s)
    return bytes(s, 'ascii')

_ZERO = _makeOctets('\x00' * 8)


class _UTC(datetime.tzinfo):
    def tzname(self):
        return 'UTC'
    def utcoffset(self, when):
        return datetime.timedelta(0, 0, 0)
    def dst(self):
        return 0
    def fromutc(self, dt):
        return dt

def _makeUTC(y, mo, d, h, mi, s):
    usec, sec = math.modf(s)
    sec = int(sec)
    usec = int(usec * 1e6)
    return datetime.datetime(y, mo, d, h, mi, sec, usec, tzinfo=_UTC())

_EPOCH = _makeUTC(1970, 1, 1, 0, 0, 0)

_SCONV = 60.0 / (1<<16) / (1<<16)

def _makeRaw(year, month, day, hour, minute, second):
    a = (((year - 1900) * 12 + month - 1) * 31 + day - 1)
    a = (a * 24 + hour) * 60 + minute
    b = int(second / _SCONV)
    return struct.pack('>II', a, b)

def _parseRaw(octets):
    a, b = struct.unpack('>II', octets)
    minute = a % 60
    hour = a // 60 % 24
    day = a // (60 * 24) % 31 + 1
    month = a // (60 * 24 * 31) % 12 + 1
    year = a // (60 * 24 * 31 * 12) + 1900
    second = b * _SCONV
    return (year, month, day, hour, minute, second)


class TimeStamp(object):
    __slots__ = ('_raw', '_elements')

    def __init__(self, *args):
        if len(args) == 1:
            raw = args[0]
            if not isinstance(raw, _RAWTYPE):
                raise TypeError('Raw octets must be of type: %s' % _RAWTYPE)
            if len(raw) != 8:
                raise TypeError('Raw must be 8 octets')
            self._raw = raw
            self._elements = _parseRaw(raw)
        elif len(args) == 6:
            self._raw = _makeRaw(*args)
            self._elements = args
        else:
            raise TypeError('Pass either a single 8-octet arg '
                            'or 5 integers and a float')

    def raw(self):
        return self._raw

    def year(self):
        return self._elements[0]

    def month(self):
        return self._elements[1]

    def day(self):
        return self._elements[2]

    def hour(self):
        return self._elements[3]

    def minute(self):
        return self._elements[4]

    def second(self):
        return self._elements[5]

    def timeTime(self):
        """ -> seconds since epoch, as a float.
        """
        delta = _makeUTC(*self._elements) - _EPOCH
        return delta.days * 86400.0 + delta.seconds

    def laterThan(self, other):
        """ Return a timestamp instance which is later than 'other'.

        If self already qualifies, return self.

        Otherwise, return a new instance one moment later than 'other'.
        """
        if not isinstance(other, self.__class__):
            raise ValueError()
        if self._raw > other._raw:
            return self
        a, b = struct.unpack('>II', other._raw)
        later = struct.pack('>II', a, b + 1)
        return self.__class__(later)

try:
    from persistent.TimeStamp import TimeStamp
except ImportError:
    pass
