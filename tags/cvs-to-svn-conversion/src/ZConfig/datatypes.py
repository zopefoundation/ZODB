##############################################################################
#
# Copyright (c) 2002, 2003 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Selection of standard datatypes for ZConfig."""

import os
import re
import sys
import datetime

# types.StringTypes was added in Python 2.2; basestring in 2.3
try:
    unicode
except NameError:
    have_unicode = False
else:
    have_unicode = True


class MemoizedConversion:
    """Conversion helper that caches the results of expensive conversions."""

    def __init__(self, conversion):
        self._memo = {}
        self._conversion = conversion

    def __call__(self, value):
        try:
            return self._memo[value]
        except KeyError:
            v = self._conversion(value)
            self._memo[value] = v
            return v


class RangeCheckedConversion:
    """Conversion helper that range checks another conversion."""

    def __init__(self, conversion, min=None, max=None):
        self._min = min
        self._max = max
        self._conversion = conversion

    def __call__(self, value):
        v = self._conversion(value)
        if self._min is not None and v < self._min:
            raise ValueError("%s is below lower bound (%s)"
                             % (`v`, `self._min`))
        if self._max is not None and v > self._max:
            raise ValueError("%s is above upper bound (%s)"
                             % (`v`, `self._max`))
        return v


class RegularExpressionConversion:
    reason = "value did not match regular expression"

    def __init__(self, regex):
        self._rx = re.compile(regex)

    def __call__(self, value):
        m = self._rx.match(value)
        if m and m.group() == value:
            return value
        else:
            raise ValueError("%s: %s" % (self.reason, repr(value)))


def check_locale(value):
    import locale
    prev = locale.setlocale(locale.LC_ALL)
    try:
        try:
            locale.setlocale(locale.LC_ALL, value)
        finally:
            locale.setlocale(locale.LC_ALL, prev)
    except locale.Error:
        raise ValueError(
            'The specified locale "%s" is not supported by your system.\n'
            'See your operating system documentation for more\n'
            'information on locale support.' % value)
    else:
        return value


class BasicKeyConversion(RegularExpressionConversion):
    def __init__(self):
        RegularExpressionConversion.__init__(self, "[a-zA-Z][-._a-zA-Z0-9]*")

    def __call__(self, value):
        value = str(value)
        return RegularExpressionConversion.__call__(self, value).lower()


class ASCIIConversion(RegularExpressionConversion):
    def __call__(self, value):
        value = RegularExpressionConversion.__call__(self, value)
        if have_unicode and isinstance(value, unicode):
            value = value.encode("ascii")
        return value


_ident_re = "[_a-zA-Z][_a-zA-Z0-9]*"

class IdentifierConversion(ASCIIConversion):
    reason = "not a valid Python identifier"

    def __init__(self):
        ASCIIConversion.__init__(self, _ident_re)


class DottedNameConversion(ASCIIConversion):
    reason = "not a valid dotted name"

    def __init__(self):
        ASCIIConversion.__init__(self,
                                 r"%s(?:\.%s)*" % (_ident_re, _ident_re))


class DottedNameSuffixConversion(ASCIIConversion):
    reason = "not a valid dotted name or suffix"

    def __init__(self):
        ASCIIConversion.__init__(self,
                                 r"(?:%s)(?:\.%s)*|(?:\.%s)+"
                                 % (_ident_re, _ident_re, _ident_re))


def integer(value):
    try:
        return int(value)
    except ValueError:
        return long(value)
    except OverflowError:
        return long(value)


def null_conversion(value):
    return value


def asBoolean(s):
    """Convert a string value to a boolean value."""
    ss = str(s).lower()
    if ss in ('yes', 'true', 'on'):
        return True
    elif ss in ('no', 'false', 'off'):
        return False
    else:
        raise ValueError("not a valid boolean value: " + repr(s))


def string_list(s):
    """Convert a string to a list of strings using .split()."""
    return s.split()


port_number = RangeCheckedConversion(integer, min=1, max=0xffff).__call__


if sys.platform[:3] == "win":
    DEFAULT_HOST = "localhost"
else:
    DEFAULT_HOST = ""

def inet_address(s):
    # returns (host, port) tuple
    host = ''
    port = None
    if ":" in s:
        host, s = s.split(":", 1)
        if s:
            port = port_number(s)
        host = host.lower()
    else:
        try:
            port = port_number(s)
        except ValueError:
            if len(s.split()) != 1:
                raise ValueError("not a valid host name: " + repr(s))
            host = s.lower()
    if not host:
        host = DEFAULT_HOST
    return host, port


class SocketAddress:
    def __init__(self, s):
        # returns (family, address) tuple
        import socket
        if "/" in s or s.find(os.sep) >= 0:
            self.family = getattr(socket, "AF_UNIX", None)
            self.address = s
        else:
            self.family = socket.AF_INET
            self.address = inet_address(s)

def float_conversion(v):
    if isinstance(v, basestring):
        if v.lower() in ["inf", "-inf", "nan"]:
            raise ValueError(`v` + " is not a portable float representation")
    return float(v)

class IpaddrOrHostname(RegularExpressionConversion):
    def __init__(self):
        # IP address regex from the Perl Cookbook, Recipe 6.23 (revised ed.)
        # We allow underscores in hostnames although this is considered
        # illegal according to RFC1034.
        expr = (r"(^(\d|[01]?\d\d|2[0-4]\d|25[0-5])\." #ipaddr
                r"(\d|[01]?\d\d|2[0-4]\d|25[0-5])\." #ipaddr cont'd
                r"(\d|[01]?\d\d|2[0-4]\d|25[0-5])\." #ipaddr cont'd
                r"(\d|[01]?\d\d|2[0-4]\d|25[0-5])$)" #ipaddr cont'd
                r"|([A-Za-z_][-A-Za-z0-9_.]*[-A-Za-z0-9_])") # or hostname
        RegularExpressionConversion.__init__(self, expr)

    def __call__(self, value):
        return RegularExpressionConversion.__call__(self, value).lower()

def existing_directory(v):
    nv = os.path.expanduser(v)
    if os.path.isdir(nv):
        return nv
    raise ValueError, '%s is not an existing directory' % v

def existing_path(v):
    nv = os.path.expanduser(v)
    if os.path.exists(nv):
        return nv
    raise ValueError, '%s is not an existing path' % v

def existing_file(v):
    nv = os.path.expanduser(v)
    if os.path.exists(nv):
        return nv
    raise ValueError, '%s is not an existing file' % v

def existing_dirpath(v):
    nv = os.path.expanduser(v)
    dir = os.path.dirname(nv)
    if not dir:
        # relative pathname with no directory component
        return nv
    if os.path.isdir(dir):
        return nv
    raise ValueError, ('The directory named as part of the path %s '
                       'does not exist.' % v)


class SuffixMultiplier:
    # d is a dictionary of suffixes to integer multipliers.  If no suffixes
    # match, default is the multiplier.  Matches are case insensitive.  Return
    # values are in the fundamental unit.
    def __init__(self, d, default=1):
        self._d = d
        self._default = default
        # all keys must be the same size
        self._keysz = None
        for k in d.keys():
            if self._keysz is None:
                self._keysz = len(k)
            else:
                assert self._keysz == len(k)

    def __call__(self, v):
        v = v.lower()
        for s, m in self._d.items():
            if v[-self._keysz:] == s:
                return int(v[:-self._keysz]) * m
        return int(v) * self._default


def timedelta(s):
    # Unlike the standard time-interval data type, which returns a float
    # number of seconds, this datatype takes a wider range of syntax and
    # returns a datetime.timedelta
    #
    # Accepts suffixes:
    #    w - weeks
    #    d - days
    #    h - hours
    #    m - minutes
    #    s - seconds
    #
    # and all arguments may be integers or floats, positive or negative.
    # More than one time interval suffix value may appear on the line, but
    # they should all be separated by spaces, e.g.:
    #
    # sleep_time 4w 2d 7h 12m 0.00001s
    weeks = days = hours = minutes = seconds = 0
    for part in s.split():
        val = float(part[:-1])
        suffix = part[-1]
        if suffix == 'w':
            weeks = val
        elif suffix == 'd':
            days = val
        elif suffix == 'h':
            hours = val
        elif suffix == 'm':
            minutes = val
        elif suffix == 's':
            seconds = val
        else:
            raise TypeError('bad part %s in %s' % (part, s))
    return datetime.timedelta(weeks=weeks, days=days, hours=hours,
                              minutes=minutes, seconds=seconds)


stock_datatypes = {
    "boolean":           asBoolean,
    "dotted-name":       DottedNameConversion(),
    "dotted-suffix":     DottedNameSuffixConversion(),
    "identifier":        IdentifierConversion(),
    "integer":           integer,
    "float":             float_conversion,
    "string":            str,
    "string-list":       string_list,
    "null":              null_conversion,
    "locale":            MemoizedConversion(check_locale),
    "port-number":       port_number,
    "basic-key":         BasicKeyConversion(),
    "inet-address":      inet_address,
    "socket-address":    SocketAddress,
    "ipaddr-or-hostname":IpaddrOrHostname(),
    "existing-directory":existing_directory,
    "existing-path":     existing_path,
    "existing-file":     existing_file,
    "existing-dirpath":  existing_dirpath,
    "byte-size":         SuffixMultiplier({'kb': 1024,
                                           'mb': 1024*1024,
                                           'gb': 1024*1024*1024L,
                                           }),
    "time-interval":     SuffixMultiplier({'s': 1,
                                           'm': 60,
                                           'h': 60*60,
                                           'd': 60*60*24,
                                           }),
    "timedelta":         timedelta,
    }


class Registry:
    def __init__(self, stock=None):
        if stock is None:
            stock = stock_datatypes.copy()
        self._stock = stock
        self._other = {}
        self._basic_key = None

    def get(self, name):
        if '.' not in name:
            if self._basic_key is None:
                self._basic_key = self._other.get("basic-key")
                if self._basic_key is None:
                    self._basic_key = self._stock.get("basic-key")
                if self._basic_key is None:
                    self._basic_key = stock_datatypes["basic-key"]
            name = self._basic_key(name)
        t = self._stock.get(name)
        if t is None:
            t = self._other.get(name)
            if t is None:
                t = self.search(name)
        return t

    def register(self, name, conversion):
        if self._stock.has_key(name):
            raise ValueError("datatype name conflicts with built-in type: "
                             + `name`)
        if self._other.has_key(name):
            raise ValueError("datatype name already registered: " + `name`)
        self._other[name] = conversion

    def search(self, name):
        if not "." in name:
            raise ValueError("unloadable datatype name: " + `name`)
        components = name.split('.')
        start = components[0]
        g = {}
        package = __import__(start, g, g)
        modulenames = [start]
        for component in components[1:]:
            modulenames.append(component)
            try:
                package = getattr(package, component)
            except AttributeError:
                n = '.'.join(modulenames)
                package = __import__(n, g, g, component)
        self._other[name] = package
        return package
