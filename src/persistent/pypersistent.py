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
import datetime
import struct
import sys
import time

from zope.interface import implements

from persistent.interfaces import IPersistent
from persistent.interfaces import IPersistentDataManager

if sys.version_info < (2.6,):
    OID_TYPE = SERIAL_TYPE = str
else:
    OID_TYPE = SERIAL_TYPE = bytes

# Bitwise flags
_CHANGED = 0x0001
_STICKY = 0x0002

# Allowed values for _p_state
GHOST = -1
UPTODATE = 0
CHANGED = 1
STICKY = 2

# These names can be used from a ghost without causing it to be activated.
SPECIAL_NAMES = ('__class__',
                 '__del__',
                 '__dict__',
                 '__of__',
                 '__setstate__'
                )

_SCONV = 60.0 / (1<<16) / (1<<16)

def makeTimestamp(year, month, day, hour, minute, second):
    a = (((year - 1900) * 12 + month - 1) * 31 + day - 1)
    a = (a * 24 + hour) * 60 + minute
    b = int(second / _SCONV)
    return struct.pack('>II', a, b)

def parseTimestamp(octets):
    a, b = struct.unpack('>II', octets)
    minute = a % 60
    hour = a // 60 % 24
    day = a // (60 * 24) % 31 + 1
    month = a // (60 * 24 * 31) % 12 + 1
    year = a // (60 * 24 * 31 * 12) + 1900
    second = b * _SCONV
    return (year, month, day, hour, minute, second)


class Persistent(object):
    """ Pure Python implmentation of Persistent base class
    """
    __slots__ = ('__jar', '__oid', '__serial', '__flags')
    implements(IPersistent)

    def __new__(cls):
        inst = super(Persistent, cls).__new__(cls)
        inst.__jar = inst.__oid =  inst.__serial = None
        inst.__flags = None
        return inst

    # _p_jar:  see IPersistent.
    def _get_jar(self):
        return self.__jar

    def _set_jar(self, value):
        if value is self.__jar:
            return
        if self.__jar is not None:
            raise ValueError('Already assigned a data manager')
        if not IPersistentDataManager.providedBy(value):
            raise ValueError('Not a data manager: %s' % value)
        self.__jar = value

    _p_jar = property(_get_jar, _set_jar)

    # _p_oid:  see IPersistent.
    def _get_oid(self):
        return self.__oid

    def _set_oid(self, value):
        if value == self.__oid:
            return
        if value is not None:
            if not isinstance(value, OID_TYPE):
                raise ValueError('Invalid OID type: %s' % value)
        if self.__oid is not None:
            raise ValueError('Already assigned an OID')
        self.__oid = value

    _p_oid = property(_get_oid, _set_oid)

    # _p_serial:  see IPersistent.
    def _get_serial(self):
        return self.__serial

    def _set_serial(self, value):
        if value is not None:
            if not isinstance(value, SERIAL_TYPE):
                raise ValueError('Invalid SERIAL type: %s' % value)
        self.__serial = value

    _p_serial = property(_get_serial, _set_serial)

    # _p_changed:  see IPersistent.
    def _get_changed(self):
        if self.__flags is None: # ghost
            return None
        return self.__flags & _CHANGED

    def _set_changed(self, value):
        if self.__flags is None:
            if value is not None:
                self._p_activate()
                self._set_changed_flag(value)
        else:
            if value is None: # -> ghost
                self._p_deactivate()
            else:
                self._set_changed_flag(value)

    def _del_changed(self):
        self._p_invalidate()

    _p_changed = property(_get_changed, _set_changed, _del_changed)

    # The '_p_sticky' property is not (yet) part of the API:  for now,
    # it exists to simplify debugging and testing assertions.
    def _get_sticky(self):
        if self.__flags is None:
            return False
        return self.__flags & _STICKY
    def _set_sticky(self, value):
        if self.__flags is None:
            raise ValueError('Ghost')
        if value:
            self.__flags |= _STICKY
        else:
            self.__flags &= ~_STICKY
    _p_sticky = property(_get_sticky, _set_sticky)

    # The '_p_status' property is not (yet) part of the API:  for now,
    # it exists to simplify debugging and testing assertions.
    def _get_status(self):
        if self.__flags is None:
            if self.__jar is None:
                return 'new'
            return 'ghost'
        if self.__flags & _CHANGED:
            if self.__jar is None:
                return 'unsaved'
            result = 'changed'
        else:
            result = 'saved'
        if self.__flags & _STICKY:
            return '%s (sticky)' % result
        return result

    _p_status = property(_get_status)

    # These attributes are defined by the C type, but not IPersistent.
    def _get_mtime(self):
        if self.__serial is not None:
            when = datetime.datetime(*parseTimestamp(self.__serial))
            return time.mktime(when.timetuple())
    _p_mtime = property(_get_mtime)

    # _p_state
    def _get_state(self):
        if self.__flags is None:
            if self.__jar is None:
                return UPTODATE
            return GHOST
        if self.__flags & _CHANGED:
            if self.__jar is None:
                return UPTODATE
            result = CHANGED
        else:
            result = UPTODATE
        if self.__flags & _STICKY:
            return STICKY
        return result

    _p_state = property(_get_state)

    # _p_estimated_size:  XXX don't want to reserve the space?
    def _get_estimated_size(self):
        return 0

    def _set_estimated_size(self, value):
        pass

    _p_estimated_size = property(_get_estimated_size, _set_estimated_size)

    # Methods from IPersistent.
    def __getstate__(self):
        """ See IPersistent.
        """
        return ()

    def __setstate__(self, state):
        """ See IPersistent.
        """
        if state != ():
            raise ValueError('No state allowed on base Persistent class')

    def _p_activate(self):
        """ See IPersistent.
        """
        if self.__flags is None:
            self.__flags = 0
        if self.__jar is not None and self.__oid is not None:
            self.__jar.setstate(self)

    def _p_deactivate(self):
        """ See IPersistent.
        """
        if self.__flags is not None and not self.__flags & _CHANGED:
            self._p_invalidate()

    def _p_invalidate(self):
        """ See IPersistent.
        """
        if self.__flags is not None and self.__flags & _STICKY:
            raise ValueError('Sticky')
        self.__flags = None

    # Methods defined in C, not part of IPersistent
    def _p_getattr(self, name):
        """\
        _p_getattr(name) -- Test whether the base class must handle the name
   
        The method unghostifies the object, if necessary.
        The method records the object access, if necessary.
 
        This method should be called by subclass __getattribute__
        implementations before doing anything else. If the method
        returns True, then __getattribute__ implementations must delegate
        to the base class, Persistent.
        """
        if name.startswith('_p_') or name in SPECIAL_NAMES:
            return True
        self._p_activate()
        # TODO set the object as acceessed with the jar's cache.
        return False

    def _p_setattr(self, name, value):
        """_p_setattr(name, value) -- Save persistent meta data

        This method should be called by subclass __setattr__ implementations
        before doing anything else.  If it returns true, then the attribute
        was handled by the base class.

        The method unghostifies the object, if necessary.
        The method records the object access, if necessary.
        """
        if name.startswith('_p_'):
            setattr(self, name, value)
            return True
        self._p_activate()
        # TODO set the object as acceessed with the jar's cache.
        return False

    def _p_delattr(self, name):
        """_p_delattr(name) -- Delete persistent meta data

        This method should be called by subclass __delattr__ implementations
        before doing anything else.  If it returns true, then the attribute
        was handled by the base class.

        The method unghostifies the object, if necessary.
        The method records the object access, if necessary.
        """
        if name.startswith('_p_'):
            delattr(self, name)
            return True
        self._p_activate()
        # TODO set the object as acceessed with the jar's cache.
        return False

    def __reduce__(self):
        """Reduce an object to contituent parts for serialization.
        """
        gna = getattr(self, '__getnewargs__', lambda: ())
        return ((type(self),) + gna(), self.__getstate__())

    # Helper methods:  not APIs
    def _register(self):
        if self.__jar is not None and self.__oid is not None:
            self.__jar.register(self)

    def _set_changed_flag(self, value):
        if value:
            before = self.__flags
            self.__flags |= _CHANGED
            if before != self.__flags:
                self._register()
        else:
            self.__flags &= ~_CHANGED
