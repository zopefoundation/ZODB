##############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
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
import sys

from zope.interface import implements

from persistent.interfaces import IPersistent
from persistent.interfaces import IPersistentDataManager

if sys.version_info < (2.6,):
    OID_TYPE = SERIAL_TYPE = str
else:
    OID_TYPE = SERIAL_TYPE = bytes

_CHANGED = 0x0001
_STICKY = 0x0002

class Persistent(object):
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
                if self.__flags & _STICKY:
                    raise ValueError('Sticky')
                if not self.__flags & _CHANGED:
                    self._p_invalidate()
            else:
                self._set_changed_flag(value)

    def _del_changed(self):
        self._set_changed(None)

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

    # The '_p_state' property is not (yet) part of the API:  for now,
    # it exists to simplify debugging and testing assertions.
    def _get_state(self):
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

    _p_state = property(_get_state)

    def __getstate__(self):
        """ See IPersistent.
        """
        return {}

    def __setstate__(self, state):
        """ See IPersistent.
        """
        if state != {}:
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

    def _p_invalidate(self):
        """ See IPersistent.
        """
        # XXX check
        self.__flags = None

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
