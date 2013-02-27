##############################################################################
#
# Copyright (c) 2013 Zope Foundation and Contributors.
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

try:
    # Python 2.x
    from cPickle import Pickler, Unpickler, dump, dumps, loads
    IMPORT_MAPPING = {}
    NAME_MAPPING = {}
except ImportError:
    # Python 3.x: can't use stdlib's pickle because
    # http://bugs.python.org/issue6784
    from zodbpickle.pickle import Pickler, dump, dumps
    from zodbpickle.pickle import Unpickler as _Unpickler, loads as _loads
    from _compat_pickle import IMPORT_MAPPING, NAME_MAPPING

    class Unpickler(_Unpickler):
        def __init__(self, f):
            super(Unpickler, self).__init__(f, encoding='ASCII', errors='bytes')

        # Py3: Python 3 doesn't allow assignments to find_global,
        # instead, find_class can be overridden

        find_global = None

        def find_class(self, modulename, name):
            if self.find_global is None:
                return super(Unpickler, self).find_class(modulename, name)
            return self.find_global(modulename, name)

    def loads(s):
        return _loads(s, encoding='ASCII', errors='bytes')


# XXX: overridable Unpickler.find_global as used in serialize.py?
# XXX: consistent spelling of inst_persistent_id/persistent_id?
#      e.g. StorageTestBase and probably elsewhere


try:
    # Python 2.x
    # XXX: why not just import BytesIO from io?
    from cStringIO import StringIO as BytesIO
except ImportError:
    # Python 3.x
    from io import BytesIO


try:
    # Python 3.x
    from base64 import decodebytes, encodebytes
except ImportError:
    # Python 2.x
    from base64 import decodestring as decodebytes, encodestring as encodebytes


# Python 3.x: ``hasattr()`` swallows only AttributeError.
def py2_hasattr(obj, name):
    try:
        getattr(obj, name)
    except:
        return False
    return True


try:
    # Py2: simply reexport the builtin
    long = long
except NameError:
    # Py3
    long = int

