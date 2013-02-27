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
    import cPickle as pickle
    IMPORT_MAPPING = {}
    NAME_MAPPING = {}
except ImportError:
    # Python 3.x: can't use stdlib's pickle because
    # http://bugs.python.org/issue6784
    ## import zodbpickle as pickle
    import pickle
    from _compat_pickle import IMPORT_MAPPING, NAME_MAPPING


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

