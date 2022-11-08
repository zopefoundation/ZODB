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
import sys

from six import PY3

from zodbpickle import binary  # noqa: F401 import unused


IS_JYTHON = sys.platform.startswith('java')

_protocol = 3

if not PY3:
    # Python 2.x
    # PyPy's cPickle doesn't have noload, and noload is broken in Python 2.7,
    # so we need zodbpickle.
    # Get the fastest working version we can (PyPy has no fastpickle)
    try:
        import zodbpickle.fastpickle as cPickle
    except ImportError:
        import zodbpickle.pickle as cPickle
    Pickler = cPickle.Pickler
    Unpickler = cPickle.Unpickler
    dump = cPickle.dump
    dumps = cPickle.dumps
    loads = cPickle.loads
    HIGHEST_PROTOCOL = cPickle.HIGHEST_PROTOCOL
    IMPORT_MAPPING = {}
    NAME_MAPPING = {}
    FILESTORAGE_MAGIC = b"FS21"
else:
    # Python 3.x: can't use stdlib's pickle because
    # http://bugs.python.org/issue6784
    import zodbpickle.pickle
    HIGHEST_PROTOCOL = 3
    from _compat_pickle import IMPORT_MAPPING  # noqa: F401 import unused
    from _compat_pickle import NAME_MAPPING  # noqa: F401 import unused

    class Pickler(zodbpickle.pickle.Pickler):
        def __init__(self, f, protocol=None):
            super(Pickler, self).__init__(f, protocol)

    class Unpickler(zodbpickle.pickle.Unpickler):
        def __init__(self, f):
            super(Unpickler, self).__init__(f)

        # Py3: Python 3 doesn't allow assignments to find_global,
        # instead, find_class can be overridden

        find_global = None

        def find_class(self, modulename, name):
            if self.find_global is None:
                return super(Unpickler, self).find_class(modulename, name)
            return self.find_global(modulename, name)

    def dump(o, f, protocol=None):
        return zodbpickle.pickle.dump(o, f, protocol)

    def dumps(o, protocol=None):
        return zodbpickle.pickle.dumps(o, protocol)

    def loads(s):
        return zodbpickle.pickle.loads(s, encoding='ASCII', errors='bytes')
    FILESTORAGE_MAGIC = b"FS30"


def PersistentPickler(persistent_id, *args, **kwargs):
    """
    Returns a :class:`Pickler` that will use the given ``persistent_id``
    to get persistent IDs. The remainder of the arguments are passed to the
    Pickler itself.

    This covers the differences between Python 2 and 3 and PyPy/zodbpickle.
    """
    p = Pickler(*args, **kwargs)
    if not PY3:
        p.inst_persistent_id = persistent_id

    # PyPy uses a python implementation of cPickle/zodbpickle in both Python 2
    # and Python 3. We can't really detect inst_persistent_id as its
    # a magic attribute that's not readable, but it doesn't hurt to
    # simply always assign to persistent_id also
    p.persistent_id = persistent_id
    return p


def PersistentUnpickler(find_global, load_persistent, *args, **kwargs):
    """
    Returns a :class:`Unpickler` that will use the given `find_global` function
    to locate classes, and the given `load_persistent` function to load
    objects from a persistent id.

    This covers the differences between Python 2 and 3 and PyPy/zodbpickle.
    """
    unpickler = Unpickler(*args, **kwargs)
    if find_global is not None:
        unpickler.find_global = find_global
        try:
            # PyPy, zodbpickle, the non-c-accelerated version
            unpickler.find_class = find_global
        except AttributeError:
            pass
    if load_persistent is not None:
        unpickler.persistent_load = load_persistent

    return unpickler


try:
    # XXX: why not just import BytesIO from io?
    from cStringIO import StringIO as BytesIO
except ImportError:
    # Python 3.x
    from io import BytesIO  # noqa: F401 import unused


try:
    # Python 3.x
    from base64 import decodebytes
    from base64 import encodebytes
except ImportError:
    # Python 2.x
    from base64 import decodestring as decodebytes
    from base64 import encodestring as encodebytes

# I want to use 'noqa: F401 import unused' on the lines above, for flake8,
# but isort removes them.  So mention the two imported functions here,
# so neither flake8 nor isort complains.
decodebytes, encodebytes


# Python 3.x: ``hasattr()`` swallows only AttributeError.
def py2_hasattr(obj, name):
    try:
        getattr(obj, name)
    except:  # noqa: E722 do not use bare 'except'
        return False
    return True


try:
    # Py2: simply reexport the builtin
    long = long
except NameError:
    # Py3
    long = int
    INT_TYPES = (int,)
else:
    INT_TYPES = (int, long)


try:
    TEXT = unicode
except NameError:  # pragma NO COVER Py3k
    TEXT = str


def ascii_bytes(x):
    if isinstance(x, TEXT):
        x = x.encode('ascii')
    return x
