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
"""Provide access to Persistent and PersistentMapping.

$Id$
"""
try:
    from cPersistence import Persistent
    from cPersistence import GHOST
    from cPersistence import UPTODATE
    from cPersistence import CHANGED
    from cPersistence import STICKY
    from cPersistence import simple_new
except ImportError: # XXX need pure-Python fallback
    _HAVE_CPERSISTECE = False
    from pyPersistence import Persistent
    from pyPersistence import GHOST
    from pyPersistence import UPTODATE
    from pyPersistence import CHANGED
    from pyPersistence import STICKY
else:
    _HAVE_CPERSISTECE = True
    import copy_reg
    copy_reg.constructor(simple_new)

try:
    from cPickleCache import PickleCache
except ImportError:
    from picklecache import PickleCache

try:
    import TimeStamp
except ImportError:
    import timestamp as TimeStamp
    import sys
    sys.modules['persistent.TimeStamp'] = sys.modules['persistent.timestamp']

if _HAVE_CPERSISTECE:
    # Make an interface declaration for Persistent, if zope.interface
    # is available.  XXX that the pyPersistent version already does this?
    try:
        from zope.interface import classImplements
    except ImportError:
        pass
    else:
        from persistent.interfaces import IPersistent
        classImplements(Persistent, IPersistent)
