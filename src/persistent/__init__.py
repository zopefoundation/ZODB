##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Provide access to Persistent and PersistentMapping.

$Id: __init__.py,v 1.8 2004/02/24 13:54:05 srichter Exp $
"""

from cPersistence import Persistent, GHOST, UPTODATE, CHANGED, STICKY
from cPickleCache import PickleCache

from cPersistence import simple_new
import copy_reg
copy_reg.constructor(simple_new)

# Make an interface declaration for Persistent,
# if zope.interface is available.
try:
    from zope.interface import classImplements
except ImportError:
    pass
else:
    from persistent.interfaces import IPersistent
    classImplements(Persistent, IPersistent)
