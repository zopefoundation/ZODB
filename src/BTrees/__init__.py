#############################################################################
#
# Copyright (c) 2007 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
#############################################################################

import zope.interface
import BTrees.Interfaces


@zope.interface.implementer(BTrees.Interfaces.IBTreeFamily)
class _Family(object):

    from BTrees import OOBTree as OO

class _Family32(_Family):
    from BTrees import OIBTree as OI
    from BTrees import IIBTree as II
    from BTrees import IOBTree as IO
    from BTrees import IFBTree as IF

    maxint = int(2**31-1)
    minint = -maxint - 1

    def __reduce__(self):
        return _family32, ()

class _Family64(_Family):
    from BTrees import OLBTree as OI
    from BTrees import LLBTree as II
    from BTrees import LOBTree as IO
    from BTrees import LFBTree as IF

    maxint = 2**63-1
    minint = -maxint - 1

    def __reduce__(self):
        return _family64, ()

def _family32():
    return family32
_family32.__safe_for_unpickling__ = True

def _family64():
    return family64
_family64.__safe_for_unpickling__ = True


family32 = _Family32()
family64 = _Family64()


BTrees.family64.IO.family = family64
BTrees.family64.OI.family = family64
BTrees.family64.IF.family = family64
BTrees.family64.II.family = family64

BTrees.family32.IO.family = family32
BTrees.family32.OI.family = family32
BTrees.family32.IF.family = family32
BTrees.family32.II.family = family32
