#############################################################################
#
# Copyright (c) 2007 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
#############################################################################

import zope.interface
import BTrees.Interfaces

from BTrees import IOBTree as IO
from BTrees import OIBTree as OI
from BTrees import IFBTree as IF
from BTrees import IIBTree as II
from BTrees import OOBTree as OO

maxint = int(2**31-1)
minint = -maxint - 1

zope.interface.moduleProvides(BTrees.Interfaces.IBTreeFamily)
