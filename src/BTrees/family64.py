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

from BTrees import LOBTree as IO
from BTrees import OLBTree as OI
from BTrees import LFBTree as IF
from BTrees import LLBTree as II
from BTrees import OOBTree as OO

maxint = 2**63-1
minint = -maxint - 1

zope.interface.moduleProvides(BTrees.Interfaces.IBTreeFamily)
