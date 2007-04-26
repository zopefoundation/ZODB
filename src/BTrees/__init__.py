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

import BTrees.family64
import BTrees.family32

BTrees.family64.IO.family = BTrees.family64
BTrees.family64.OI.family = BTrees.family64
BTrees.family64.IF.family = BTrees.family64
BTrees.family64.II.family = BTrees.family64

BTrees.family32.IO.family = BTrees.family32
BTrees.family32.OI.family = BTrees.family32
BTrees.family32.IF.family = BTrees.family32
BTrees.family32.II.family = BTrees.family32
