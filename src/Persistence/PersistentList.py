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

"""Python implementation of persistent list.

$Id: PersistentList.py,v 1.5 2003/11/28 16:44:46 jim Exp $"""

__version__='$Revision: 1.5 $'[11:-2]

import Persistence
import persistent
from persistent.list import PersistentList

if Persistence.Persistent is not persistent.Persistent:
    class PersistentList(Persistence.Persistent, PersistentList):
        """Legacy persistent list class
        
        This class mixes in ExtensionClass Base if it is present.
        
        Unless you actually want ExtensionClass semantics, use
        persistent.list.PersistentList instead.
        """
