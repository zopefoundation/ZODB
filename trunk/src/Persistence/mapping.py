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

"""Python implementation of persistent base types

$Id: mapping.py,v 1.2 2003/12/29 22:40:45 tim_one Exp $"""

__version__='$Revision: 1.2 $'[11:-2]

import Persistence
import persistent
from persistent.mapping import PersistentMapping

if Persistence.Persistent is not persistent.Persistent:
    class PersistentMapping(Persistence.Persistent, PersistentMapping):
        """Legacy persistent mapping class

        This class mixes in ExtensionClass Base if it is present.

        Unless you actually want ExtensionClass semantics, use
        persistent.mapping.PersistentMapping instead.
        """

        def __setstate__(self, state):
            if 'data' not in state:
                state['data'] = state['_container']
                del state['_container']
            self.__dict__.update(state)
