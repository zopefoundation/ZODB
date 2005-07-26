##############################################################################
#
# Copyright (c) 2001, 2002, 2003 Zope Corporation and Contributors.
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
"""Persistence and ExtensionClass combined

$Id$
"""

from persistent import PickleCache

try:
    from _Persistence import Persistent
except:
    from warnings import warn
    warn("""Couldn't import the ExtensionClass-based base class

    There are two possibilities:

    1. You don't care about ExtensionClass. You are importing
       Persistence because that's what you imported in the past.
       In this case, you should really use the persistent package
       instead:

          >>> from persistent import Persistent
          >>> from persistent.list import PersistentList
          >>> from persistent.mapping import PersistentMapping

    2. You want your classes to be ExtensionClasses. In this case,
       you need to install the ExtensionClass package
       separately. ExtensionClass is no-longer included with ZODB3.

    """)

    from persistent import Persistent

Overridable = Persistent

from Persistence.mapping import PersistentMapping

# This is a travesty. Whimper. The Data.fs.in used in Zope 2 have
# ancient pickles refering to BoboPOS. Waaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa!
import sys
sys.modules['BoboPOS'] = sys.modules['Persistence']
sys.modules['BoboPOS.PersistentMapping'] = sys.modules['Persistence.mapping']
del sys
