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

__version__ = '3.1b1+'

import sys
import cPersistence, Persistence
from zLOG import register_subsystem
register_subsystem('ZODB')

# This is lame. Don't look. :(
sys.modules['cPersistence']=cPersistence

Persistent=cPersistence.Persistent

# Install Persistent and PersistentMapping in Persistence
if not hasattr(Persistence, 'Persistent'):
    Persistence.Persistent=Persistent
    Persistent.__module__='Persistence'
    Persistence.Overridable=cPersistence.Overridable
    Persistence.Overridable.__module__='Persistence'
    if not hasattr(Persistence, 'PersistentMapping'):
        import PersistentMapping
        sys.modules['PersistentMapping']=PersistentMapping
        sys.modules['BoboPOS']=sys.modules['ZODB']
        sys.modules['BoboPOS.PersistentMapping']=PersistentMapping
        PersistentMapping=PersistentMapping.PersistentMapping
        from PersistentMapping import PersistentMapping
        Persistence.PersistentMapping=PersistentMapping
        PersistentMapping.__module__='Persistence'
        del PersistentMapping

del cPersistence

from DB import DB

import Transaction
