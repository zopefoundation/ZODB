##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Conventience function for creating test databases

$Id: util.py,v 1.5 2004/04/19 21:19:07 tim_one Exp $
"""

import time
import persistent
import transaction
from ZODB.MappingStorage import MappingStorage
from ZODB.DB import DB as _DB
try:
    from transaction import get_transaction
except ImportError:
    pass # else assume ZODB will install it as a builtin

def DB(name='Test'):
    return _DB(MappingStorage(name))

def commit():
    transaction.commit()

def pack(db):
    db.pack(time.time()+1)

class P(persistent.Persistent):

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return 'P(%s)' % self.name
