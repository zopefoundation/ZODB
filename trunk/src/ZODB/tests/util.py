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

$Id: util.py,v 1.2 2004/02/19 02:59:10 jeremy Exp $
"""

import time
import persistent
from ZODB.DemoStorage import DemoStorage
from ZODB.DB import DB as _DB
try:
    from transaction import get_transaction
except ImportError:
    pass # else assume ZODB will install it as a builtin

def DB(name='Test'):
    return _DB(DemoStorage(name))

def commit():
    get_transaction().commit()

def pack(db):
    db.pack(time.time()+1)

class P(persistent.Persistent):
    
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return 'P(%s)' % self.name
