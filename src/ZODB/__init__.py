##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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

# The next line must use double quotes, so release.py recognizes it.
__version__ = "3.4.1a2"

import sys
import __builtin__

from persistent import TimeStamp
from persistent import list
from persistent import mapping

# Backward compat for old imports.
sys.modules['ZODB.TimeStamp'] = sys.modules['persistent.TimeStamp']
sys.modules['ZODB.PersistentMapping'] = sys.modules['persistent.mapping']
sys.modules['ZODB.PersistentList'] = sys.modules['persistent.list']

del mapping, list, sys

from DB import DB

# TODO:  get_transaction() scheduled to go away in ZODB 3.6.
from transaction import get_transaction
__builtin__.get_transaction = get_transaction

del __builtin__
