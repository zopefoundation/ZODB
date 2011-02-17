##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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

import sys

from persistent import timestamp as TimeStamp
from persistent import list
from persistent import mapping

# Backward compat for old imports.
sys.modules['ZODB.TimeStamp'] = sys.modules['persistent.timestamp']
sys.modules['ZODB.PersistentMapping'] = sys.modules['persistent.mapping']
sys.modules['ZODB.PersistentList'] = sys.modules['persistent.list']

del mapping, list, sys

from DB import DB, connection
