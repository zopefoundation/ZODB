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

# The next line must use double quotes, so replace.py recognizes it.
__version__ = "3.3a3"

import sys
import __builtin__

from persistent import TimeStamp
from DB import DB
from transaction import get as get_transaction

# Backward compat for old imports. I don't think TimeStamp should
# really be in persistent anyway.
sys.modules['ZODB.TimeStamp'] = sys.modules['persistent.TimeStamp']

# XXX Issue deprecation warning if this variant is used?
__builtin__.get_transaction = get_transaction
del __builtin__
