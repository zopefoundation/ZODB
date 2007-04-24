############################################################################
#
# Copyright (c) 2001, 2002, 2004 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
############################################################################
"""Exported transaction functions.

$Id$
"""

from transaction._transaction import Transaction
from transaction._manager import TransactionManager, ThreadTransactionManager

manager = ThreadTransactionManager()
get = manager.get
begin = manager.begin
commit = manager.commit
abort = manager.abort
doom = manager.doom
isDoomed = manager.isDoomed
savepoint = manager.savepoint
