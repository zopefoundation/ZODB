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

def get():
    return manager.get()

def begin():
    return manager.begin()

def commit(sub=False):
    manager.get().commit(sub)

def abort(sub=False):
    manager.get().abort(sub)

def get_transaction():
    from ZODB.utils import deprecated36
    deprecated36("""   use transaction.get() instead of get_transaction().
   transaction.commit() is a shortcut spelling of transaction.get().commit(),
   and transaction.abort() of transaction.get().abort().""")
    return get()
