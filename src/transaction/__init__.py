############################################################################
#
# Copyright (c) 2001, 2002, 2004 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
############################################################################

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

# XXX Issue deprecation warning if this variant is used?
get_transaction = get
