##############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
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
"""Utility classes or functions

$Id: util.py,v 1.2 2004/02/20 16:56:56 fdrake Exp $
"""

from transaction.interfaces import IRollback

try:
    from zope.interface import implements
except ImportError:
    def implements(*args):
        pass

class NoSavepointSupportRollback:
    """Rollback for data managers that don't support savepoints

    >>> class DataManager:
    ...     def savepoint(self, txn):
    ...         return NoSavepointSupportRollback(self)
    >>> rb = DataManager().savepoint('some transaction')
    >>> rb.rollback()
    Traceback (most recent call last):
    ...
    NotImplementedError: """ \
           """DataManager data managers do not support """ \
           """savepoints (aka subtransactions
    
    """

    implements(IRollback)

    def __init__(self, dm):
        self.dm = dm.__class__.__name__

    def rollback(self):
        raise NotImplementedError(
            "%s data managers do not support savepoints (aka subtransactions"
            % self.dm)
