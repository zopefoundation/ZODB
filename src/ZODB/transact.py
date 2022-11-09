##############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
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
"""Tools to simplify transactions within applications."""

import transaction

from ZODB.POSException import ConflictError
from ZODB.POSException import ReadConflictError


def _commit(note):
    t = transaction.get()
    if note:
        t.note(note)
    t.commit()


def transact(f, note=None, retries=5):
    """Returns transactional version of function argument f.

    Higher-order function that converts a regular function into
    a transactional function.  The transactional function will
    retry up to retries time before giving up.  If note, it will
    be added to the transaction metadata when it commits.

    The retries occur on ConflictErrors.  If some other
    TransactionError occurs, the transaction will not be retried.
    """

    # TODO:  deal with ZEO disconnected errors?

    def g(*args, **kwargs):
        n = retries
        while n:
            n -= 1
            try:
                r = f(*args, **kwargs)
            except ReadConflictError:
                # the only way ReadConflictError can happen here is due to
                # simultaneous pack removing objects revision that f could try
                # to load.
                transaction.abort()
                if not n:
                    raise
                continue
            try:
                _commit(note)
            except ConflictError:
                transaction.abort()
                if not n:
                    raise
                continue
            return r
        raise RuntimeError("couldn't commit transaction")
    return g
