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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

try:
    from zope.interface import Interface
except ImportError:
    class Interface:
        pass


class IDataManager(Interface):
    """Data management interface for storing objects transactionally

    This is currently implemented by ZODB database connections.

    XXX This exists to document ZODB4 behavior, to help create some
    backward-compatability support for Zope 3.  New classes shouldn't
    implement this. They should implement ZODB.interfaces.IDataManager
    for now. Our hope is that there will eventually be an interface
    like this or that this interface will evolve and become the
    standard interface. There are some issues to be resolved first, like:

    - Probably want separate abort methods for use in and out of
      two-phase commit.

    - The savepoint api may need some more thought.
    
    """

    def prepare(transaction):
        """Perform the first phase of a 2-phase commit

        The data manager prepares for commit any changes to be made
        persistent.  A normal return from this method indicated that
        the data manager is ready to commit the changes.

        The data manager must raise an exception if it is not prepared
        to commit the transaction after executing prepare().

        The transaction must match that used for preceeding
        savepoints, if any.
        """
        
        # This is equivalent to zodb3's tpc_begin, commit, and
        # tpc_vote combined.

    def abort(transaction):
        """Abort changes made by transaction

        This may be called before two-phase commit or in the second
        phase of two-phase commit.

        The transaction must match that used for preceeding
        savepoints, if any.

        """

        # This is equivalent to *both* zodb3's abort and tpc_abort
        # calls. This should probably be split into 2 methods.

    def commit(transaction):
        """Finish two-phase commit

        The prepare method must be called, with the same transaction,
        before calling commit.
        
        """

        # This is equivalent to zodb3's tpc_finish

    def savepoint(transaction):
        """Do tentative commit of changes to this point.

        Should return an object implementing IRollback that can be used
        to rollback to the savepoint.
        
        Note that (unlike zodb3) this doesn't use a 2-phase commit
        protocol.  If this call fails, or if a rollback call on the
        result fails, the (containing) transaction should be
        aborted.  Aborting the containing transaction is *not* the
        responsibility of the data manager, however.

        An implementation that doesn't support savepoints should
        implement this method by returning a rollback implementation
        that always raises an error when it's rollback method is
        called. The savepoing method shouldn't raise an error. This
        way, transactions that create savepoints can proceed as long
        as an attempt is never made to roll back a savepoint.

        """


class IRollback(Interface):

    def rollback():
        """Rollback changes since savepoint.

        IOW, rollback to the last savepoint.

        It is an error to rollback to a savepoint if:

        - An earlier savepoint within the same transaction has been
          rolled back to, or

        - The transaction has ended.
        """
