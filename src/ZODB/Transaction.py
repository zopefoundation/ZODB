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
"""Transaction management

$Id: Transaction.py,v 1.48 2003/03/07 00:11:10 jeremy Exp $
"""

import time, sys, struct, POSException
from struct import pack
from string import split, strip, join
from zLOG import LOG, ERROR, PANIC, INFO, BLATHER, WARNING
from POSException import ConflictError

# Flag indicating whether certain errors have occurred.
hosed=0

# There is an order imposed on all jars, based on the storages they
# serve, that must be consistent across all applications using the
# storages.  The order is defined by the sortKey() method of the jar.

def jar_cmp(j1, j2):
    # Call sortKey() every time, because a ZEO client could reconnect
    # to a different server at any time.
    try:
        k1 = j1.sortKey()
    except:
        LOG("TM", WARNING, "jar missing sortKey() method: %s" % j1)
        k1 = id(j1)

    try:
        k2 = j2.sortKey()
    except:
        LOG("TM", WARNING, "jar missing sortKey() method: %s" % j2)
        k2 = id(j2)
        
    return cmp(k1, k2)

class Transaction:
    user = ''
    description = ''
    _connections = None
    _extension = None
    _sub = None # This is a subtrasaction flag

    # The _non_st_objects variable is either None or a list
    # of jars that do not support subtransactions. This is used to
    # manage non-subtransaction-supporting jars during subtransaction
    # commits and aborts to ensure that they are correctly committed
    # or aborted in the "outside" transaction.
    _non_st_objects=None

    def __init__(self, id=None):
        self._id=id
        self._objects=[]
        self._append=self._objects.append

    def _init(self):
        self._objects=[]
        self._append=self._objects.append
        self.user=self.description=''
        if self._connections:
            for c in self._connections.values(): c.close()
            del self._connections

    def log(self, msg, level=INFO, error=None):
        LOG("TM:%s" % self._id, level, msg, error=error)

    def sub(self):
        # Create a manually managed subtransaction for internal use
        r=self.__class__()
        r.user=self.user
        r.description=self.description
        r._extension=self._extension
        return r

    def __str__(self):
        if self._id is None:
            return "Transaction user=%s" % `self.user`
        else:
            return "Transaction thread=%s user=%s" % (self._id, `self.user`)

    def __del__(self):
        if self._objects:
            self.abort(freeme=0)

    def abort(self, subtransaction=0, freeme=1):
        """Abort the transaction.

        This is called from the application.  This means that we haven't
        entered two-phase commit yet, so no tpc_ messages are sent.
        """
        if subtransaction and (self._non_st_objects is not None):
            raise POSException.TransactionError, (
                """Attempted to abort a sub-transaction, but a participating
                data manager doesn't support partial abort.
                """)

        t = None

        if not subtransaction:
            # Must add in any non-subtransaction supporting objects that
            # may have been stowed away from previous subtransaction
            # commits.
            if self._non_st_objects is not None:
                self._objects.extend(self._non_st_objects)
                self._non_st_objects = None

            if self._sub is not None:
                # Abort of top-level transaction after commiting
                # subtransactions.
                subjars = self._sub.values()
                subjars.sort(jar_cmp)
                self._sub = None
            else:
                subjars = []

        try:
            # Abort the objects
            for o in self._objects:
                try:
                    j = getattr(o, '_p_jar', o)
                    if j is not None:
                        j.abort(o, self)
                except:
                    # Record the first exception that occurred
                    if t is None:
                        t, v, tb = sys.exc_info()
                    else:
                        self.log("Failed to abort object %s" %
                                 repr(o._p_oid), error=sys.exc_info())

            # tpc_begin() was never called, so tpc_abort() should not be
            # called.

            if not subtransaction:
                # abort_sub() must be called to clear subtransaction state
                for jar in subjars:
                    jar.abort_sub(self) # This should never fail

            if t is not None:
                raise t, v, tb

        finally:
            if t is not None:
                del tb # don't keep traceback in local variable
            del self._objects[:] # Clear registered
            if not subtransaction and freeme:
                if self._id is not None:
                    free_transaction()
            else:
                self._init()

    def begin(self, info=None, subtransaction=None):
        """Begin a new transaction.

        This aborts any transaction in progres.
        """
        if self._objects:
            self.abort(subtransaction, 0)
        if info:
            info=split(info,'\t')
            self.user=strip(info[0])
            self.description=strip(join(info[1:],'\t'))

    def commit(self, subtransaction=None):
        """Finalize the transaction."""
        objects = self._objects

        subjars = []
        if subtransaction:
            if self._sub is None:
                # Must store state across multiple subtransactions
                # so that the final commit can commit all subjars.
                self._sub = {}
        else:
            if self._sub is not None:
                # This commit is for a top-level transaction that
                # has previously committed subtransactions.  Do
                # one last subtransaction commit to clear out the
                # current objects, then commit all the subjars.
                if objects:
                    self.commit(1)
                    objects = []
                subjars = self._sub.values()
                subjars.sort(jar_cmp)
                self._sub = None

                # If there were any non-subtransaction-aware jars
                # involved in earlier subtransaction commits, we need
                # to add them to the list of jars to commit.
                if self._non_st_objects is not None:
                    objects.extend(self._non_st_objects)
                    self._non_st_objects = None

        if (objects or subjars) and hosed:
            # Something really bad happened and we don't
            # trust the system state.
            raise POSException.TransactionError, hosed_msg

        # It's important that:
        #
        # - Every object in self._objects is either committed or
        #   aborted.
        #
        # - For each object that is committed we call tpc_begin on
        #   it's jar at least once
        #
        # - For every jar for which we've called tpc_begin on, we
        #   either call tpc_abort or tpc_finish. It is OK to call
        #   these multiple times, as the storage is required to ignore
        #   these calls if tpc_begin has not been called.
        #
        # - That we call tpc_begin() in a globally consistent order,
        #   so that concurrent transactions involving multiple storages
        #   do not deadlock.
        try:
            ncommitted = 0
            jars = self._get_jars(objects, subtransaction)
            try:
                # If not subtransaction, then jars will be modified.
                self._commit_begin(jars, subjars, subtransaction)
                ncommitted += self._commit_objects(objects)
                if not subtransaction:
                    # Unless this is a really old jar that doesn't
                    # implement tpc_vote(), it must raise an exception
                    # if it can't commit the transaction.
                    for jar in jars:
                        try:
                            vote = jar.tpc_vote
                        except AttributeError:
                            pass
                        else:
                            vote(self)

                # Handle multiple jars separately.  If there are
                # multiple jars and one fails during the finish, we
                # mark this transaction manager as hosed.
                if len(jars) == 1:
                    self._finish_one(jars[0])
                else:
                    self._finish_many(jars)
            except:
                # Ugh, we got an got an error during commit, so we
                # have to clean up.  First save the original exception
                # in case the cleanup process causes another
                # exception.
                error = sys.exc_info()
                try:
                    self._commit_error(objects, ncommitted, jars, subjars)
                except:
                    LOG('ZODB', ERROR,
                        "A storage error occured during transaction "
                        "abort.  This shouldn't happen.",
                        error=sys.exc_info())
                raise error[0], error[1], error[2]
        finally:
            del objects[:] # clear registered
            if not subtransaction and self._id is not None:
                free_transaction()

    def _get_jars(self, objects, subtransaction):
        # Returns a list of jars for this transaction.
        
        # Find all the jars and sort them in a globally consistent order.
        # objects is a list of persistent objects and jars.
        # If this is a subtransaction and a jar is not subtransaction aware,
        # it's object gets delayed until the parent transaction commits.
        
        d = {}
        for o in objects:
            jar = getattr(o, '_p_jar', o)
            if jar is None:
                # I don't think this should ever happen, but can't
                # prove that it won't.  If there is no jar, there
                # is nothing to be done.
                self.log("Object with no jar registered for transaction: "
                         "%s" % repr(o), level=BLATHER)
                continue
            # jar may not be safe as a dictionary key
            key = id(jar)
            d[key] = jar

            if subtransaction:
                if hasattr(jar, "commit_sub"):
                    self._sub[key] = jar
                else:
                    if self._non_st_objects is None:
                        self._non_st_objects = []
                    self._non_st_objects.append(o)
                
        jars = d.values()
        jars.sort(jar_cmp)

        return jars

    def _commit_begin(self, jars, subjars, subtransaction):
        if subtransaction:
            assert not subjars
            for jar in jars:
                try:
                    jar.tpc_begin(self, subtransaction)
                except TypeError:
                    # Assume that TypeError means that tpc_begin() only
                    # takes one argument, and that the jar doesn't
                    # support subtransactions.
                    jar.tpc_begin(self)
        else:
            # Merge in all the jars used by one of the subtransactions.

            # When the top-level subtransaction commits, the tm must
            # call commit_sub() for each jar involved in one of the
            # subtransactions.  The commit_sub() method should call
            # tpc_begin() on the storage object.

            # It must also call tpc_begin() on jars that were used in
            # a subtransaction but don't support subtransactions.

            # These operations must be performed on the jars in order.

            # Modify jars inplace to include the subjars, too.
            jars += subjars
            jars.sort(jar_cmp)
            # assume that subjars is small, so that it's cheaper to test
            # whether jar in subjars than to make a dict and do has_key.
            for jar in jars:
                if jar in subjars:
                    jar.commit_sub(self)
                else:
                    jar.tpc_begin(self)

    def _commit_objects(self, objects):
        ncommitted = 0
        for o in objects:
            jar = getattr(o, "_p_jar", o)
            if jar is None:
                continue
            jar.commit(o, self)
            ncommitted += 1
        return ncommitted

    def _finish_one(self, jar):
        try:
            # The database can't guarantee consistency if call fails.
            jar.tpc_finish(self)
        except:
            # Bug if it does, we need to keep track of it
            LOG('ZODB', PANIC,
                "A storage error occurred in the last phase of a "
                "two-phase commit.  This shouldn\'t happen. ",
                error=sys.exc_info())
            raise

    def _finish_many(self, jars):
        global hosed
        try:
            for jar in jars:
                # The database can't guarantee consistency if call fails.
                jar.tpc_finish(self)
        except:
            # XXX We should consult ZConfig to decide whether we want to put
            # the transaction manager in a hosed state or not.
            #hosed = 1
            LOG('ZODB', PANIC,
                "A storage error occurred in the last phase of a "
                "two-phase commit.  This shouldn\'t happen. "
                "The application will not be allowed to commit "
                "until the site/storage is reset by a restart. ",
                error=sys.exc_info())
            raise

    def _commit_error(self, objects, ncommitted, jars, subjars):
        # First, we have to abort any uncommitted objects.  The abort
        # will mark the object for invalidation, so that it's last
        # committed state will be restored.
        for o in objects[ncommitted:]:
            try:
                j = getattr(o, '_p_jar', o)
                if j is not None:
                    j.abort(o, self)
            except:
                # nothing to do but log the error
                self.log("Failed to abort object %s" % repr(o._p_oid),
                         error=sys.exc_info())

        # Abort the two-phase commit.  It's only necessary to abort the
        # commit for jars that began it, but it is harmless to abort it
        # for all.
        for j in jars:
            try:
                j.tpc_abort(self) # This should never fail
            except:
                LOG('ZODB', ERROR,
                    "A storage error occured during object abort. This "
                    "shouldn't happen. ", error=sys.exc_info())

        # After the tpc_abort(), call abort_sub() on all the
        # subtrans-aware jars to *really* abort the subtransaction.
        
        # Example: For Connection(), the tpc_abort() will abort the
        # subtransaction TmpStore() and abort_sub() will remove the
        # TmpStore.

        for j in subjars:
            try:
                j.abort_sub(self) # This should never fail
            except:
                LOG('ZODB', ERROR,
                    "A storage error occured during sub-transaction "
                    "object abort.  This shouldn't happen.",
                    error=sys.exc_info())

    def register(self,object):
        'Register the given object for transaction control.'
        self._append(object)

    def note(self, text):
        if self.description:
            self.description = "%s\n\n%s" % (self.description, strip(text))
        else:
            self.description = strip(text)

    def setUser(self, user_name, path='/'):
        self.user="%s %s" % (path, user_name)

    def setExtendedInfo(self, name, value):
        ext=self._extension
        if ext is None:
            ext=self._extension={}
        ext[name]=value

hosed_msg = \
"""A serious error, which was probably a system error,
occurred in a previous database transaction.  This
application may be in an invalid state and must be
restarted before database updates can be allowed.

Beware though that if the error was due to a serious
system problem, such as a disk full condition, then
the application may not come up until you deal with
the system problem.  See your application log for
information on the error that lead to this problem.
"""

############################################################################
# install get_transaction:

try:
    import thread

except:
    _t = Transaction(None)

    def get_transaction(_t=_t):
        return _t

    def free_transaction(_t=_t):
        _t.__init__()

else:
    _t = {}

    def get_transaction(_id=thread.get_ident, _t=_t, get=_t.get):
        id = _id()
        t = get(id, None)
        if t is None:
            _t[id] = t = Transaction(id)
        return t

    def free_transaction(_id=thread.get_ident, _t=_t):
        id = _id()
        try:
            del _t[id]
        except KeyError:
            pass

    del thread

del _t

import __builtin__
__builtin__.get_transaction=get_transaction
del __builtin__
