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
"""Database connection support

$Id: Connection.py,v 1.81 2003/01/14 15:20:21 shane Exp $"""

from cPickleCache import PickleCache
from POSException import ConflictError, ReadConflictError
from ExtensionClass import Base
import ExportImport, TmpStore
from zLOG import LOG, ERROR, BLATHER, WARNING
from coptimizations import new_persistent_id
from ConflictResolution import ResolvedSerial

from cPickle import Unpickler, Pickler
from cStringIO import StringIO
import sys
from time import time
from types import StringType, ClassType

global_code_timestamp = 0

def updateCodeTimestamp():
    '''
    Called after changes are made to persistence-based classes.
    Causes all connection caches to be re-created as the
    connections are reopened.
    '''
    global global_code_timestamp
    global_code_timestamp = time()

ExtensionKlass = Base.__class__

class Connection(ExportImport.ExportImport):
    """Object managers for individual object space.

    An object space is a version of collection of objects.  In a
    multi-threaded application, each thread get's it's own object
    space.

    The Connection manages movement of objects in and out of object storage.
    """
    _tmp=None
    _debug_info=()
    _opened=None
    _code_timestamp = 0

    # Experimental. Other connections can register to be closed
    # when we close by putting something here.

    def __init__(self, version='', cache_size=400,
                 cache_deactivate_after=60):
        """Create a new Connection"""
        self._version=version
        self._cache = cache = PickleCache(self, cache_size)
        if version:
            # Caches for versions end up empty if the version
            # is not used for a while. Non-version caches
            # keep their content indefinitely.

            # XXX Why do we want version caches to behave this way?

            self._cache.cache_drain_resistance = 100
        self._incrgc=self.cacheGC=cache.incrgc
        self._invalidated=d={}
        self._invalid=d.has_key
        self._committed=[]
        self._code_timestamp = global_code_timestamp
        self._load_count = 0   # Number of objects unghosted
        self._store_count = 0  # Number of objects stored

    def _cache_items(self):
        # find all items on the lru list
        items = self._cache.lru_items()
        # fine everything. some on the lru list, some not
        everything = self._cache.cache_data
        # remove those items that are on the lru list
        for k,v in items:
            del everything[k]
        # return a list of [ghosts....not recently used.....recently used]
        return everything.items() + items

    def __repr__(self):
        if self._version:
            ver = ' (in version %s)' % `self._version`
        else:
            ver = ''
        return '<Connection at %08x%s>' % (id(self), ver)

    def _breakcr(self):
        try: del self._cache
        except: pass
        try: del self._incrgc
        except: pass
        try: del self.cacheGC
        except: pass

    def __getitem__(self, oid, tt=type(())):
        obj = self._cache.get(oid, None)
        if obj is not None:
            return obj

        __traceback_info__ = (oid)
        p, serial = self._storage.load(oid, self._version)
        __traceback_info__ = (oid, p)
        file=StringIO(p)
        unpickler=Unpickler(file)
        unpickler.persistent_load=self._persistent_load

        try:
            object = unpickler.load()
        except:
            raise "Could not load oid %s, pickled data in traceback info may\
            contain clues" % (oid)

        klass, args = object

        if type(klass) is tt:
            module, name = klass
            klass=self._db._classFactory(self, module, name)

        if (args is None or
            not args and not hasattr(klass,'__getinitargs__')):
            object=klass.__basicnew__()
        else:
            object=apply(klass,args)
            if klass is not ExtensionKlass:
                object.__dict__.clear()

        object._p_oid=oid
        object._p_jar=self
        object._p_changed=None
        object._p_serial=serial

        self._cache[oid] = object
        if oid=='\0\0\0\0\0\0\0\0':
            self._root_=object # keep a ref
        return object

    def _persistent_load(self,oid,
                        tt=type(())):

        __traceback_info__=oid

        if type(oid) is tt:
            # Quick instance reference.  We know all we need to know
            # to create the instance wo hitting the db, so go for it!
            oid, klass = oid
            obj = self._cache.get(oid, None)
            if obj is not None:
                return obj

            if type(klass) is tt:
                module, name = klass
                try: klass=self._db._classFactory(self, module, name)
                except:
                    # Eek, we couldn't get the class. Hm.
                    # Maybe their's more current data in the
                    # object's actual record!
                    return self[oid]

            object=klass.__basicnew__()
            object._p_oid=oid
            object._p_jar=self
            object._p_changed=None

            self._cache[oid] = object

            return object

        obj = self._cache.get(oid, None)
        if obj is not None:
            return obj
        return self[oid]

    def sortKey(self):
        # XXX will raise an exception if the DB hasn't been set
        storage_key = self._sortKey()
        # If two connections use the same storage, give them a
        # consistent order using id().  This is unique for the
        # lifetime of a connection, which is good enough.
        return "%s:%s" % (storage_key, id(self))

    def _setDB(self, odb):
        """Begin a new transaction.

        Any objects modified since the last transaction are invalidated.
        """
        self._db=odb
        self._storage=s=odb._storage
        self._sortKey = odb._storage.sortKey
        self.new_oid=s.new_oid
        if self._code_timestamp != global_code_timestamp:
            # New code is in place.  Start a new cache.
            self._resetCache()
        else:
            self._cache.invalidate(self._invalidated)
        self._opened=time()

        return self

    def _resetCache(self):
        '''
        Creates a new cache, discarding the old.
        '''
        self._code_timestamp = global_code_timestamp
        self._invalidated.clear()
        orig_cache = self._cache
        self._cache = cache = PickleCache(self, orig_cache.cache_size)
        self._incrgc = self.cacheGC = cache.incrgc

    def abort(self, object, transaction):
        """Abort the object in the transaction.

        This just deactivates the thing.
        """
        if object is self:
            self._cache.invalidate(self._invalidated)
        else:
            assert object._p_oid is not None
            self._cache.invalidate(object._p_oid)

    def cacheFullSweep(self, dt=0):
        self._cache.full_sweep(dt)

    def cacheMinimize(self, dt=0):
        # dt is ignored
        self._cache.minimize()

    __onCloseCallbacks = None

    def onCloseCallback(self, f):
        if self.__onCloseCallbacks is None:
            self.__onCloseCallbacks = []
        self.__onCloseCallbacks.append(f)

    def close(self):
        self._incrgc() # This is a good time to do some GC
        db=self._db

        # Call the close callbacks.
        if self.__onCloseCallbacks is not None:
            for f in self.__onCloseCallbacks:
                try: f()
                except:
                    f=getattr(f, 'im_self', f)
                    LOG('ZODB',ERROR, 'Close callback failed for %s' % f,
                        error=sys.exc_info())
            self.__onCloseCallbacks = None
        self._db=self._storage=self._tmp=self.new_oid=self._opened=None
        self._debug_info=()
        # Return the connection to the pool.
        db._closeConnection(self)

    __onCommitActions = None

    def onCommitAction(self, method_name, *args, **kw):
        if self.__onCommitActions is None:
            self.__onCommitActions = []
        self.__onCommitActions.append((method_name, args, kw))
        get_transaction().register(self)

    def commit(self, object, transaction):
        if object is self:
            # We registered ourself.  Execute a commit action, if any.
            if self.__onCommitActions is not None:
                method_name, args, kw = self.__onCommitActions.pop(0)
                apply(getattr(self, method_name), (transaction,) + args, kw)
            return
        
        oid = object._p_oid
        invalid = self._invalid
        if oid is None or object._p_jar is not self:
            # new object
            oid = self.new_oid()
            object._p_jar = self
            object._p_oid = oid
            self._creating.append(oid)

        elif object._p_changed:
            if invalid(oid) and not hasattr(object, '_p_resolveConflict'):
                raise ConflictError(object=object)
            self._invalidating.append(oid)

        else:
            # Nothing to do
            return

        stack = [object]

        # Create a special persistent_id that passes T and the subobject
        # stack along:
        #
        # def persistent_id(object,
        #                   self=self,
        #                   stackup=stackup, new_oid=self.new_oid):
        #     if (not hasattr(object, '_p_oid') or
        #         type(object) is ClassType): return None
        #
        #     oid=object._p_oid
        #
        #     if oid is None or object._p_jar is not self:
        #         oid = self.new_oid()
        #         object._p_jar=self
        #         object._p_oid=oid
        #         stackup(object)
        #
        #     klass=object.__class__
        #
        #     if klass is ExtensionKlass: return oid
        #
        #     if hasattr(klass, '__getinitargs__'): return oid
        #
        #     module=getattr(klass,'__module__','')
        #     if module: klass=module, klass.__name__
        #
        #     return oid, klass

        file=StringIO()
        seek=file.seek
        pickler=Pickler(file,1)
        pickler.persistent_id=new_persistent_id(self, stack)
        dbstore=self._storage.store
        file=file.getvalue
        cache=self._cache
        get=cache.get
        dump=pickler.dump
        clear_memo=pickler.clear_memo


        version=self._version

        while stack:
            object=stack[-1]
            del stack[-1]
            oid=object._p_oid
            serial=getattr(object, '_p_serial', '\0\0\0\0\0\0\0\0')
            if serial == '\0\0\0\0\0\0\0\0':
                # new object
                self._creating.append(oid)
            else:
                #XXX We should never get here
                if invalid(oid) and not hasattr(object, '_p_resolveConflict'):
                    raise ConflictError(object=object)
                self._invalidating.append(oid)

            klass = object.__class__

            if klass is ExtensionKlass:
                # Yee Ha!
                dict={}
                dict.update(object.__dict__)
                del dict['_p_jar']
                args=object.__name__, object.__bases__, dict
                state=None
            else:
                if hasattr(klass, '__getinitargs__'):
                    args = object.__getinitargs__()
                    len(args) # XXX Assert it's a sequence
                else:
                    args = None # New no-constructor protocol!

                module=getattr(klass,'__module__','')
                if module: klass=module, klass.__name__
                __traceback_info__=klass, oid, self._version
                state=object.__getstate__()

            seek(0)
            clear_memo()
            dump((klass,args))
            dump(state)
            p=file(1)
            s=dbstore(oid,serial,p,version,transaction)
            self._store_count = self._store_count + 1
            # Put the object in the cache before handling the
            # response, just in case the response contains the
            # serial number for a newly created object
            try: cache[oid]=object
            except:
                # Dang, I bet its wrapped:
                if hasattr(object, 'aq_base'):
                    cache[oid]=object.aq_base
                else:
                    raise

            self._handle_serial(s, oid)

    def commit_sub(self, t):
        """Commit all work done in subtransactions"""
        tmp=self._tmp
        if tmp is None: return
        src=self._storage

        LOG('ZODB', BLATHER,
            'Commiting subtransaction of size %s' % src.getSize())

        self._storage=tmp
        self._tmp=None

        tmp.tpc_begin(t)

        load=src.load
        store=tmp.store
        dest=self._version
        get=self._cache.get
        oids=src._index.keys()

        # Copy invalidating and creating info from temporary storage:
        invalidating=self._invalidating
        invalidating[len(invalidating):]=oids
        creating=self._creating
        creating[len(creating):]=src._creating

        for oid in oids:
            data, serial = load(oid, src)
            s=store(oid, serial, data, dest, t)
            self._handle_serial(s, oid, change=0)

    def abort_sub(self, t):
        """Abort work done in subtransactions"""
        tmp=self._tmp
        if tmp is None: return
        src=self._storage
        self._tmp=None
        self._storage=tmp

        self._cache.invalidate(src._index.keys())
        self._invalidate_creating(src._creating)

    def _invalidate_creating(self, creating=None):
        """Dissown any objects newly saved in an uncommitted transaction.
        """
        if creating is None:
            creating=self._creating
            self._creating=[]

        cache=self._cache
        cache_get=cache.get
        for oid in creating:
            o=cache_get(oid, None)
            if o is not None:
                del cache[oid]
                del o._p_jar
                del o._p_oid

    #XXX

    def db(self): return self._db

    def getVersion(self): return self._version

    def invalidate(self, oid):
        """Invalidate a particular oid

        This marks the oid as invalid, but doesn't actually invalidate
        it.  The object data will be actually invalidated at certain
        transaction boundaries.
        """
        assert oid is not None
        self._invalidated[oid] = 1

    def modifiedInVersion(self, oid):
        try: return self._db.modifiedInVersion(oid)
        except KeyError:
            return self._version

    def register(self, object):
        """Register an object with the appropriate transaction manager.

        A subclass could override this method to customize the default
        policy of one transaction manager for each thread.
        """
        assert object._p_jar is self
        # XXX Figure out why this assert causes test failures
        # assert object._p_oid is not None
        get_transaction().register(object)

    def root(self):
        return self['\0\0\0\0\0\0\0\0']

    def setstate(self, object):
        oid = object._p_oid

        if self._storage is None:
            msg = ("Shouldn't load state for %s "
                   "when the connection is closed" % `oid`)
            LOG('ZODB', ERROR, msg)
            raise RuntimeError(msg)

        try:
            p, serial = self._storage.load(oid, self._version)
            self._load_count = self._load_count + 1

            # XXX this is quite conservative!
            # We need, however, to avoid reading data from a transaction
            # that committed after the current "session" started, as
            # that might lead to mixing of cached data from earlier
            # transactions and new inconsistent data.
            #
            # Note that we (carefully) wait until after we call the
            # storage to make sure that we don't miss an invaildation
            # notifications between the time we check and the time we
            # read.
            if self._invalid(oid):
                if not hasattr(object.__class__, '_p_independent'):
                    get_transaction().register(self)
                    raise ReadConflictError(object=object)
                invalid = 1
            else:
                invalid = 0

            file = StringIO(p)
            unpickler = Unpickler(file)
            unpickler.persistent_load = self._persistent_load
            unpickler.load()
            state = unpickler.load()

            if hasattr(object, '__setstate__'):
                object.__setstate__(state)
            else:
                d = object.__dict__
                for k, v in state.items():
                    d[k] = v

            object._p_serial = serial

            if invalid:
                if object._p_independent():
                    try:
                        del self._invalidated[oid]
                    except KeyError:
                        pass
                else:
                    get_transaction().register(self)
                    raise ConflictError(object=object)

        except ConflictError:
            raise
        except:
            LOG('ZODB',ERROR, "Couldn't load state for %s" % `oid`,
                error=sys.exc_info())
            raise

    def oldstate(self, object, serial):
        oid=object._p_oid
        p = self._storage.loadSerial(oid, serial)
        file=StringIO(p)
        unpickler=Unpickler(file)
        unpickler.persistent_load=self._persistent_load
        unpickler.load()
        return  unpickler.load()

    def setklassstate(self, object):
        try:
            oid=object._p_oid
            __traceback_info__=oid
            p, serial = self._storage.load(oid, self._version)
            file=StringIO(p)
            unpickler=Unpickler(file)
            unpickler.persistent_load=self._persistent_load

            copy = unpickler.load()

            klass, args = copy

            if klass is not ExtensionKlass:
                LOG('ZODB',ERROR,
                    "Unexpected klass when setting class state on %s"
                    % getattr(object,'__name__','(?)'))
                return

            copy=apply(klass,args)
            object.__dict__.clear()
            object.__dict__.update(copy.__dict__)

            object._p_oid=oid
            object._p_jar=self
            object._p_changed=0
            object._p_serial=serial
        except:
            LOG('ZODB',ERROR, 'setklassstate failed', error=sys.exc_info())
            raise

    def tpc_abort(self, transaction):
        if self.__onCommitActions is not None:
            del self.__onCommitActions
        self._storage.tpc_abort(transaction)
        self._cache.invalidate(self._invalidated)
        self._cache.invalidate(self._invalidating)
        self._invalidate_creating()

    def tpc_begin(self, transaction, sub=None):
        self._invalidating = []
        self._creating = []
        if sub:
            # Sub-transaction!
            if self._tmp is None:
                _tmp = TmpStore.TmpStore(self._version)
                self._tmp = self._storage
                self._storage = _tmp
                _tmp.registerDB(self._db, 0)

        self._storage.tpc_begin(transaction)

    def tpc_vote(self, transaction):
        if self.__onCommitActions is not None:
            del self.__onCommitActions
        try:
            vote = self._storage.tpc_vote
        except AttributeError:
            return
        s = vote(transaction)
        self._handle_serial(s)

    def _handle_serial(self, store_return, oid=None, change=1):
        """Handle the returns from store() and tpc_vote() calls."""

        # These calls can return different types depending on whether
        # ZEO is used.  ZEO uses asynchronous returns that may be
        # returned in batches by the ClientStorage.  ZEO1 can also
        # return an exception object and expect that the Connection
        # will raise the exception.

        # When commit_sub() exceutes a store, there is no need to
        # update the _p_changed flag, because the subtransaction
        # tpc_vote() calls already did this.  The change=1 argument
        # exists to allow commit_sub() to avoid setting the flag
        # again.
        if not store_return:
            return
        if isinstance(store_return, StringType):
            assert oid is not None
            serial = store_return
            obj = self._cache.get(oid, None)
            if obj is None:
                return
            if serial == ResolvedSerial:
                obj._p_changed = None
            else:
                if change:
                    obj._p_changed = 0
                obj._p_serial = serial
        else:
            for oid, serial in store_return:
                if not isinstance(serial, StringType):
                    raise serial
                obj = self._cache.get(oid, None)
                if obj is None:
                    continue
                if serial == ResolvedSerial:
                    obj._p_changed = None
                else:
                    if change:
                        obj._p_changed = 0
                    obj._p_serial = serial

    def tpc_finish(self, transaction):
        # It's important that the storage call the function we pass
        # (self._invalidate_invalidating) while it still has it's
        # lock.  We don't want another thread to be able to read any
        # updated data until we've had a chance to send an
        # invalidation message to all of the other connections!

        if self._tmp is not None:
            # Commiting a subtransaction!
            # There is no need to invalidate anything.
            self._storage.tpc_finish(transaction)
            self._storage._creating[:0]=self._creating
            del self._creating[:]
        else:
            self._db.begin_invalidation()
            self._storage.tpc_finish(transaction,
                                     self._invalidate_invalidating)

        self._cache.invalidate(self._invalidated)
        self._incrgc() # This is a good time to do some GC

    def _invalidate_invalidating(self):
        for oid in self._invalidating:
            assert oid is not None
            self._db.invalidate(oid, self)
        self._db.finish_invalidation()

    def sync(self):
        get_transaction().abort()
        sync=getattr(self._storage, 'sync', 0)
        if sync != 0: sync()
        self._cache.invalidate(self._invalidated)
        self._incrgc() # This is a good time to do some GC

    def getDebugInfo(self):
        return self._debug_info

    def setDebugInfo(self, *args):
        self._debug_info = self._debug_info + args

    def getTransferCounts(self, clear=0):
        """Returns the number of objects loaded and stored.

        Set the clear argument to reset the counters.
        """
        res = self._load_count, self._store_count
        if clear:
            self._load_count = 0
            self._store_count = 0
        return res


    ######################################################################
    # Just plain weird. Don't try this at home kids.
    def exchange(self, old, new):
        oid=old._p_oid
        new._p_oid=oid
        new._p_jar=self
        new._p_changed=1
        get_transaction().register(new)
        self._cache[oid]=new

class tConnection(Connection):

    def close(self):
        self._breakcr()
