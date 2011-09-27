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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Persistence Interfaces
"""

from zope.interface import Interface
from zope.interface import Attribute

# Allowed values for _p_state
try:
    from .cPersistence import GHOST
    from .cPersistence import UPTODATE
    from .cPersistence import CHANGED
    from .cPersistence import STICKY
except ImportError:
    GHOST = -1
    UPTODATE = 0
    CHANGED = 1
    STICKY = 2


class IPersistent(Interface):
    """Python persistent interface

    A persistent object can be in one of several states:

    - Unsaved

      The object has been created but not saved in a data manager.

      In this state, the _p_changed attribute is non-None and false
      and the _p_jar attribute is None.

    - Saved

      The object has been saved and has not been changed since it was saved.

      In this state, the _p_changed attribute is non-None and false
      and the _p_jar attribute is set to a data manager.

    - Sticky

      This state is identical to the saved state except that the
      object cannot transition to the ghost state.  This is a special
      state used by C methods of persistent objects to make sure that
      state is not unloaded in the middle of computation.

      In this state, the _p_changed attribute is non-None and false
      and the _p_jar attribute is set to a data manager.

      There is no Python API for detecting whether an object is in the
      sticky state.

    - Changed

      The object has been changed.

      In this state, the _p_changed attribute is true
      and the _p_jar attribute is set to a data manager.

    - Ghost

      the object is in memory but its state has not been loaded from
      the database (or its state has been unloaded).  In this state,
      the object doesn't contain any application data.

      In this state, the _p_changed attribute is None, and the _p_jar
      attribute is set to the data manager from which the object was
      obtained.

    In all the above, _p_oid (the persistent object id) is set when
    _p_jar first gets set.

    The following state transitions are possible:

    - Unsaved -> Saved

      This transition occurs when an object is saved in the
      database.  This usually happens when an unsaved object is added
      to (e.g. as an attribute or item of) a saved (or changed) object
      and the transaction is committed.

    - Saved  -> Changed
      Sticky -> Changed
      Ghost  -> Changed

      This transition occurs when someone sets an attribute or sets
      _p_changed to a true value on a saved, sticky or ghost object.  When
      the transition occurs, the persistent object is required to call the
      register() method on its data manager, passing itself as the
      only argument.

      Prior to ZODB 3.6, setting _p_changed to a true value on a ghost object
      was ignored (the object remained a ghost, and getting its _p_changed
      attribute continued to return None).

    - Saved -> Sticky

      This transition occurs when C code marks the object as sticky to
      prevent its deactivation.

    - Saved -> Ghost

      This transition occurs when a saved object is deactivated or
      invalidated.  See discussion below.

    - Sticky -> Saved

      This transition occurs when C code unmarks the object as sticky to
      allow its deactivation.

    - Changed -> Saved

      This transition occurs when a transaction is committed.  After
      saving the state of a changed object during transaction commit,
      the data manager sets the object's _p_changed to a non-None false
      value.

    - Changed -> Ghost

      This transition occurs when a transaction is aborted.  All changed
      objects are invalidated by the data manager by an abort.

    - Ghost -> Saved

      This transition occurs when an attribute or operation of a ghost
      is accessed and the object's state is loaded from the database.

    Note that there is a separate C API that is not included here.
    The C API requires a specific data layout and defines the sticky
    state.


    About Invalidation, Deactivation and the Sticky & Ghost States

    The sticky state is intended to be a short-lived state, to prevent
    an object's state from being discarded while we're in C routines.  It
    is an error to invalidate an object in the sticky state.

    Deactivation is a request that an object discard its state (become
    a ghost).  Deactivation is an optimization, and a request to
    deactivate may be ignored.  There are two equivalent ways to
    request deactivation:

          - call _p_deactivate()
          - set _p_changed to None

    There are two ways to invalidate an object:  call the
    _p_invalidate() method (preferred) or delete its _p_changed
    attribute.  This cannot be ignored, and is used when semantics
    require invalidation.  Normally, an invalidated object transitions
    to the ghost state.  However, some objects cannot be ghosts.  When
    these objects are invalidated, they immediately reload their state
    from their data manager, and are then in the saved state.

    """

    _p_jar = Attribute(
        """The data manager for the object.

        The data manager implements the IPersistentDataManager interface.

        If there is no data manager, then this is None.

        Once assigned to a data manager, an object cannot be re-assigned
        to another.
        """)

    _p_oid = Attribute(
        """The object id.

        It is up to the data manager to assign this.

        The special value None is reserved to indicate that an object
        id has not been assigned.  Non-None object ids must be non-empty
        strings.  The 8-byte string '\0'*8 (8 NUL bytes) is reserved to
        identify the database root object.

        Once assigned an OID, an object cannot be re-assigned another.
        """)

    _p_changed = Attribute(
        """The persistent state of the object.

        This is one of:

        None -- The object is a ghost.

        false but not None -- The object is saved (or has never been saved).

        true -- The object has been modified since it was last saved.

        The object state may be changed by assigning or deleting this
        attribute; however, assigning None is ignored if the object is
        not in the saved state, and may be ignored even if the object is
        in the saved state.

        At and after ZODB 3.6, setting _p_changed to a true value for a ghost
        object activates the object; prior to 3.6, setting _p_changed to a
        true value on a ghost object was ignored.

        Note that an object can transition to the changed state only if
        it has a data manager.  When such a state change occurs, the
        'register' method of the data manager must be called, passing the
        persistent object.

        Deleting this attribute forces invalidation independent of
        existing state, although it is an error if the sticky state is
        current.
        """)

    _p_serial = Attribute(
        """The object serial number.

        This member is used by the data manager to distiguish distinct
        revisions of a given persistent object.

        This is an 8-byte string (not Unicode).
        """)

    _p_mtime = Attribute(
        """The object's modification time (read-only).

        This is a float, representing seconds since the epoch (as returned
        by time.time).
        """)

    _p_state = Attribute(
        """The object's persistence state token.

        Must be one of GHOST, UPTODATE, CHANGED, or STICKY.
        """)

    _p_estimated_size = Attribute(
        """An estimate of the object's size in bytes.

        May be set by the data manager.
        """)

    # Attribute access protocol
    def __getattribute__(name):
        """ Handle activating ghosts before returning an attribute value.

        "Special" attributes and '_p_*' attributes don't require activation.
        """

    def __setattr__(name, value):
        """ Handle activating ghosts before setting an attribute value.

        "Special" attributes and '_p_*' attributes don't require activation.
        """

    def __delattr__(name):
        """ Handle activating ghosts before deleting an attribute value.

        "Special" attributes and '_p_*' attributes don't require activation.
        """

    # Pickling protocol.
    def __getstate__():
        """Get the object data.

        The state should not include persistent attributes ("_p_name").
        The result must be picklable.
        """

    def __setstate__(state):
        """Set the object data.
        """

    def __reduce__():
        """Reduce an object to contituent parts for serialization.
        """

    # Custom methods
    def _p_activate():
        """Activate the object.

        Change the object to the saved state if it is a ghost.
        """

    def _p_deactivate():
        """Deactivate the object.

        Possibly change an object in the saved state to the
        ghost state.  It may not be possible to make some persistent
        objects ghosts, and, for optimization reasons, the implementation
        may choose to keep an object in the saved state.
        """

    def _p_invalidate():
        """Invalidate the object.

        Invalidate the object.  This causes any data to be thrown
        away, even if the object is in the changed state.  The object
        is moved to the ghost state; further accesses will cause
        object data to be reloaded.
        """

    def _p_getattr(name):
        """Test whether the base class must handle the name
   
        The method unghostifies the object, if necessary.
        The method records the object access, if necessary.
 
        This method should be called by subclass __getattribute__
        implementations before doing anything else. If the method
        returns True, then __getattribute__ implementations must delegate
        to the base class, Persistent.
        """

    def _p_setattr(name, value):
        """Save persistent meta data

        This method should be called by subclass __setattr__ implementations
        before doing anything else.  If it returns true, then the attribute
        was handled by the base class.

        The method unghostifies the object, if necessary.
        The method records the object access, if necessary.
        """

    def _p_delattr(name):
        """Delete persistent meta data

        This method should be called by subclass __delattr__ implementations
        before doing anything else.  If it returns true, then the attribute
        was handled by the base class.

        The method unghostifies the object, if necessary.
        The method records the object access, if necessary.
        """

# TODO:  document conflict resolution.

class IPersistentDataManager(Interface):
    """Provide services for managing persistent state.

    This interface is used by a persistent object to interact with its
    data manager in the context of a transaction.
    """
    _cache = Attribute("The pickle cache associated with this connection.")

    def setstate(object):
        """Load the state for the given object.

        The object should be in the ghost state. The object's state will be
        set and the object will end up in the saved state.

        The object must provide the IPersistent interface.
        """

    def oldstate(obj, tid):
        """Return copy of 'obj' that was written by transaction 'tid'.

        The returned object does not have the typical metadata (_p_jar, _p_oid,
        _p_serial) set. I'm not sure how references to other peristent objects
        are handled.

        Parameters
        obj: a persistent object from this Connection.
        tid: id of a transaction that wrote an earlier revision.

        Raises KeyError if tid does not exist or if tid deleted a revision of
        obj.
        """

    def register(object):
        """Register an IPersistent with the current transaction.

        This method must be called when the object transitions to
        the changed state.

        A subclass could override this method to customize the default
        policy of one transaction manager for each thread.
        """

# Maybe later:
##     def mtime(object):
##         """Return the modification time of the object.

##         The modification time may not be known, in which case None
##         is returned.  If non-None, the return value is the kind of
##         timestamp supplied by Python's time.time().
##         """


class IPickleCache(Interface):
    """ API of the cache for a ZODB connection.
    """
    def __getitem__(oid):
        """ -> the persistent object for OID.

        o Raise KeyError if not found.
        """

    def __setitem__(oid, value):
        """ Save the persistent object under OID.

        o 'oid' must be a string, else raise ValueError.

        o Raise KeyError on duplicate
        """

    def __delitem__(oid):
        """ Remove the persistent object for OID.

        o 'oid' must be a string, else raise ValueError.

        o Raise KeyError if not found.
        """

    def get(oid, default=None):
        """ -> the persistent object for OID.

        o Return 'default' if not found.
        """

    def mru(oid):
        """ Move the element corresonding to 'oid' to the head.

        o Raise KeyError if no element is found.
        """

    def __len__():
        """ -> the number of OIDs in the cache.
        """

    def items():
        """-> a sequence of tuples (oid, value) for cached objects.

        o Only includes items in 'data' (no p-classes).
        """

    def ringlen():
        """ -> the number of persistent objects in the ring.

        o Only includes items in the ring (no ghosts or p-classes).
        """

    def lru_items():
        """ -> a sequence of tuples (oid, value) for cached objects.

        o Tuples will be in LRU order.

        o Only includes items in the ring (no ghosts or p-classes).
        """

    def klass_items():
        """-> a sequence of tuples (oid, value) for cached p-classes.

        o Only includes persistent classes.
        """

    def incrgc():
        """ Perform an incremental garbage collection sweep.

        o Reduce number of non-ghosts to 'cache_size', if possible.
        
        o Ghostify in LRU order.

        o Skip dirty or sticky objects.

        o Quit once we get down to 'cache_size'.
        """

    def full_sweep():
        """ Perform a full garbage collection sweep.

        o Reduce number of non-ghosts to 0, if possible.

        o Ghostify all non-sticky / non-changed objecs.
        """

    def minimize():
        """ Alias for 'full_sweep'.

        o XXX?
        """

    def new_ghost(oid, obj):
        """ Add the given (ghost) object to the cache.

        Also, set its _p_jar and _p_oid, and ensure it is in the
        GHOST state.

        If the object doesn't define '_p_oid' / '_p_jar', raise.

        If the object's '_p_oid' is not None, raise.

        If the object's '_p_jar' is not None, raise.

        If 'oid' is already in the cache, raise. 
        """

    def reify(to_reify):
        """ Reify the indicated objects.

        o If 'to_reify' is a string, treat it as an OID.

        o Otherwise, iterate over it as a sequence of OIDs.

        o For each OID, if present in 'data' and in GHOST state:

            o Call '_p_activate' on the object.

            o Add it to the ring.

        o If any OID is present but not in GHOST state, skip it.

        o Raise KeyErrory if any OID is not present.
        """

    def invalidate(to_invalidate):
        """ Invalidate the indicated objects.

        o If 'to_invalidate' is a string, treat it as an OID.

        o Otherwise, iterate over it as a sequence of OIDs.

        o Any OID corresponding to a p-class will cause the corresponding
            p-class to be removed from the cache.

        o For all other OIDs, ghostify the corrsponding object and 
            remove it from the ring.
        """

    def debug_info():
        """Return debugging data about objects in the cache.

        o Return a sequence of tuples, (oid, refcount, typename, state).
        """

    def update_object_size_estimation(oid, new_size):
        """Update the cache's size estimation for 'oid', if known to the cache.
        """

    cache_size = Attribute(u'Target size of the cache')
    cache_drain_resistance = Attribute(u'Factor for draining cache below '
                                        u'target size')
    cache_non_ghost_count = Attribute(u'Number of non-ghosts in the cache '
                                        u'(XXX how is it different from '
                                        u'ringlen?')
    cache_data = Attribute(u"Property:  copy of our 'data' dict")
    cache_klass_count = Attribute(u"Property: len of 'persistent_classes'")
