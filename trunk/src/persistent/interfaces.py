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
    from zope.interface import Attribute
except ImportError:

    # just allow the module to compile if zope isn't available

    class Interface(object):
        pass

    def Attribute(s):
        return s

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

      This state is identical to the up-to-date state except that the
      object cannot transition to the ghost state. This is a special
      state used by C methods of persistent objects to make sure that
      state is not unloaded in the middle of computation.

      In this state, the _p_changed attribute is non-None and false
      and the _p_jar attribute is set to a data manager.

      There is, currently, no official way to detect whether an object
      is in the sticky state.

    - Changed

      The object has been changed.

      In this state, the _p_changed attribute is true
      and the _p_jar attribute is set to a data manager.

    - Ghost

      the object is in memory but its state has not been loaded from
      the database (or has been unloaded).  In this state, the object
      doesn't contain any data.

    The following state transactions are possible:

    - Unsaved -> Saved

      This transition occurs when an object is saved in the
      database. This usually happens when an unsaved object is added
      to (e.g. as an attribute or item of) a saved (or changed) object
      and the transaction is committed.

    - Saved  -> Changed
      Sticky -> Changed

      This transition occurs when someone sets an attribute or sets
      _p_changed to a true value on an up-to-date or sticky
      object. When the transition occurs, the persistent object is
      required to call the register method on its data manager,
      passing itself as the only argument.

    - Saved -> Sticky

      This transition occurs when C code marks the object as sticky to
      prevent its deactivation and transition to the ghost state.

    - Saved -> Ghost

      This transition occurs when an saved object is deactivated, by:
      calling _p_deactivate, setting _p_changed to None, or deleting
      _p_changed.

    - Sticky -> Saved

      This transition occurs when C code unmarks the object as sticky to
      allow its deactivation and transition to the ghost state.

    - Changed -> Saved

      This transition occurs when a transaction is committed.
      The data manager affects the transaction by setting _p_changed
      to a true value.

    - Changed -> Ghost

      This transition occurs when a transaction is aborted.
      The data manager affects the transaction by deleting _p_changed.

    - Ghost -> Saved

      This transition occurs when an attribute or operation of a ghost
      is accessed and the object's state is loaded from the database.

    Note that there is a separate C API that is not included here.
    The C API requires a specific data layout and defines the sticky
    state that is used to prevent object deactivation while in C
    routines.

    """

    _p_jar=Attribute(
        """The data manager for the object

        The data manager implements the IPersistentDataManager interface.
        If there is no data manager, then this is None.
        """)

    _p_oid=Attribute(
        """The object id

        It is up to the data manager to assign this.
        The special value None is reserved to indicate that an object
        id has not been assigned.
        """)

    _p_changed=Attribute(
        """The persistent state of the object

        This is one of:

        None -- The object is a ghost. It is not active.

        false -- The object is saved (or has never been saved).

        true -- The object has been modified.

        The object state may be changed by assigning this attribute,
        however, assigning None is ignored if the object is not in the
        up-to-date state.

        Note that an object can change to the modified state only if
        it has a data manager. When such a state change occurs, the
        'register' method of the data manager is called, passing the
        persistent object.

        Deleting this attribute forces deactivation independent of
        existing state.

        Note that an attribute is used for this to allow optimized
        cache implementations.
        """)

    _p_serial=Attribute(
        """The object serial number

        This is an arbitrary object.
        """)

    _p_atime=Attribute(
        """The integer object access time, in seconds, modulus one day

        XXX When does a day start, the current implementation appears
        to use gmtime, but this hasn't be explicitly specified.

        XXX Why just one day?
        """)

    def __getstate__():
        """Get the object state data

        The state should not include persistent attributes ("_p_name")
        """

    def __setstate__(state):
        """Set the object state data

        Note that this does not affect the object's persistent state.
        """

    def _p_activate():
        """Activate the object

        Change the object to the up-to-date state if it is a ghost.
        """

    def _p_deactivate():
        """Deactivate the object

        If possible, change an object in the up-to-date state to the
        ghost state.  It may not be possible to make some persistent
        objects ghosts.
        """

class IPersistentNoReadConflicts(IPersistent):
    def _p_independent():
        """Hook for subclasses to prevent read conflict errors

        A specific persistent object type can define this method and
        have it return true if the data manager should ignore read
        conflicts for this object.
        """
class IPersistentDataManager(Interface):
    """Provide services for managing persistent state.

    This interface is used by a persistent object to interact with its
    data manager in the context of a transaction.
    """

    def setstate(object):
        """Load the state for the given object.

        The object should be in the deactivated (ghost) state.
        The object's state will be set and the object will end up
        in the up-to-date state.

        The object must implement the IPersistent interface.
        """

    def register(object):
        """Register a IPersistent with the current transaction.

        This method provides some insulation of the persistent object
        from details of transaction management. For example, it allows
        the use of per-database-connection rather than per-thread
        transaction managers.

        A persistent object should not register with its data manager
        more than once during a single transaction.  XXX should is too
        wishy-washy; we should probably guarantee that this is true,
        and it might be.
        """

    def mtime(object):
        """Return the modification time of the object.

        The modification time may not be known, in which case None
        is returned.
        """

class ICache(Interface):
    """In-memory object cache

    The cache serves two purposes.  It peforms pointer swizzling, and
    it keeps a bounded set of recently used but otherwise unreferenced
    in objects to avoid the cost of re-loading them.

    Pointer swizzling is the process of converting between persistent
    object ids and Python object ids.  When a persistent object is
    serialized, its references to other persistent objects are
    represented as persitent object ids (oids).  When the object is
    unserialized, the oids are converted into references to Python
    objects.  If several different serialized objects refer to the
    same object, they must all refer to the same object when they are
    unserialized.

    A cache stores persistent objects, but it treats ghost objects and
    non-ghost or active objects differently.  It has weak references
    to ghost objects, because ghost objects are only stored in the
    cache to satisfy the pointer swizzling requirement.  It has strong
    references to active objects, because it caches some number of
    them even if they are unreferenced.

    The cache keeps some number of recently used but otherwise
    unreferenced objects in memory.  We assume that there is a good
    chance the object will be used again soon, so keeping it memory
    avoids the cost of recreating the object.
    
    An ICache implementation is intended for use by an
    IPersistentDataManager.
    """

    def get(oid):
        """Return the object from the cache or None."""

    def set(oid, obj):
        """Store obj in the cache under oid.

        obj must implement IPersistent
        """

    def remove(oid):
        """Remove oid from the cache if it exists."""

    def invalidate(oids):
        """Make all of the objects in oids ghosts.

        `oids` is an iterable object that yields oids.
        
        The cache must attempt to change each object to a ghost by
        calling _p_deactivate().

        If an oid is not in the cache, ignore it.
        """

    def clear():
        """Invalidate all the active objects."""

    def activate(oid):
        """Notification that object oid is now active.

        The caller is notifying the cache of a state change.

        Raises LookupError if oid is not in cache.
        """

    def shrink():
        """Remove excess active objects from the cache."""

    def statistics():
        """Return dictionary of statistics about cache size.
        
        Contains at least the following keys:
        active -- number of active objects
        ghosts -- number of ghost objects
        """
