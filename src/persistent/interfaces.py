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

$Id$
"""

from zope.interface import Interface
from zope.interface import Attribute

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
        """)

    _p_oid = Attribute(
        """The object id.

        It is up to the data manager to assign this.
        The special value None is reserved to indicate that an object
        id has not been assigned.  Non-None object ids must be non-empty
        strings.  The 8-byte string '\0'*8 (8 NUL bytes) is reserved to
        identify the database root object.
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

    def __getstate__():
        """Get the object data.

        The state should not include persistent attributes ("_p_name").
        The result must be picklable.
        """

    def __setstate__(state):
        """Set the object data.
        """

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

# TODO:  document conflict resolution.

class IPersistentDataManager(Interface):
    """Provide services for managing persistent state.

    This interface is used by a persistent object to interact with its
    data manager in the context of a transaction.
    """

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
