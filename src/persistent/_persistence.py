##############################################################################
#
# Copyright (c) 2008 Zope Corporation and Contributors.
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

from weakref import ref, getweakrefs

GhostState   = None  # The object is a ghost
ChangedState = True   # The object has been modified
SavedState   = False   # The object is not a ghost and has not been modified.

Used       = 1
Unused     = 2
Changed    = 4

class PersistentObserver(ref):
    """
       one entry in a cache

       There is one observer but multiple events, for persistence.
    """
    
    def __init__(self, obj):
        ref.__init__(self, obj, self.cb)
        self.used = 0 # should be read-only

        # assigned by the data manager
        # self.oid = persistent object id
        # self.serial = object serial or None for a ghost
        # self.manager = ?  (r/o attribute)
        # self.manager_data = dm specific data
        # self.nonghostifiable
        self.state = SavedState

        # self.mtime ???

    def cb(self, wref):
        pass

    def __setitem__(self, event, ignored):

        if event & Used:
            self.used += 1

        elif event & Unused:
            self.used -= 1
        
        self.manager[event] = self


def observer(pobj):
    """Return an object's persistent observer, if any.
    """
    for r in getweakrefs(pobj):
        if isinstance(r, PersistentObserver):
            return r
    return None

def oid(pobj):
    obsv = observer(pobj)
    if obsv:
        return obsv.oid

def manager(pobj):
    obsv = observer(pobj)
    if obsv:
        return obsv.manager

def state(pobj):
    obsv = observer(pobj)
    if obsv:  
        return obsv.state
    else:
        return SavedState

def changed(pobj):
    """
       If the object is in the saved or read state, move it to the modified
       state.  Else, do nothing.

       This function is the preferred way to tell the persistence system that
       an object has changed in cases where the persistence system cannot
       detect a change automatically.
    """
    obsv = observer(pobj)
    if obsv:
        obsv[Changed] = 1

def unchanged(pobj):
    """
       If the object is in the changed state, move it to the saved state.
       Else, do nothing.

       This function is used in those very rare situations in which the
       persistence system would determine that an object has changed when it
       should not.
    """
    dm = manager(pobj)
    if dm:
        dm.unchanged(pobj)

def invalidate(pobj):
    dm = manager(pobj)
    if dm:
        dm.invalidate(pobj)

def activate(pobj):
    dm = manager(pobj)
    if dm:
        dm.activate(pobj)

def deactivate(pobj):
    dm = manager(pobj)
    if dm:
        dm.deactivate(pobj)

def mtime(pobj):
    dm = manager(pobj)
    if dm:
        dm.mtime(pobj)

def _get_p_state(pobj):
    s = state(pobj)
    if s is GhostState:
        return -1
    if s is SavedState:
        return 0
    if s is ChangedState:
        return 1

def _set_p_changed(pobj, value):
    if value:
        changed(pobj)
    else:
        unchanged(pobj)

class Persistent(object):
    """Mix-in class providing IPersistent support
    """

    # Deprecated
    _p_jar = property(manager)
    _p_oid = property(oid)
    _p_changed = property(state, _set_p_changed, invalidate)
    _p_state = property(_get_p_state)
    _p_serial = property(lambda self: observer(self).serial)
    _p_mtime = property(mtime)
    _p_invalidate = invalidate
    _p_activate = activate
    _p_deactivate = deactivate
    
    # New interface
    
    _p_nonghostifiable = False # the default
    
    def _p_release_state(self):
        # unconditionally release state. Called by the data manager
        del self.__dict__
    
    def __getstate__(self):
        return dict((k, v) for k, v in self.__dict__.iteritems() if not k[:3] == '_v_')
    
    def __setstate__(self, state):
        del self.__dict__
        self.__dict__.update(state)

    def __getattribute__(self, name):
        if name[:3] != '_p_' and name not in ('__dict__', '__setstate__'):
            obsv = observer(self)
            if obsv:
                obsv[Used] = 1
            try:
                return object.__getattribute__(self, name)
            finally:
                if obsv:
                    obsv[Unused] = 1
        else:
            return object.__getattribute__(self, name)

    def __setattr__(self, name, v):
        if name[:3] not in ('_p_', '_v_') and name != '__dict__':
            obsv = observer(self)
            if obsv:
                obsv[Changed | Used] = 1
            try:
                return object.__setattr__(self, name, v)
            finally:
                if obsv:
                    obsv[Unused] = 1
        else:
            return object.__setattr__(self, name, v)
