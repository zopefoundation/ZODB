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

from _persistence import observer, Changed, Used, Unused, GhostState, SavedState, ChangedState
from _persistence import PersistentObserver as PersistentObserverBase
from UserDict import UserDict

class PersistentCache(UserDict):
    """
       Maintains a collection of weak references to persistent objects.
    """
    class PersistentObserver(PersistentObserverBase):
        pass

class DM:
    # A very basic dm
    def __init__(self, serial='00000000', oid=1):
        self.serial = serial
        self.oid = oid
        self.cache = PersistentCache()
        self.storage = {} # a mapping of oid -> (klass, data)
        self.registered = set() # objects which have changed
    
    def __setitem__(self, event, obsv):
        """Events sent through the observer
        """
        
        if event & Changed:
            if obsv.state is GhostState:
                self.setstate(obsv())
            if obsv.state is SavedState:
                self.register(obsv())
                obsv.state = ChangedState
        
        elif event & Used:
            if obsv.state is GhostState:
                self.setstate(obsv())
                obsv.state = SavedState
    
    ######
    # These methods are called by users of Persistent
    
    def activate(self, pobj):
        obsv = observer(pobj)
        if obsv and obsv.state is GhostState: # unattached objects cannot be ghost
            self.setstate(pobj)
    
    def deactivate(self, pobj):
        obsv = observer(pobj)
        if obsv and obsv.state is SavedState and not obsv.nonghostifiable:
            obsv()._p_release_state()
            obsv.state = GhostState
    
    def invalidate(self, pobj):
        # ignore nonghostifiable for now
        obsv = observer(pobj)
        if obsv and not obsv.nonghostifiable:
            self.deregister(pobj)
            pobj._p_release_state()
            obsv.state = GhostState
    
    def unchanged(self, pobj):
        obsv = observer(pobj)
        if obsv and obsv.state == ChangedState:
            self.deregister(pobj)
            obsv.state = SavedState
    #
    ######
    
    def setstate(self, pobj):
        obsv = observer(pobj)
        klass, data = self.storage[obsv.oid]
        pobj.__setstate__(data)
        obsv.state = SavedState
        
    def register(self, pobj):
        """Register a Persistent object with the transaction
        
        As a side effect this ensures that changed objects cannot be gc'ed
        """
        self.registered.add(pobj)
    
    def deregister(self, pobj):
        self.registered.discard(pobj)
        
    def add(self, pobj, oid=None):
        """oid is there only for the tests
        """
        assert observer(pobj) is None
        if oid is None:
            oid = self.oid
            self.oid += 1
        obsv = self.cache.PersistentObserver(pobj)
        obsv.state = SavedState
        obsv.oid = oid
        obsv.serial = self.serial
        obsv.manager = self
        obsv.nonghostifiable = getattr(type(pobj), '_p_nonghostifiable', False)
        self.cache[oid] = obsv
    
    def get(self, oid):
        obsv = self.cache.get(oid, None)
        if obsv is not None:
            pobj = obsv()
            if pobj is not None:
                return pobj
        
        klass, data = self.storage[oid] # this might raise a keyerror
        nonghostifiable = getattr(klass, '_p_nonghostifiable', False)
        pobj = object.__new__(klass)
        obsv = self.cache.PersistentObserver(pobj)
        obsv.state = GhostState        
        obsv.serial = self.serial
        obsv.oid = oid
        obsv.manager = self
        obsv.nonghostifiable = nonghostifiable
        self.cache[obsv.oid] = obsv
        return pobj
