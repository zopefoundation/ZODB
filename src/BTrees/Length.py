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

import Persistence

class Length(Persistence.Persistent):
    """BTree lengths are too expensive to compute

    Objects that use BTrees need to keep track of lengths themselves.
    This class provides an object for doing this.

    As a bonus, the object support application-level conflict resolution.
    """

    def __init__(self, v=0): self.value=v

    def __getstate__(self): return self.value

    def __setstate__(self, v): self.value=v

    def set(self, v): self.value=v

    def _p_resolveConflict(self, old, s1, s2): return s1 + s2 - old

    def _p_independent(self):
        # My state doesn't depend on or materially effect the state of
        # other objects.
        return 1

    def change(self, delta): self.value = self.value + delta

    def __call__(self, *args): return self.value
