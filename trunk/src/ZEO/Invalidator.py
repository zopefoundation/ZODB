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
"""Facility for (roughly) atomically invalidating cache entries.

Note that this is not *really* atomic, but it is close enough.
"""

import cPickle
import tempfile

class Invalidator:

    _d=None
    
    def __init__(self, dinvalidate, cinvalidate):
        self.dinvalidate=dinvalidate
        self.cinvalidate=cinvalidate

    def close(self):
        self.dinvalidate = None
        self.cinvalidate = None

    def begin(self):
        self._tfile=tempfile.TemporaryFile()
        pickler=cPickle.Pickler(self._tfile, 1)
        pickler.fast=1 # Don't use the memo
        self._d=pickler.dump

    def invalidate(self, args):
        if self._d is None: return
        self._d(args)

    def end(self):
        if self._d is None: return
        self._d((0,0))
        self._d=None
        self._tfile.seek(0)
        load=cPickle.Unpickler(self._tfile).load
        self._tfile=None

        cinvalidate=self.cinvalidate
        dinvalidate=self.dinvalidate

        while 1:
            oid, version = load()
            if not oid: break
            cinvalidate(oid, version=version)
            dinvalidate(oid, version=version)

    def Invalidate(self, args):
        cinvalidate=self.cinvalidate
        dinvalidate=self.dinvalidate
        for oid, version in args:
            cinvalidate(oid, version=version)
            dinvalidate(oid, version=version)
