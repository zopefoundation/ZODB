"""Facility for (roughly) atomically invalidating cache entries.

Note that this is not *really* atomic, but it is close enough.
"""

import tempfile, cPickle

class Invalidator:

    _d=None
    
    def __init__(self, dinvalidate, cinvalidate):
        self.dinvalidate=dinvalidate
        self.cinvalidate=cinvalidate

    def begin(self):
        self._tfile=tempfile.TemporaryFile()
        self._d=cPickle.Pickler(self._tfile).dump

    def invalidate(self, args):
        if self._d is None: return
        for arg in args:
            self._d(arg)

    def end(self):
        if self._d is None: return
        self._d((0,0,0))
        self._d=None
        self._tfile.seek(0)
        load=cPickle.Unpickler(self._tfile).load
        self._tfile=None

        cinvalidate=self.cinvalidate
        dinvalidate=self.dinvalidate

        while 1:
            oid, serial, version = load()
            if not oid: break
            cinvalidate(oid, version=version)
            dinvalidate(oid, version=version)

