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
"""Test that a storage's values persist across open and close."""

class PersistentStorage:

    def checkUpdatesPersist(self):
        oids = []

        def new_oid_wrapper(l=oids, new_oid=self._storage.new_oid):
            oid = new_oid()
            l.append(oid)
            return oid

        self._storage.new_oid = new_oid_wrapper

        self._dostore()
        oid = self._storage.new_oid()
        revid = self._dostore(oid)
        if self._storage.supportsVersions():
            self._dostore(oid, revid, data=8, version='b')
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=1)
        revid = self._dostore(oid, revid, data=2)
        self._dostore(oid, revid, data=3)

        # keep copies of all the objects
        objects = []
        for oid in oids:
            p, s = self._storage.load(oid, '')
            objects.append((oid, '', p, s))
            ver = self._storage.modifiedInVersion(oid)
            if ver:
                p, s = self._storage.load(oid, ver)
                objects.append((oid, ver, p, s))

        self._storage.close()
        self.open()

        # keep copies of all the objects
        for oid, ver, p, s in objects:
            _p, _s = self._storage.load(oid, ver)
            self.assertEquals(p, _p)
            self.assertEquals(s, _s)
