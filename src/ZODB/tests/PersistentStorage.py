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
"""Test that a storage's values persist across open and close."""

from ZODB.utils import load_current


class PersistentStorage(object):

    def checkUpdatesPersist(self):
        oids = []

        def new_oid_wrapper(l=oids, new_oid=self._storage.new_oid):  # noqa: E741 E501 ambiguous variable name 'l' and line too long
            oid = new_oid()
            l.append(oid)
            return oid

        self._storage.new_oid = new_oid_wrapper

        self._dostore()
        oid = self._storage.new_oid()
        revid = self._dostore(oid)
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=1)
        revid = self._dostore(oid, revid, data=2)
        self._dostore(oid, revid, data=3)

        # keep copies of all the objects
        objects = []
        for oid in oids:
            p, s = load_current(self._storage, oid)
            objects.append((oid, '', p, s))

        self._storage.close()
        self.open()

        # keep copies of all the objects
        for oid, ver, p, s in objects:
            _p, _s = load_current(self._storage, oid)
            self.assertEqual(p, _p)
            self.assertEqual(s, _s)
