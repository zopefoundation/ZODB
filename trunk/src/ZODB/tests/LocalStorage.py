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
class LocalStorage:
    """A single test that only make sense for local storages.

    A local storage is one that doens't use ZEO. The __len__()
    implementation for ZEO is inexact.
    """
    def checkLen(self):
        eq = self.assertEqual
        # The length of the database ought to grow by one each time
        eq(len(self._storage), 0)
        self._dostore()
        eq(len(self._storage), 1)
        self._dostore()
        eq(len(self._storage), 2)
