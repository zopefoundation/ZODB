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
"""Provide backward compatability with storages that have undoLog, but not undoInfo."""


class UndoLogCompatible:

    def undoInfo(self, first=0, last=-20, specification=None):
        if specification:
            def filter(desc, spec=specification.items()):
                get=desc.get
                for k, v in spec:
                    if get(k, None) != v:
                        return 0
                return 1
        else: filter=None
            
        return self.undoLog(first, last, filter)
