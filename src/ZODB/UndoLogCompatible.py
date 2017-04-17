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
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Provide backward compatibility with storages that only have undoLog()."""


class UndoLogCompatible(object):

    def undoInfo(self, first=0, last=-20, specification=None):
        if specification:
            # filter(desc) returns true iff `desc` is a "superdict"
            # of `specification`, meaning that `desc` contains the same
            # (key, value) pairs as `specification`, and possibly additional
            # (key, value) pairs.  Another way to do this might be
            #    d = desc.copy()
            #    d.update(specification)
            #    return d == desc
            def filter(desc, spec=specification.items()):
                get = desc.get
                for k, v in spec:
                    if get(k, None) != v:
                        return 0
                return 1
        else:
            filter = None

        return self.undoLog(first, last, filter)
