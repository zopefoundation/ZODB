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
"""ZODB undo support for Zope.

This package is used to support the Prefix object that Zope uses for
undo.  It is a separate package only to aid configuration management.
This package is included in Zope and ZODB3, so that ZODB3 is suitable
for running a ZEO server that handles Zope undo.
"""

class Prefix:
    """A Prefix() is equal to any string it as a prefix of.

    This class can be compared to a string (or arbitrary sequence).
    The comparison will return True if the prefix value is a prefix of
    the string being compared.

    Two prefixes can not be compared.
    """

    __no_side_effects__ = 1

    def __init__(self, path):
        self.value = len(path), path

    def __cmp__(self, o):
        l, v = self.value
        return cmp(o[:l], v)
