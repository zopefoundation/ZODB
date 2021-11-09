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
"""A minimal persistent object to use for tests"""
from persistent import Persistent


class MinPO(Persistent):
    def __init__(self, value=None):
        self.value = value

    def __cmp__(self, aMinPO):
        return cmp(self.value, aMinPO.value)  # noqa: F821 undefined name 'cmp'

    def __hash__(self):
        return hash(self.value)

    # Py3: Python 3 does not support cmp() anymore. This is insane!!

    def __eq__(self, aMinPO):
        return self.value == aMinPO.value

    def __lt__(self, aMinPO):
        return self.value < aMinPO.value

    # @functools.total_ordering is not available in 2.6 :-(

    def __ne__(self, aMinPO):
        return self.value != aMinPO.value

    def __gt__(self, aMinPO):
        return self.value > aMinPO.value

    def __le__(self, aMinPO):
        return self.value <= aMinPO.value

    def __ge__(self, aMinPO):
        return self.value >= aMinPO.value

    def __repr__(self):
        return "MinPO(%s)" % self.value
