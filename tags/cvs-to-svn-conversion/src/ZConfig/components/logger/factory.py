##############################################################################
#
# Copyright (c) 2002 Zope Corporation and Contributors.
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

_marker = object()

class Factory:
    """Generic wrapper for instance construction.

    Calling the factory causes the instance to be created if it hasn't
    already been created, and returns the object.  Calling the factory
    multiple times returns the same object.

    The instance is created using the factory's create() method, which
    must be overriden by subclasses.
    """
    def __init__(self):
        self.instance = _marker

    def __call__(self):
        if self.instance is _marker:
            self.instance = self.create()
        return self.instance

    def create(self):
        raise NotImplementedError("subclasses need to override create()")
