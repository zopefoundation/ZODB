##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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
"""Mapping Interfaces

$Id$
"""
from zope.interface import Interface

class IItemMapping(Interface):
    """Simplest readable mapping object
    """

    def __getitem__(key):
        """Get a value for a key

        A KeyError is raised if there is no value for the key.
        """


class IReadMapping(IItemMapping):
    """Basic mapping interface
    """

    def get(key, default=None):
        """Get a value for a key

        The default is returned if there is no value for the key.
        """

    def __contains__(key):
        """Tell if a key exists in the mapping."""


class IWriteMapping(Interface):
    """Mapping methods for changing data"""
    
    def __delitem__(key):
        """Delete a value from the mapping using the key."""

    def __setitem__(key, value):
        """Set a new item in the mapping."""
        

class IEnumerableMapping(IReadMapping):
    """Mapping objects whose items can be enumerated.
    """

    def keys():
        """Return the keys of the mapping object.
        """

    def __iter__():
        """Return an iterator for the keys of the mapping object.
        """

    def values():
        """Return the values of the mapping object.
        """

    def items():
        """Return the items of the mapping object.
        """

    def __len__():
        """Return the number of items.
        """


class IMapping(IReadMapping, IWriteMapping, IEnumerableMapping):
    ''' Full mapping interface '''
