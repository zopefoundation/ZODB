##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
"""Adapter registry tests

$Id$
"""
import unittest, doctest
import zope.interface
from zope.interface.adapter import AdapterRegistry
import zope.interface

class IF0(zope.interface.Interface):
    pass
class IF1(IF0):
    pass

class IB0(zope.interface.Interface):
    pass
class IB1(IB0):
    pass

class IR0(zope.interface.Interface):
    pass
class IR1(IR0):
    pass

def test_multi_adapter_w_default():
    """
    >>> registry = AdapterRegistry()
    
    >>> registry.register([None, None], IB1, 'bob', 'A0')

    >>> registry.lookup((IF1, IR1), IB0, 'bob')
    'A0'
    
    >>> registry.register([None, IR0], IB1, 'bob', 'A1')

    >>> registry.lookup((IF1, IR1), IB0, 'bob')
    'A1'
    
    >>> registry.lookup((IF1, IR1), IB0, 'bruce')

    >>> registry.register([None, IR1], IB1, 'bob', 'A2')
    >>> registry.lookup((IF1, IR1), IB0, 'bob')
    'A2'
    """

def test_multi_adapter_w_inherited_and_multiple_registrations():
    """
    >>> registry = AdapterRegistry()

    >>> class IX(zope.interface.Interface):
    ...    pass

    >>> registry.register([IF0, IR0], IB1, 'bob', 'A1')
    >>> registry.register([IF1, IX], IB1, 'bob', 'AX')

    >>> registry.lookup((IF1, IR1), IB0, 'bob')
    'A1'
    """

def test_named_adapter_with_default():
    """Query a named simple adapter

    >>> registry = AdapterRegistry()

    If we ask for a named adapter, we won't get a result unless there
    is a named adapter, even if the object implements the interface:

    >>> registry.lookup([IF1], IF0, 'bob')

    >>> registry.register([None], IB1, 'bob', 'A1')
    >>> registry.lookup([IF1], IB0, 'bob')
    'A1'

    >>> registry.lookup([IF1], IB0, 'bruce')

    >>> registry.register([None], IB0, 'bob', 'A2')
    >>> registry.lookup([IF1], IB0, 'bob')
    'A2'
    """

def test_multi_adapter_gets_closest_provided():
    """
    >>> registry = AdapterRegistry()
    >>> registry.register([IF1, IR0], IB0, 'bob', 'A1')
    >>> registry.register((IF1, IR0), IB1, 'bob', 'A2')
    >>> registry.lookup((IF1, IR1), IB0, 'bob')
    'A1'

    >>> registry = AdapterRegistry()
    >>> registry.register([IF1, IR0], IB1, 'bob', 'A2')
    >>> registry.register([IF1, IR0], IB0, 'bob', 'A1')
    >>> registry.lookup([IF1, IR0], IB0, 'bob')
    'A1'

    >>> registry = AdapterRegistry()
    >>> registry.register([IF1, IR0], IB0, 'bob', 'A1')
    >>> registry.register([IF1, IR1], IB1, 'bob', 'A2')
    >>> registry.lookup([IF1, IR1], IB0, 'bob')
    'A2'

    >>> registry = AdapterRegistry()
    >>> registry.register([IF1, IR1], IB1, 'bob', 2)
    >>> registry.register([IF1, IR0], IB0, 'bob', 1)
    >>> registry.lookup([IF1, IR1], IB0, 'bob')
    2
    """

def test_multi_adapter_check_non_default_dont_hide_default():
    """
    >>> registry = AdapterRegistry()

    >>> class IX(zope.interface.Interface):
    ...     pass

    
    >>> registry.register([None, IR0], IB0, 'bob', 1)
    >>> registry.register([IF1,   IX], IB0, 'bob', 2)
    >>> registry.lookup([IF1, IR1], IB0, 'bob')
    1
    """


def test_suite():
    from docfilesuite import DocFileSuite
    return unittest.TestSuite((
        DocFileSuite('../adapter.txt', 'foodforthought.txt'),
        doctest.DocTestSuite(),
        ))

if __name__ == '__main__': unittest.main()
