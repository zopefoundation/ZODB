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
"""Interfaces

This package implements the Python "scarecrow" proposal.

The package exports two names, 'Interface' and 'Attribute' directly.
Interface is used to create an interface with a class statement, as
in:



  class IMyInterface(Interface):
    '''Interface documentation
    '''

    def meth(arg1, arg2):
        '''Documentation for meth
        '''

    # Note that there is no self argument

To find out what you can do with interfaces, see the interface
interface, IInterface in the IInterface module.

The package has several public modules:

  XXX This docstring needs to be updated after the Grand Renaming.

  o Attribute has the implementation for interface attributes
    for people who want to build interfaces by hand.
    (Maybe someone should cry YAGNI for this. ;)

  o Document has a utility for documenting an interface as structured text.

  o Exceptions has the interface-defined exceptions

  o IAttribute defines the attribute descriptor interface.

  o IElement defined the base interface for IAttribute, IInterface,
    and IMethod.

  o IInterface defines the interface interface

  o IMethod defined the method interface.

  o Implements has various utilities for examining interface assertions.

  o Method has the implementation for interface methods. See above.

  o Verify has utilities for verifying (sort of) interfaces.

See the module doc strings for more information.

There is also a script, pyskel.py in the package that can be used to
create interface skeletons. Run it without arguments to get documentation.

Revision information:
$Id$
"""

from zope.interface.interface import Interface, _wire

# Need to actually get the interface elements to implement the right interfaces
_wire()
del _wire

from zope.interface.interface import Attribute, invariant

from zope.interface.declarations import providedBy, implementedBy
from zope.interface.declarations import classImplements, classImplementsOnly
from zope.interface.declarations import directlyProvidedBy, directlyProvides
from zope.interface.declarations import implements, implementsOnly
from zope.interface.declarations import classProvides, moduleProvides
from zope.interface.declarations import Declaration
from zope.interface.exceptions import Invalid

# The following are to make spec pickles cleaner
from zope.interface.declarations import Provides


from zope.interface.interfaces import IInterfaceDeclaration

moduleProvides(IInterfaceDeclaration)

__all__ = ('Interface', 'Attribute') + tuple(IInterfaceDeclaration)
