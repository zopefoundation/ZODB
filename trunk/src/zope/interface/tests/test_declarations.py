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
"""Test the new API for making and checking interface declarations


$Id$
"""

import unittest
from zope.interface import *
from zope.testing.doctestunit import DocTestSuite
from zope.interface import Interface

class I1(Interface): pass
class I2(Interface): pass
class I3(Interface): pass
class I4(Interface): pass
class I5(Interface): pass

class A:
    implements(I1)
class B:
    implements(I2)
class C(A, B):
    implements(I3)

class COnly(A, B):
    implementsOnly(I3)

class COnly_old(A, B):
    __implemented__ = I3
    
class D(COnly):
    implements(I5)
    
def test_ObjectSpecification_Simple():
    """
    >>> c = C()
    >>> directlyProvides(c, I4)
    >>> [i.__name__ for i in providedBy(c)]
    ['I4', 'I3', 'I1', 'I2']
    """

def test_ObjectSpecification_Simple_w_only():
    """
    >>> c = COnly()
    >>> directlyProvides(c, I4)
    >>> [i.__name__ for i in providedBy(c)]
    ['I4', 'I3']
    """

def test_ObjectSpecification_Simple_old_style():
    """
    >>> c = COnly_old()
    >>> directlyProvides(c, I4)
    >>> [i.__name__ for i in providedBy(c)]
    ['I4', 'I3']
    """


class Test(unittest.TestCase):

    # Note that most of the tests are in the doc strings of the
    # declarations module.

    def test_backward_compat(self):

        class C1: __implemented__ = I1
        class C2(C1): __implemented__ = I2, I5
        class C3(C2): __implemented__ = I3, C2.__implemented__

        self.assert_(C3.__implemented__.__class__ is tuple)

        self.assertEqual(
            [i.getName() for i in providedBy(C3())],
            ['I3', 'I2', 'I5'],
            )

        class C4(C3):
            implements(I4)

        self.assertEqual(
            [i.getName() for i in providedBy(C4())],
            ['I4', 'I3', 'I2', 'I5'],
            )

        self.assertEqual(
            [i.getName() for i in C4.__implemented__],
            ['I4', 'I3', 'I2', 'I5'],
            )

        # Note that C3.__implemented__ should now be a sequence of interfaces
        self.assertEqual(
            [i.getName() for i in C3.__implemented__],
            ['I3', 'I2', 'I5'],
            )
        self.failIf(C3.__implemented__.__class__ is tuple)

    def test_module(self):
        import zope.interface.tests.m1
        import zope.interface.tests.m2
        directlyProvides(zope.interface.tests.m2,
                         zope.interface.tests.m1.I1,
                         zope.interface.tests.m1.I2,
                         )
        self.assertEqual(list(providedBy(zope.interface.tests.m1)),
                         list(providedBy(zope.interface.tests.m2)),
                         )

    def test_builtins(self):
        # Setup

        intspec = implementedBy(int)
        olddeclared = intspec.declared
                
        classImplements(int, I1)
        class myint(int):
            implements(I2)

        x = 42
        self.assertEqual([i.getName() for i in providedBy(x)],
                         ['I1'])

        x = myint(42)
        directlyProvides(x, I3)
        self.assertEqual([i.getName() for i in providedBy(x)],
                         ['I3', 'I2', 'I1'])

        # cleanup
        intspec.declared = olddeclared
        classImplements(int)

        x = 42
        self.assertEqual([i.getName() for i in providedBy(x)],
                         [])
        

def test_signature_w_no_class_interfaces():
    """
    >>> from zope.interface import *
    >>> class C:
    ...     pass
    >>> c = C()
    >>> list(providedBy(c))
    []
    
    >>> class I(Interface):
    ...    pass
    >>> directlyProvides(c, I)
    >>> list(providedBy(c))  == list(directlyProvidedBy(c))
    1
    """

def test_classImplement_on_deeply_nested_classes():
    """This test is in response to a bug found, which is why it's a bit
    contrived

    >>> from zope.interface import *
    >>> class B1:
    ...     pass
    >>> class B2(B1):
    ...     pass
    >>> class B3(B2):
    ...     pass
    >>> class D:
    ...     implements()
    >>> class S(B3, D):
    ...     implements()

    This failed due to a bug in the code for finding __providedBy__
    descriptors for old-style classes.

    """

def test_pickle_provides_specs():
    """
    >>> from pickle import dumps, loads
    >>> a = A()
    >>> I2.providedBy(a)
    0
    >>> directlyProvides(a, I2)
    >>> I2.providedBy(a)
    1
    >>> a2 = loads(dumps(a))
    >>> I2.providedBy(a2)
    1
    
    """

def test_that_we_dont_inherit_class_provides():
    """
    >>> class X:
    ...     classProvides(I1)
    >>> class Y(X):
    ...     pass
    >>> [i.__name__ for i in X.__provides__]
    ['I1']
    >>> Y.__provides__
    Traceback (most recent call last):
    ...
    AttributeError: __provides__
    
    """

def test_that_we_dont_inherit_provides_optimizations():
    """

    When we make a declaration for a class, we install a __provides__
    descriptors that provides a default for instances that don't have
    instance-specific declarations:
    
    >>> class A:
    ...     implements(I1)

    >>> class B:
    ...     implements(I2)

    >>> [i.__name__ for i in A().__provides__]
    ['I1']
    >>> [i.__name__ for i in B().__provides__]
    ['I2']

    But it's important that we don't use this for subclasses without
    declarations.  This would cause incorrect results:

    >>> class X(A, B):
    ...     pass

    >>> X().__provides__
    Traceback (most recent call last):
    ...
    AttributeError: __provides__

    However, if we "induce" a declaration, by calling implementedBy
    (even indirectly through providedBy):

    >>> [i.__name__ for i in providedBy(X())]
    ['I1', 'I2']


    then the optimization will work:
    
    >>> [i.__name__ for i in X().__provides__]
    ['I1', 'I2']
    
    """

def test_classProvides_before_implements():
    """Special descriptor for class __provides__

    The descriptor caches the implementedBy info, so that
    we can get declarations for objects without instance-specific
    interfaces a bit quicker.

        For example::

          >>> from zope.interface import Interface
          >>> class IFooFactory(Interface):
          ...     pass
          >>> class IFoo(Interface):
          ...     pass
          >>> class C:
          ...     classProvides(IFooFactory)
          ...     implements(IFoo)
          >>> [i.getName() for i in C.__provides__]
          ['IFooFactory']

          >>> [i.getName() for i in C().__provides__]
          ['IFoo']
    """

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    suite.addTest(DocTestSuite("zope.interface.declarations"))
    suite.addTest(DocTestSuite())
    
    return suite


if __name__ == '__main__':
    unittest.main()
