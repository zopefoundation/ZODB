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

from zope.interface.exceptions import BrokenImplementation, DoesNotImplement
from zope.interface.exceptions import BrokenMethodImplementation
from types import FunctionType, MethodType
from zope.interface.interface import fromMethod, fromFunction, Method

# This will be monkey-patched when running under Zope 2, so leave this
# here:
MethodTypes = (MethodType, )


def _verify(iface, candidate, tentative=0, vtype=None):
    """Verify that 'candidate' might correctly implements 'iface'.

    This involves:

      o Making sure the candidate defines all the necessary methods

      o Making sure the methods have the correct signature

      o Making sure the candidate asserts that it implements the interface

    Note that this isn't the same as verifying that the class does
    implement the interface.

    If optional tentative is true, suppress the "is implemented by" test.
    """

    if vtype == 'c':
        tester = iface.implementedBy
    else:
        tester = iface.providedBy

    if not tentative and not tester(candidate):
        raise DoesNotImplement(iface)

    for n, d in iface.namesAndDescriptions(1):
        if not hasattr(candidate, n):
            if (not isinstance(d, Method)) and vtype == 'c':
                # We can't verify non-methods on classes, since the
                # class may provide attrs in it's __init__.
                continue
            
            raise BrokenImplementation(iface, n)

        attr = getattr(candidate, n)
        if type(attr) is FunctionType:
            # should never get here
            meth = fromFunction(attr, n)
        elif (isinstance(attr, MethodTypes)
              and type(attr.im_func) is FunctionType):
            meth = fromMethod(attr, n)
        else:
            continue # must be an attribute...

        d=d.getSignatureInfo()
        meth = meth.getSignatureInfo()

        mess = _incompat(d, meth)
        if mess:
            raise BrokenMethodImplementation(n, mess)

    return True

def verifyClass(iface, candidate, tentative=0):
    return _verify(iface, candidate, tentative, vtype='c')

def verifyObject(iface, candidate, tentative=0):
    return _verify(iface, candidate, tentative, vtype='o')

def _incompat(required, implemented):
    #if (required['positional'] !=
    #    implemented['positional'][:len(required['positional'])]
    #    and implemented['kwargs'] is None):
    #    return 'imlementation has different argument names'
    if len(implemented['required']) > len(required['required']):
        return 'implementation requires too many arguments'
    if ((len(implemented['positional']) < len(required['positional']))
        and not implemented['varargs']):
        return "implementation doesn't allow enough arguments"
    if required['kwargs'] and not implemented['kwargs']:
        return "implementation doesn't support keyword arguments"
    if required['varargs'] and not implemented['varargs']:
        return "implementation doesn't support variable arguments"
