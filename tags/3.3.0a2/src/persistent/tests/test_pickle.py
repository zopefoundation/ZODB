##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
"""Basic pickling tests

$Id: test_pickle.py,v 1.3 2003/12/29 22:40:50 tim_one Exp $
"""

from persistent import Persistent
import pickle


def print_dict(d):
    d = d.items()
    d.sort()
    print '{%s}' % (', '.join(
        [('%r: %r' % (k, v)) for (k, v) in d]
        ))

def cmpattrs(self, other, *attrs):
    for attr in attrs:
        if attr[:3] in ('_v_', '_p_'):
            continue
        c = cmp(getattr(self, attr, None), getattr(other, attr, None))
        if c:
            return c
    return 0

class Simple(Persistent):
    def __init__(self, name, **kw):
        self.__name__ = name
        self.__dict__.update(kw)
        self._v_favorite_color = 'blue'
        self._p_foo = 'bar'

    def __cmp__(self, other):
        return cmpattrs(self, other, '__class__', *(self.__dict__.keys()))

def test_basic_pickling():
    """
    >>> x = Simple('x', aaa=1, bbb='foo')

    >>> x.__getnewargs__()
    ()

    >>> print_dict(x.__getstate__())
    {'__name__': 'x', 'aaa': 1, 'bbb': 'foo'}

    >>> f, (c,), state = x.__reduce__()
    >>> f.__name__
    '__newobj__'
    >>> f.__module__
    'copy_reg'
    >>> c.__name__
    'Simple'

    >>> print_dict(state)
    {'__name__': 'x', 'aaa': 1, 'bbb': 'foo'}

    >>> pickle.loads(pickle.dumps(x)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 0)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 1)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 2)) == x
    1

    >>> x.__setstate__({'z': 1})
    >>> x.__dict__
    {'z': 1}

    """

class Custom(Simple):

    def __new__(cls, x, y):
        r = Persistent.__new__(cls)
        r.x, r.y = x, y
        return r

    def __init__(self, x, y):
        self.a = 42

    def __getnewargs__(self):
        return self.x, self.y

    def __getstate__(self):
        return self.a

    def __setstate__(self, a):
        self.a = a


def test_pickling_w_overrides():
    """
    >>> x = Custom('x', 'y')
    >>> x.a = 99

    >>> (f, (c, ax, ay), a) = x.__reduce__()
    >>> f.__name__
    '__newobj__'
    >>> f.__module__
    'copy_reg'
    >>> c.__name__
    'Custom'
    >>> ax, ay, a
    ('x', 'y', 99)

    >>> pickle.loads(pickle.dumps(x)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 0)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 1)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 2)) == x
    1

    """

class Slotted(Persistent):
    __slots__ = 's1', 's2', '_p_splat', '_v_eek'
    def __init__(self, s1, s2):
        self.s1, self.s2 = s1, s2
        self._v_eek = 1
        self._p_splat = 2

class SubSlotted(Slotted):
    __slots__ = 's3', 's4'
    def __init__(self, s1, s2, s3):
        Slotted.__init__(self, s1, s2)
        self.s3 = s3


    def __cmp__(self, other):
        return cmpattrs(self, other, '__class__', 's1', 's2', 's3', 's4')


def test_pickling_w_slots_only():
    """
    >>> x = SubSlotted('x', 'y', 'z')

    >>> x.__getnewargs__()
    ()

    >>> d, s = x.__getstate__()
    >>> d
    >>> print_dict(s)
    {'s1': 'x', 's2': 'y', 's3': 'z'}

    >>> pickle.loads(pickle.dumps(x)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 0)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 1)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 2)) == x
    1

    >>> x.s4 = 'spam'

    >>> d, s = x.__getstate__()
    >>> d
    >>> print_dict(s)
    {'s1': 'x', 's2': 'y', 's3': 'z', 's4': 'spam'}

    >>> pickle.loads(pickle.dumps(x)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 0)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 1)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 2)) == x
    1

    """

class SubSubSlotted(SubSlotted):

    def __init__(self, s1, s2, s3, **kw):
        SubSlotted.__init__(self, s1, s2, s3)
        self.__dict__.update(kw)
        self._v_favorite_color = 'blue'
        self._p_foo = 'bar'

    def __cmp__(self, other):
        return cmpattrs(self, other,
                        '__class__', 's1', 's2', 's3', 's4',
                        *(self.__dict__.keys()))

def test_pickling_w_slots():
    """
    >>> x = SubSubSlotted('x', 'y', 'z', aaa=1, bbb='foo')

    >>> x.__getnewargs__()
    ()

    >>> d, s = x.__getstate__()
    >>> print_dict(d)
    {'aaa': 1, 'bbb': 'foo'}
    >>> print_dict(s)
    {'s1': 'x', 's2': 'y', 's3': 'z'}

    >>> pickle.loads(pickle.dumps(x)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 0)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 1)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 2)) == x
    1

    >>> x.s4 = 'spam'

    >>> d, s = x.__getstate__()
    >>> print_dict(d)
    {'aaa': 1, 'bbb': 'foo'}
    >>> print_dict(s)
    {'s1': 'x', 's2': 'y', 's3': 'z', 's4': 'spam'}

    >>> pickle.loads(pickle.dumps(x)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 0)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 1)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 2)) == x
    1

    """

def test_pickling_w_slots_w_empty_dict():
    """
    >>> x = SubSubSlotted('x', 'y', 'z')

    >>> x.__getnewargs__()
    ()

    >>> d, s = x.__getstate__()
    >>> print_dict(d)
    {}
    >>> print_dict(s)
    {'s1': 'x', 's2': 'y', 's3': 'z'}

    >>> pickle.loads(pickle.dumps(x)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 0)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 1)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 2)) == x
    1

    >>> x.s4 = 'spam'

    >>> d, s = x.__getstate__()
    >>> print_dict(d)
    {}
    >>> print_dict(s)
    {'s1': 'x', 's2': 'y', 's3': 'z', 's4': 'spam'}

    >>> pickle.loads(pickle.dumps(x)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 0)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 1)) == x
    1
    >>> pickle.loads(pickle.dumps(x, 2)) == x
    1

    """

from doctest import DocTestSuite
import unittest

def test_suite():
    return unittest.TestSuite((
        DocTestSuite(),
        ))

if __name__ == '__main__': unittest.main()
