##############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
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
"""Broken object support

$Id: broken.py,v 1.4 2004/04/19 21:19:05 tim_one Exp $
"""

import sys
import persistent

broken_cache = {}

class Broken(object):
    """Broken object base class

       Broken objects are placeholders for objects that can no longer be
       created because their class has gone away.

       Broken objects don't really do much of anything, except hold their
       state.   The Broken class is used as a base class for creating
       classes in leu of missing classes::

         >>> Atall = type('Atall', (Broken, ), {'__module__': 'not.there'})

       The only thing the class can be used for is to create new objects::

         >>> Atall()
         <broken not.there.Atall instance>
         >>> Atall().__Broken_newargs__
         ()
         >>> Atall().__Broken_initargs__
         ()

         >>> Atall(1, 2).__Broken_newargs__
         (1, 2)
         >>> Atall(1, 2).__Broken_initargs__
         (1, 2)

         >>> a = Atall.__new__(Atall, 1, 2)
         >>> a
         <broken not.there.Atall instance>
         >>> a.__Broken_newargs__
         (1, 2)
         >>> a.__Broken_initargs__

       You can't modify broken objects::

         >>> a.x = 1
         Traceback (most recent call last):
         ...
         BrokenModified: Can't change broken objects

       But you can set their state::

         >>> a.__setstate__({'x': 1, })

       You can pickle broken objects::

         >>> r = a.__reduce__()
         >>> len(r)
         3
         >>> r[0] is rebuild
         True
         >>> r[1]
         ('not.there', 'Atall', 1, 2)
         >>> r[2]
         {'x': 1}

         >>> import cPickle
         >>> a2 = cPickle.loads(cPickle.dumps(a, 1))
         >>> a2
         <broken not.there.Atall instance>
         >>> a2.__Broken_newargs__
         (1, 2)
         >>> a2.__Broken_initargs__
         >>> a2.__Broken_state__
         {'x': 1}

       Cleanup::

         >>> broken_cache.clear()
       """

    __Broken_state__ = __Broken_initargs__ = None

    __name__ = 'bob XXX'

    def __new__(class_, *args):
        result = object.__new__(class_)
        result.__dict__['__Broken_newargs__'] = args
        return result

    def __init__(self, *args):
        self.__dict__['__Broken_initargs__'] = args

    def __reduce__(self):
        """We pickle broken objects in hope of being able to fix them later
        """
        return (rebuild,
                ((self.__class__.__module__, self.__class__.__name__)
                 + self.__Broken_newargs__),
                self.__Broken_state__,
                )

    def __setstate__(self, state):
        self.__dict__['__Broken_state__'] = state

    def __repr__(self):
        return "<broken %s.%s instance>" % (
            self.__class__.__module__, self.__class__.__name__)

    def __setattr__(self, name, value):
        raise BrokenModified("Can't change broken objects")

def find_global(modulename, globalname,
                # These are *not* optimizations. Callers can override these.
                Broken=Broken, type=type,
                ):
    """Find a global object, returning a broken class if it can't be found.

       This function looks up global variable in modules::

         >>> import sys
         >>> find_global('sys', 'path') is sys.path
         True

       If an object can't be found, a broken class is returned::

         >>> broken = find_global('ZODB.not.there', 'atall')
         >>> issubclass(broken, Broken)
         True
         >>> broken.__module__
         'ZODB.not.there'
         >>> broken.__name__
         'atall'

       Broken classes are cached::

         >>> find_global('ZODB.not.there', 'atall') is broken
         True

       If we "repair" a missing global::

         >>> class ZODBnotthere:
         ...     atall = []

         >>> sys.modules['ZODB.not'] = ZODBnotthere
         >>> sys.modules['ZODB.not.there'] = ZODBnotthere

       we can then get the repaired value::

         >>> find_global('ZODB.not.there', 'atall') is ZODBnotthere.atall
         True

       Of course, if we beak it again::

         >>> del sys.modules['ZODB.not']
         >>> del sys.modules['ZODB.not.there']

       we get the broken value::

         >>> find_global('ZODB.not.there', 'atall') is broken
         True

       Cleanup::

         >>> broken_cache.clear()
       """
    try:
        __import__(modulename)
    except ImportError:
        pass
    else:
        module = sys.modules[modulename]
        try:
            return getattr(module, globalname)
        except AttributeError:
            pass

    try:
        return broken_cache[(modulename, globalname)]
    except KeyError:
        pass

    class_ = type(globalname, (Broken, ), {'__module__': modulename})
    broken_cache[(modulename, globalname)] = class_
    return class_

def rebuild(modulename, globalname, *args):
    """Recreate a broken object, possibly recreating the missing class

       This functions unpickles broken objects::

         >>> broken = rebuild('ZODB.notthere', 'atall', 1, 2)
         >>> broken
         <broken ZODB.notthere.atall instance>
         >>> broken.__Broken_newargs__
         (1, 2)

       If we "repair" the brokenness::

         >>> class notthere: # fake notthere module
         ...     class atall(object):
         ...         def __new__(self, *args):
         ...             ob = object.__new__(self)
         ...             ob.args = args
         ...             return ob
         ...         def __repr__(self):
         ...             return 'atall %s %s' % self.args

         >>> sys.modules['ZODB.notthere'] = notthere

         >>> rebuild('ZODB.notthere', 'atall', 1, 2)
         atall 1 2

         >>> del sys.modules['ZODB.notthere']

       Cleanup::

         >>> broken_cache.clear()

       """
    class_ = find_global(modulename, globalname)
    return class_.__new__(class_, *args)

class BrokenModified(TypeError):
    """Attempt to modify a broken object
    """

class PersistentBroken(Broken, persistent.Persistent):
    r"""Persistent broken objects

        Persistent broken objects are used for broken objects that are
        also persistent.  In addition to having to track the original
        object data, they need to handle persistent meta data.

        Persistent broken classes are created from existing broken classes
        using the persistentBroken, function::

          >>> Atall = type('Atall', (Broken, ), {'__module__': 'not.there'})
          >>> PAtall = persistentBroken(Atall)

        (Note that we always get the *same* persistent broken class
         for a given broken class::

          >>> persistentBroken(Atall) is PAtall
          True

         )

        Persistent broken classes work a lot like broken classes::

          >>> a = PAtall.__new__(PAtall, 1, 2)
          >>> a
          <persistent broken not.there.Atall instance None>
          >>> a.__Broken_newargs__
          (1, 2)
          >>> a.__Broken_initargs__
          >>> a.x = 1
          Traceback (most recent call last):
          ...
          BrokenModified: Can't change broken objects

        Unlike regular broken objects, persistent broken objects keep
        track of persistence meta data:

          >>> a._p_oid = '\0\0\0\0****'
          >>> a
          <persistent broken not.there.Atall instance '\x00\x00\x00\x00****'>

        and persistent broken objects aren't directly picklable:

          >>> a.__reduce__()
          Traceback (most recent call last):
          ...
          BrokenModified: """ \
        r"""<persistent broken not.there.Atall instance '\x00\x00\x00\x00****'>

        but you can get their state:

          >>> a.__setstate__({'y': 2})
          >>> a.__getstate__()
          {'y': 2}

       Cleanup::

         >>> broken_cache.clear()

        """

    def __new__(class_, *args):
        result = persistent.Persistent.__new__(class_)
        result.__dict__['__Broken_newargs__'] = args
        return result

    def __reduce__(self, *args):
        raise BrokenModified(self)

    def __getstate__(self):
        return self.__Broken_state__

    def __setattr__(self, name, value):
        if name.startswith('_p_'):
            persistent.Persistent.__setattr__(self, name, value)
        else:
            raise BrokenModified("Can't change broken objects")

    def __repr__(self):
        return "<persistent broken %s.%s instance %r>" % (
            self.__class__.__module__, self.__class__.__name__,
            self._p_oid)

    def __getnewargs__(self):
        return self.__Broken_newargs__

def persistentBroken(class_):
    try:
        return class_.__dict__['__Broken_Persistent__']
    except KeyError:
        class_.__Broken_Persistent__ = (
            type(class_.__name__,
                 (PersistentBroken, class_),
                 {'__module__': class_.__module__},
                 )
            )
        return class_.__dict__['__Broken_Persistent__']
