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
"""Overriding attr methods

This module tests and documents, through example, overriding attribute
access methods.

$Id: test_overriding_attrs.py,v 1.4 2004/03/02 22:17:43 jeremy Exp $
"""

from persistent import Persistent
try:
    from transaction import get_transaction
except ImportError:
    pass # else assume ZODB will install it as a builtin
from ZODB.tests.util import DB

class SampleOverridingGetattr(Persistent):
    """Example of overriding __getattr__
    """
    
    def __getattr__(self, name):
        """Get attributes that can't be gotten the usual way

        The __getattr__ method works pretty much the same for persistent
        classes as it does for other classes.  No special handling is
        needed.  If an object is a ghost, then it will be activated before
        __getattr__ is called.

        In this example, our objects returns a tuple with the attribute
        name, converted to upper case and the value of _p_changed, for any
        attribute that isn't handled by the default machinery.

        >>> o = SampleOverridingGetattr()
        >>> o._p_changed
        0
        >>> o._p_oid
        >>> o._p_jar
        >>> o.spam
        ('SPAM', False)
        >>> o.spam = 1
        >>> o.spam
        1

        We'll save the object, so it can be deactivated:

        >>> db = DB()
        >>> conn = db.open()
        >>> conn.root()['o'] = o
        >>> get_transaction().commit()
        >>> o._p_deactivate()
        >>> o._p_changed

        And now, if we ask for an attribute it doesn't have, 

        >>> o.eggs
        ('EGGS', False)

        And we see that the object was activated before calling the
        __getattr__ method.

        We always close databases after we use them:

        >>> db.close()
        """
        # Don't pretend we have any special attributes.
        if name.startswith("__") and name.endswrith("__"):
            raise AttributeError, name
        else:
            return name.upper(), self._p_changed

class SampleOverridingGetattributeSetattrAndDelattr(Persistent):
    """Example of overriding __getattribute__, __setattr__, and __delattr__

    In this example, we'll provide an example that shows how to
    override the __getattribute__, __setattr__, and __delattr__
    methods.  We'll create a class that stores it's attributes in a
    secret dictionary within it's instance dictionary.

    The class will have the policy that variables with names starting
    with 'tmp_' will be volatile.
    
    """

    def __init__(self, **kw):
        self.__dict__['__secret__'] = kw.copy()

    def __getattribute__(self, name):
        """Get an attribute value

        The __getattribute__ method is called for all attribute
        accesses.  It overrides the attribute access support inherited
        from Persistent.

        Our sample class let's us provide initial values as keyword
        arguments to the constructor:

        >>> o = SampleOverridingGetattributeSetattrAndDelattr(x=1)
        >>> o._p_changed
        0
        >>> o._p_oid
        >>> o._p_jar        
        >>> o.x
        1
        >>> o.y
        Traceback (most recent call last):
        ...
        AttributeError: y

        Next, we'll save the object in a database so that we can
        deactivate it: 

        >>> db = DB()
        >>> conn = db.open()
        >>> conn.root()['o'] = o
        >>> get_transaction().commit()
        >>> o._p_deactivate()
        >>> o._p_changed

        And we'll get some data:

        >>> o.x
        1

        which activates the object:

        >>> o._p_changed
        0

        It works for missing attribes too:
        
        >>> o._p_deactivate()
        >>> o._p_changed
        
        >>> o.y
        Traceback (most recent call last):
        ...
        AttributeError: y

        >>> o._p_changed
        0

        See the very important note in the comment below!

        We always close databases after we use them:

        >>> db.close()
        """

        #################################################################
        # IMPORTANT! READ THIS! 8->
        #
        # We *always* give Persistent a chance first.
        # Persistent handles certain special attributes, like _p_
        # attributes. In particular, the base class handles __dict__
        # and __class__.
        #
        # We call _p_getattr. If it returns True, then we have to
        # use Persistent.__getattribute__ to get the value.
        #
        #################################################################
        if Persistent._p_getattr(self, name):
            return Persistent.__getattribute__(self, name)

        # Data should be in our secret dictionary:
        secret = self.__dict__['__secret__']
        if name in secret:
            return secret[name]

        # Maybe it's a method:
        meth = getattr(self.__class__, name, None)
        if meth is None:
            raise AttributeError, name
        
        return meth.__get__(self, self.__class__)
        

    def __setattr__(self, name, value):
        """Set an attribute value

        The __setattr__ method is called for all attribute
        assignments.  It overrides the attribute assignment support
        inherited from Persistent.

        Implementors of __setattr__ methods:

        1. Must call Persistent._p_setattr first to allow it
           to handle some attributes and to make sure that the object
           is activated if necessary, and

        2. Must set _p_changed to mark objects as changed.

        See the comments in the source below.

        >>> o = SampleOverridingGetattributeSetattrAndDelattr()
        >>> o._p_changed
        0
        >>> o._p_oid
        >>> o._p_jar
        >>> o.x
        Traceback (most recent call last):
        ...
        AttributeError: x

        >>> o.x = 1
        >>> o.x
        1

        Because the implementation doesn't store attributes directly
        in the instance dictionary, we don't have a key for the attribute:

        >>> 'x' in o.__dict__
        False
        
        Next, we'll save the object in a database so that we can
        deactivate it: 

        >>> db = DB()
        >>> conn = db.open()
        >>> conn.root()['o'] = o
        >>> get_transaction().commit()
        >>> o._p_deactivate()
        >>> o._p_changed

        We'll modify an attribute

        >>> o.y = 2
        >>> o.y
        2

        which reactivates it, and markes it as modified, because our
        implementation marked it as modified:

        >>> o._p_changed
        1

        Now, if commit:
        
        >>> get_transaction().commit()
        >>> o._p_changed
        0

        And deactivate the object:

        >>> o._p_deactivate()
        >>> o._p_changed

        and then set a variable with a name starting with 'tmp_',
        The object will be activated, but not marked as modified,
        because our __setattr__ implementation  doesn't mark the
        object as changed if the name starts with 'tmp_':

        >>> o.tmp_foo = 3
        >>> o._p_changed
        0
        >>> o.tmp_foo
        3
        
        We always close databases after we use them:

        >>> db.close()

        """

        #################################################################
        # IMPORTANT! READ THIS! 8->
        #
        # We *always* give Persistent a chance first.
        # Persistent handles certain special attributes, like _p_
        # attributes.
        #
        # We call _p_setattr. If it returns True, then we are done.
        # It has already set the attribute.
        #
        #################################################################
        if Persistent._p_setattr(self, name, value):
            return

        self.__dict__['__secret__'][name] = value

        if not name.startswith('tmp_'):
            self._p_changed = 1
        
    def __delattr__(self, name):
        """Delete an attribute value

        The __delattr__ method is called for all attribute
        deletions.  It overrides the attribute deletion support
        inherited from Persistent.

        Implementors of __delattr__ methods:

        1. Must call Persistent._p_delattr first to allow it
           to handle some attributes and to make sure that the object
           is activated if necessary, and

        2. Must set _p_changed to mark objects as changed.

        See the comments in the source below.

        >>> o = SampleOverridingGetattributeSetattrAndDelattr(
        ...         x=1, y=2, tmp_z=3)
        >>> o._p_changed
        0
        >>> o._p_oid
        >>> o._p_jar
        >>> o.x
        1
        >>> del o.x
        >>> o.x
        Traceback (most recent call last):
        ...
        AttributeError: x

        Next, we'll save the object in a database so that we can
        deactivate it: 

        >>> db = DB()
        >>> conn = db.open()
        >>> conn.root()['o'] = o
        >>> get_transaction().commit()
        >>> o._p_deactivate()
        >>> o._p_changed

        If we delete an attribute:

        >>> del o.y

        The object is activated.  It is also marked as changed because
        our implementation marked it as changed.

        >>> o._p_changed
        1
        >>> o.y
        Traceback (most recent call last):
        ...
        AttributeError: y

        >>> o.tmp_z
        3

        Now, if commit:
        
        >>> get_transaction().commit()
        >>> o._p_changed
        0

        And deactivate the object:

        >>> o._p_deactivate()
        >>> o._p_changed

        and then delete a variable with a name starting with 'tmp_',
        The object will be activated, but not marked as modified,
        because our __delattr__ implementation  doesn't mark the
        object as changed if the name starts with 'tmp_':

        >>> del o.tmp_z
        >>> o._p_changed
        0
        >>> o.tmp_z
        Traceback (most recent call last):
        ...
        AttributeError: tmp_z
        
        We always close databases after we use them:

        >>> db.close()

        """

        #################################################################
        # IMPORTANT! READ THIS! 8->
        #
        # We *always* give Persistent a chance first.
        # Persistent handles certain special attributes, like _p_
        # attributes.
        #
        # We call _p_delattr. If it returns True, then we are done.
        # It has already deleted the attribute.
        #
        #################################################################
        if Persistent._p_delattr(self, name):
            return

        del self.__dict__['__secret__'][name]
        
        if not name.startswith('tmp_'):
            self._p_changed = 1
                    

def test_suite():
    from doctest import DocTestSuite
    return DocTestSuite()
