===================
Persistence support
===================

(This document is under construction. More basic documentation will
 eventually appear here.)


Overriding __getattr__, __getattribute__, __setattr__, and __delattr__
-----------------------------------------------------------------------  

Subclasses can override the attribute-management methods.  For the
__getattr__ method, the behavior is like that for regular Python
classes and for earlier versions of ZODB 3.

For __getattribute__, __setattr__, and __delattr__, it is necessary to
cal certain methods defined by persistent.Persistent.  Detailed
examples and documentation is provided in the test module,
persistent.tests.test_overriding_attrs.




  
