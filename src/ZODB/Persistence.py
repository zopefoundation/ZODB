#!/usr/local/bin/python 
# $What$

__doc__='''Python implementation of persistent base types


$Id: Persistence.py,v 1.5 1997/03/25 20:42:42 jim Exp $'''
#     Copyright 
#
#       Copyright 1996 Digital Creations, L.C., 910 Princess Anne
#       Street, Suite 300, Fredericksburg, Virginia 22401 U.S.A. All
#       rights reserved.  Copyright in this software is owned by DCLC,
#       unless otherwise indicated. Permission to use, copy and
#       distribute this software is hereby granted, provided that the
#       above copyright notice appear in all copies and that both that
#       copyright notice and this permission notice appear. Note that
#       any product, process or technology described in this software
#       may be the subject of other Intellectual Property rights
#       reserved by Digital Creations, L.C. and are not licensed
#       hereunder.
#
#     Trademarks 
#
#       Digital Creations & DCLC, are trademarks of Digital Creations, L.C..
#       All other trademarks are owned by their respective companies. 
#
#     No Warranty 
#
#       The software is provided "as is" without warranty of any kind,
#       either express or implied, including, but not limited to, the
#       implied warranties of merchantability, fitness for a particular
#       purpose, or non-infringement. This software could include
#       technical inaccuracies or typographical errors. Changes are
#       periodically made to the software; these changes will be
#       incorporated in new editions of the software. DCLC may make
#       improvements and/or changes in this software at any time
#       without notice.
#
#     Limitation Of Liability 
#
#       In no event will DCLC be liable for direct, indirect, special,
#       incidental, economic, cover, or consequential damages arising
#       out of the use of or inability to use this software even if
#       advised of the possibility of such damages. Some states do not
#       allow the exclusion or limitation of implied warranties or
#       limitation of liability for incidental or consequential
#       damages, so the above limitation or exclusion may not apply to
#       you.
#  
#
# If you have questions regarding this software,
# contact:
#
#   Digital Creations, L.C.
#   910 Princess Ann Street
#   Fredericksburge, Virginia  22401
#
#   info@digicool.com
#
#   (540) 371-6909
#
# $Log: Persistence.py,v $
# Revision 1.5  1997/03/25 20:42:42  jim
# Changed to make all persistent objects transactional.
#
# Revision 1.4  1997/03/14 16:19:55  jim
# Changed so no longer save on del.
# Added check in __save__ so that we don't save if we have decided that
# we haven't changed.
#
# Revision 1.3  1997/03/08 22:03:54  jfulton
# Paul made change to clear method to see if keys exist.
#
# Revision 1.2  1997/03/05 22:59:48  jim
# Added clear method.
#
# Revision 1.1  1997/02/11 13:14:06  jim
# *** empty log message ***
#
#
# 
__version__='$Revision: 1.5 $'[11:-2]

class Persistent:
    """\
    Persistent object support mix-in class

    When a persistent object is loaded from a database, the object's
    data is not immediately loaded.  Loading of the objects data is
    defered until an attempt is made to access an attribute of the
    object. 

    The object also tries to keep track of whether it has changed.  It
    is easy for this to be done incorrectly.  For this reason, methods
    of subclasses that change state other than by setting attributes
    should: 'self.__changed__(1)' to flag instances as changed.

    Data are not saved automatically.  To save an object's state, call
    the object's '__save__' method.

    You must not override the object's '__getattr__' and '__setattr__'
    methods.  If you override the objects '__getstate__' method, then
    you must be careful not to include any attributes with names
    starting with '_p_' in the state.

    """ 
    _p_oid=None	       # A Persistent object-id, unique within a jar
    _p_changed=None    # A flag indicating whether the object has changed
    _p_read_time=0     # The time when the object was read.
    _p_jar=None        # The last jar that this object was stored in.
    
    def __getattr__(self,key):
	' '
	try: setstate=self.__dict__['_p_setstate']
	except: raise AttributeError, key
	setstate(self)
	try: return self.__dict__[key]
	except: raise AttributeError, key

    def _p___init__(self,oid,jar):
	d=self.__dict__
	if self._p_oid is None:
	    d['_p_oid']=oid
	if self._p_oid==oid:
	    newstate={}
	    for key in d.keys():
		if key[:3] != '_p_':
		    newstate[key]=d[key]
		    del d[key]		
	    d['_p_newstate']=newstate
	    d['_p_jar']=jar
	    d['_p_setstate']=jar.setstate
	    d['_p_changed']=0

    def _p___reinit__(self,oid,jar,copy):
	d=self.__dict__
	cd=copy.__dict__
	if self._p_oid is None: d['_p_oid']=oid
	if self._p_oid==oid:
	    newstate={}
	    for key in cd.keys():
		if key[:3] != '_p_': newstate[key]=cd[key]
	    for key in d.keys():
		if key[:3] != '_p_': del d[key]
	    d['_p_newstate']=newstate
	    d['_p_jar']=jar
	    d['_p_setstate']=jar.setstate
	    d['_p_changed']=0

    def __setattr__(self,key,value):
	' '
	if key[:3]=='_p_':
	    self.__dict__[key]=value
	    return
    
	try:
	    setstate=self.__dict__['_p_setstate']
	    try: setstate(self)
	    except: raise TypeError, (sys.exc_type, sys.exc_value,
				      sys.exc_traceback)
	except KeyError: pass
	except TypeError, v: raise v[0], v[1], v[2]

	self.__dict__[key]=value
	self.__changed__(1)

    def __repr__(self):
	' '
	return '<%s instance at %s>' % (self.__class__.__name__,hex(id(self)))
    
    def __getstate__(self):
	
	# First, update my state, if necessary:
	try:
	    setstate=self.__dict__['_p_setstate']
	    try: setstate(self)
	    except: raise TypeError, (sys.exc_type, sys.exc_value,
				      sys.exc_traceback)
	except KeyError: pass
	except TypeError, v: raise v[0], v[1], v[2]

	state={}
	d=self.__dict__
	for k in d.keys():
	    if k[:3] != '_p_':
		state[k]=d[k]
	return state

    def __setstate__(self,state):
	d=self.__dict__
	for k in state.keys():
	    d[k]=state[k]
	return state

    def __save__(self):
	'''\
	Update the object in a persistent database.
	'''
	jar=self._p_jar
	if jar and self._p_changed: jar.store(self)

    def __changed__(self,value=None):
	'''\
	Flag or determine whether an object has changed
	
	If a value is specified, then it should indicate whether an
	object has or has not been changed.  If no value is specified,
	then the return value will indicate whether the object has
	changed.
	'''	
	if value is not None:
	    self.__dict__['_p_changed']=value
	else:
	    return self._p_changed

    # The following was copied from the SingleThreadedTransaction module:
    #
    # Base class for all transactional objects
    # Transactional objects, like persistent objects track
    # changes in state.  Unlike persistent objects, transactional
    # objects work in conjunction with a transaction manager to manage
    # saving state and recovering from errors.
    #

    def __changed__(self,v=None):
	if v and not self._p_changed and self._p_jar is not None:
	    try: get_transaction().register(self)
	    except: pass
	return Persistence.Persistent.__changed__(self,v)

    def __inform_commit__(self,transaction,start_time):
	self.__save__()

    def __inform_abort__(self,transaction,start_time):
	try: self._p_jar.oops(self,start_time)
	except: pass


try:
    import cPersistence
    from cPersistence import Persistent
except: pass
	
class PersistentMapping(Persistent):
    """\
    A persistent wrapper for mapping objects.

    This class allows wrapping of mapping objects so that
    object changes are registered.  As a side effect,
    mapping objects may be subclassed.
    """

    def __init__(self,container=None):
	if container is None: container={}
	self._container=container

    def __getitem__(self, key):
	return self._container[key]

    def __setitem__(self, key, v):
	self._container[key]=v
	try: del self._keys
	except: pass
	self.__changed__(1)

    def __delitem__(self, key):
	del self._container[key]
	try: del self._keys
	except: pass
	self.__changed__(1)

    def __len__(self):     return len(self._container)

    def keys(self):
	try: return self._keys
	except: pass
	keys=self._keys=filter(
	    lambda k: k[:1]!='_',
	    self._container.keys())
	keys.sort()
	return keys

    def clear(self):
	self._container={}
	if hasattr(self,'_keys'): del self._keys

    def values(self):
	return map(lambda k, d=self: d[k], self.keys())

    def items(self):
	return map(lambda k, d=self: (k,d[k]), self.keys())

    def has_key(self,key): return self._container.has_key(key)

