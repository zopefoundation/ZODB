#!/usr/local/bin/python 
# $What$

__doc__='''Python implementation of a persistent base types

$Id: Persistence.py,v 1.13 1998/06/05 22:07:05 jim Exp $'''
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
__version__='$Revision: 1.13 $'[11:-2]

try:
    from cPersistence import Persistent
except: 
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
    
	You must not override the object's '__getattr__' and '__setattr__'
	methods.  If you override the objects '__getstate__' method, then
	you must be careful not to include any attributes with names
	starting with '_p_' or '_v_' in the state.
    
	""" 
	_p_oid=None        # A Persistent object-id, unique within a jar
	_p_changed=0       # The object state: None=ghost, 0=normal, 1=changed
	_p_jar=None        # The last jar that this object was stored in.
    
	def _p___init__(self,oid,jar):
	    """Post creation initialization

	    This is *only* used if we have __getinitargs__!
	    """
	    d=self.__dict__
	    if d:
		newstate={}
		for key in d.keys():
		    if key[:3] != '_p_':
			newstate[key]=d[key]
			del d[key]          
		if newstate: d['_p_newstate']=newstate
		
	    d['_p_oid']=oid
	    d['_p_jar']=jar
	    d['_p_changed']=None
    
	def _p_deactivate(self,copy=None):
	    if copy is None: newstate=None
	    else: newstate=copy.__dict__
	    d=self.__dict__
	    oid=self._p_oid
	    jar=self._p_jar
	    d.clear()
	    if newstate: d['_p_newstate']=newstate
	    d['_p_oid']=oid
	    d['_p_jar']=jar
	    d['_p_changed']=None

	_p___reinit=_p_deactivate # Back. Comp.
	
	def __getattr__(self,key):
	    'Get an item'
	    if self._p_changed is None and key[:3] != '_p_':
		self._p_jar.setstate(self)
		if self.__dict__.has_key(key): return self.__dict__[key]

	    raise AttributeError, key
    
	def __setattr__(self,key,value):
	    ' '
            k=key[:3]
	    if k=='_p_' or k=='_v_':
		self.__dict__[key]=value
		return

	    jar=self._p_jar
	    if self._p_changed is None:	jar.setstate(self)
	    self.__dict__[key]=value
	    if jar is not None:
		try:
		    get_transaction().register(self)
		    self._p_changed=1
		except: pass
    
	def __changed__(self,v=-1):
	    old=self._p_changed
	    if v != -1:
		if v and not old and self._p_jar is not None:
		    try: get_transaction().register(self)
		    except: pass
		self._p_changed = not not v

	    return old
	
	def __getstate__(self):
	    
	    # First, update my state, if necessary:
	    if self._p_changed is None:	self._p_jar.setstate(self)
    
	    state={}
	    d=self.__dict__
	    for k,v in d.items():
		if k[:3] != '_p_' and k[:3] != '_v_': state[k]=v
	    return state
    
	def __setstate__(self,state):
	    d=self.__dict__
	    for k,v in state.items(): d[k]=v
	    return state
    
	def __save__(self):
	    '''\
	    Update the object in a persistent database.
	    '''
	    jar=self._p_jar
	    if jar and self._p_changed: jar.store(self)
    
	def __repr__(self):
	    ' '
	    return '<%s instance at %s>' % (self.__class__.__name__,
					    hex(id(self)))

	def __inform_commit__(self,T,start_time):
	    jar=self._p_jar
	    if jar and self._p_changed: jar.store(self,T)
    
	def __inform_abort__(self,T,start_time):
	    try: self._p_jar.abort(self,start_time)
	    except: pass


############################################################################
# $Log: Persistence.py,v $
# Revision 1.13  1998/06/05 22:07:05  jim
# Fixed bug in Persistent.__setattr__ that caused changes to
# "volatile" attributes (starting with _v_) to cause database writes.
#
# Revision 1.12  1998/03/12 15:38:51  jim
# Fixed bug in __changed__.
#
# Revision 1.11  1997/12/15 23:01:17  jim
# *** empty log message ***
#
# Revision 1.10  1997/10/30 18:49:22  jim
# Changed abort to use jar's abort method.
#
# Revision 1.9  1997/04/22 00:16:50  jim
# Changed to use new cPersistent header.
#
# Revision 1.8  1997/04/04 13:52:27  jim
# Fixed bug in persistent mapping that caused extraneous records to be
# written.
#
# Revision 1.7  1997/04/03 17:33:32  jim
# Changed to pass transaction to jar store method.
#
# Revision 1.6  1997/03/28 23:04:48  jim
# Changed reinit to tolerate being called with no arguments.
#
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
