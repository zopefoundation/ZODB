#!/usr/local/bin/python 
# $What$

__doc__='''Python implementation of persistent base types


$Id: mapping.py,v 1.1 1997/12/15 17:51:33 jim Exp $'''
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
__version__='$Revision: 1.1 $'[11:-2]

import Persistence
	
class PersistentMapping(Persistence.Persistent):
    """A persistent wrapper for mapping objects.

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
	try: del self._v_keys
	except: pass
	self.__changed__(1)

    def __delitem__(self, key):
	del self._container[key]
	try: del self._v_keys
	except: pass
	self.__changed__(1)

    def __len__(self):     return len(self._container)

    def keys(self):
	try: return self._v_keys
	except: pass
	keys=self._v_keys=filter(
	    lambda k: k[:1]!='_',
	    self._container.keys())
	keys.sort()
	return keys

    def clear(self):
	self._container={}
	if hasattr(self,'_v_keys'): del self._v_keys

    def values(self):
	return map(lambda k, d=self: d[k], self.keys())

    def items(self):
	return map(lambda k, d=self: (k,d[k]), self.keys())

    def has_key(self,key): return self._container.has_key(key)

############################################################################
# $Log: mapping.py,v $
# Revision 1.1  1997/12/15 17:51:33  jim
# Split off from Persistence.
#
