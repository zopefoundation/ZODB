#!/usr/local/bin/python 
# $What$

__doc__='''PickleJar Object Cache

$Id: PickleCache.py,v 1.1 1997/12/15 17:51:55 jim Exp $'''
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
#   Digital Creations, bobo@digicool.com
#
#   (540) 371-6909
# 
__version__='$Revision: 1.1 $'[11:-2]
	
class PickleCache:

    def __init__(self, cache_size, cache_age=1000):
        if cache_size < 1: cache_size=1
        self.cache_size=cache_size
        self.data, self.cache_ids, self.cache_location ={}, [], 0
	for a in 'keys', 'items', 'values', 'has_key':
	    setattr(self,a,getattr(self.data,a))


    def __getitem__(self, key):
        cache=self.data
        v=cache[key]

        # Do cache GC
        n=min(len(cache)/self.cache_size,10)
        if n:
            l=self.cache_location
            ids=self.cache_ids
            while n:
                if not l:
                    ids=self.cache_ids=cache.keys()
                    l=len(ids)
                l=l-1
                n=n-1
                id=ids[l]
                if getrefcount(cache[id]) <= 2:
                    del cache[id]
            self.cache_location=l

        return v

    def __setitem__(self, key, v): self.data[key]=v

    def __delitem__(self, key): del self.data[key]

    def values(self): return self.data.values()

    def full_sweep(self):
        cache=self.data
        for id in cache.keys():
            if getrefcount(cache[id]) <= 2: del cache[id]
    
    minimize=full_sweep

############################################################################
#
# $Log: PickleCache.py,v $
# Revision 1.1  1997/12/15 17:51:55  jim
# Split off from PickleJar.
#
#
#
