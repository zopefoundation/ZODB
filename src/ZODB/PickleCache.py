##############################################################################
#
# Copyright (c) 1996-1998, Digital Creations, Fredericksburg, VA, USA.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
#   o Redistributions of source code must retain the above copyright
#     notice, this list of conditions, and the disclaimer that follows.
# 
#   o Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions, and the following disclaimer in
#     the documentation and/or other materials provided with the
#     distribution.
# 
#   o Neither the name of Digital Creations nor the names of its
#     contributors may be used to endorse or promote products derived
#     from this software without specific prior written permission.
# 
# 
# THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS AND CONTRIBUTORS *AS IS*
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
# PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL
# CREATIONS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
# TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
# USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
# DAMAGE.
#
# 
# If you have questions regarding this software, contact:
#
#   Digital Creations, L.C.
#   910 Princess Ann Street
#   Fredericksburge, Virginia  22401
#
#   info@digicool.com
#
#   (540) 371-6909
#
##############################################################################
__doc__='''PickleJar Object Cache

$Id: PickleCache.py,v 1.4 1998/10/23 21:41:24 jim Exp $'''
__version__='$Revision: 1.4 $'[11:-2]
        
from sys import getrefcount

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

    def __len__(self): return len(self.data)

    def values(self): return self.data.values()

    def full_sweep(self):
        cache=self.data
        for id in cache.keys():
            if getrefcount(cache[id]) <= 2: del cache[id]

    def minimize(self):
        cache=self.data
        keys=cache.keys()
        rc=getrefcount
        last=None
        l=len(cache)
        while l != last:
            for id in keys():
                if rc(cache[id]) <= 2: del cache[id]
                cache[id]._p_deactivate()
            l=len(cache)
