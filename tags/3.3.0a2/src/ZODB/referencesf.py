##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Provide a function that can find object references in pickles
"""
import cPickle, cStringIO

def referencesf(p, rootl=None,
                Unpickler=cPickle.Unpickler,
                StringIO=cStringIO.StringIO,
                tt=type(()),
                type=type):

    if rootl is None: rootl=[]
    u=Unpickler(StringIO(p))
    l=len(rootl)
    u.persistent_load=rootl
    u.noload()
    try: u.noload()
    except:
        # Hm.  We failed to do second load.  Maybe there wasn't a
        # second pickle.  Let's check:
        f=StringIO(p)
        u=Unpickler(f)
        u.persistent_load=[]
        u.noload()
        if len(p) > f.tell(): raise ValueError, 'Error unpickling, %s' % p

    # References may have class info, so we need to
    # check for wrapped references.
    for i in range(l, len(rootl)):
        v=rootl[i]
        if v:
            if type(v) is tt: v=v[0]
            rootl[i]=v

    return rootl
