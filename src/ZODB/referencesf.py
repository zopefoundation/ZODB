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

def referencesf(p, rootl=None,):

    if rootl is None:
        rootl = []

    u = cPickle.Unpickler(cStringIO.StringIO(p))
    l = len(rootl)
    u.persistent_load = rootl
    u.noload()
    try:
        u.noload()
    except:
        # Hm.  We failed to do second load.  Maybe there wasn't a
        # second pickle.  Let's check:
        f = cStringIO.StringIO(p)
        u = cPickle.Unpickler(f)
        u.persistent_load = []
        u.noload()
        if len(p) > f.tell():
            raise ValueError, 'Error unpickling, %s' % p


    # References may be:
    #
    # - A tuple, in which case they are an oid and class.
    #   In this case, just extract the first element, which is
    #   the oid
    #
    # - A list, which is a weak reference. We skip those.
    #
    # - Anything else must be an oid. This means that an oid
    #   may not be a list or a tuple. This is a bit lame.
    #   We could avoid this lamosity by allowing single-element
    #   tuples, so that we wrap oids that are lists or tuples in
    #   tuples.
    #
    # - oids may *not* be false.  I'm not sure why. 

    out = []
    for v in rootl:
        assert v # Let's see if we ever get empty ones
        if type(v) is list:
            # skip wekrefs
            continue
        if type(v) is tuple:
            v = v[0]
        out.append(v)

    rootl[:] = out

    return rootl
