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

"""ZEO depends on recent versions of asyncore and cPickle

Try to fix up the imports of these to make these dependencies work,
localizing the hacks^H^H^H^H^Hchanges here.
"""
import sys, os

def whiff(where):
    if not where: return 0
    import imp
    try: m=imp.find_module('ZServer', [where])
    except: return 0
    else: return 1
    

def fap():
    # if we are using an old version of Python, our asyncore is likely to
    # be out of date.  If ZServer is sitting around, we can get a current
    # version of ayncore from it. In any case, if we are going to be used
    # with Zope, it's important to use the version from Zope.
    try:
        import ZServer
    except:
        # Try a little harder to import ZServer
        import os, imp
        
        location = package_home()
        location = os.path.split(location)[0]
        location = os.path.split(location)[0]
        location = os.path.split(location)[0]
        
        if whiff(location):
            sys.path.append(location)
            try:
                import ZServer
            except:
                pass

    import asyncore

    if sys.version[:1] < '2' and asyncore.loop.func_code.co_argcount < 3:
        raise ImportError, 'Cannot import an up-to-date asyncore'

    sys.modules['ZEO.asyncore']=asyncore

    # We need a recent version of cPickle too.
    if sys.version[:3] < '1.6':
        try:
            from ZODB import cPickle
            sys.modules['ZEO.cPickle']=cPickle
        except:
            # Try a little harder
            import cPickle
    else:
        import cPickle

    import cStringIO
    p=cPickle.Pickler(cStringIO.StringIO(),1)
    try:
        p.fast=1
    except:
        raise ImportError, 'Cannot import an up-to-date cPickle'
    p=cPickle.Unpickler(cStringIO.StringIO())
    try:
        p.find_global=1
    except:
        raise ImportError, 'Cannot import an up-to-date cPickle'


def package_home():
    m=sys.modules[__name__]
    if hasattr(m,'__path__'):
        r=m.__path__[0]
    elif "." in __name__:
        from string import rfind
        r=sys.modules[__name__[:rfind(__name__,'.')]].__path__[0]
    else:
        r=__name__
    return os.path.join(os.getcwd(), r)

fap()
