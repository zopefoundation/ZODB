##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""More convenience functions for dealing with proxies.

$Id$
"""
from zope.interface import moduleProvides
from zope.proxy.interfaces import IProxyIntrospection
from types import ClassType
from zope.proxy._zope_proxy_proxy import *
from zope.proxy._zope_proxy_proxy import _CAPI

moduleProvides(IProxyIntrospection)
__all__ = tuple(IProxyIntrospection)

def ProxyIterator(p):
    yield p
    while isProxy(p):
        p = getProxiedObject(p)
        yield p
