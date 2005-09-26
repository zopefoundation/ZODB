#!/usr/bin/env python
##############################################################################
#
# Copyright (c) 2005 Zope Corporation and Contributors.
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

import os
import posixpath
import sys

import zpkgsetup.setup

# Note that release.py must be able to recognize the VERSION line.
VERSION = "3.5.1"

here = os.path.dirname(os.path.abspath(__file__))

def join(*parts):
    local_full_path = os.path.join(here, *parts)
    relative_path = posixpath.join(*parts)
    return local_full_path, relative_path

context = zpkgsetup.setup.SetupContext("ZODB3", VERSION, __file__)

context.load_metadata(
    os.path.join(here, "PUBLICATION.cfg"))

context.scan("ZODB3",          here, ".")
context.scan("BTrees",         *join("src", "BTrees"))
context.scan("Persistence",    *join("src", "Persistence"))
context.scan("persistent",     *join("src", "persistent"))
context.scan("ThreadedAsync",  *join("src", "ThreadedAsync"))
context.scan("transaction",    *join("src", "transaction"))
context.scan("ZConfig",        *join("src", "ZConfig"))
context.scan("zdaemon",        *join("src", "zdaemon"))
context.scan("ZEO",            *join("src", "ZEO"))
context.scan("ZODB",           *join("src", "ZODB"))
context.scan("ZODB-Scripts",   *join("src", "scripts"))
context.scan("zope",           *join("src", "zope"))
context.scan("zope.interface", *join("src", "zope", "interface"))
context.scan("zope.proxy",     *join("src", "zope", "proxy"))
context.scan("zope.testing",   *join("src", "zope", "testing"))
context.scan("ZopeUndo",       *join("src", "ZopeUndo"))
context.setup()
