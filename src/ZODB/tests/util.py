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
"""Conventience function for creating test databases

$Id$
"""

from ZODB.MappingStorage import DB

import os
import tempfile
import time
import unittest
import persistent
import transaction
import zope.testing.setupstack

def setUp(test, name='test'):
    transaction.abort()
    d = tempfile.mkdtemp(prefix=name)
    zope.testing.setupstack.register(test, zope.testing.setupstack.rmtree, d)
    zope.testing.setupstack.register(
        test, setattr, tempfile, 'tempdir', tempfile.tempdir)
    tempfile.tempdir = d
    zope.testing.setupstack.register(test, os.chdir, os.getcwd())
    os.chdir(d)
    zope.testing.setupstack.register(test, transaction.abort)

tearDown = zope.testing.setupstack.tearDown

class TestCase(unittest.TestCase):

    def setUp(self):
        self.globs = {}
        name = self.__class__.__name__
        mname = getattr(self, '_TestCase__testMethodName', '')
        if mname:
            name += '-' + mname
        setUp(self, name)

    tearDown = tearDown

def pack(db):
    db.pack(time.time()+1)

class P(persistent.Persistent):

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return 'P(%s)' % self.name
