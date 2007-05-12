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

import os
import shutil
import sys
import tempfile
import time

import persistent
import transaction
from ZODB.MappingStorage import MappingStorage
from ZODB.DB import DB as _DB

def DB(name='Test', **dbargs):
    return _DB(MappingStorage(name), **dbargs)

def commit():
    transaction.commit()

def pack(db):
    db.pack(time.time()+1)

class P(persistent.Persistent):

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return 'P(%s)' % self.name

def setUp(test):
    test.globs['__teardown_stack__'] = []
    tmp = tempfile.mkdtemp('test')
    registerTearDown(test, lambda : rmtree(tmp))
    here = os.getcwd()
    registerTearDown(test, lambda : os.chdir(here))
    os.chdir(tmp)

if sys.platform == 'win32':    
    # On windows, we can't remove a directory of there are files upen.
    # We may need to wait a while for processes to exit.
    def rmtree(path):
        for i in range(1000):
            try:
                shutil.rmtree(path)
            except OSError:
                time.sleep(0.01)
            else:
                break

else:
    rmtree = shutil.rmtree
            
def registerTearDown(test, func):
    test.globs['__teardown_stack__'].append(func)    
    
def tearDown(test):
    for f in test.globs['__teardown_stack__']:
        f()
