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
"""Test suite for AuthZEO."""

import glob
import os
import time
import unittest

from ThreadedAsync import LoopCallback
from ZEO.auth.database import Database
#from ZEO.auth.auth_srp import SRPDatabase
from ZEO.ClientStorage import ClientStorage
from ZEO.StorageServer import StorageServer
from ZODB.FileStorage import FileStorage

storage = FileStorage('auth-test.fs')

SOCKET='auth-test-socket'
STORAGES={'1': storage}

class BaseTest(unittest.TestCase):
    def createDB(self, name):
        if os.path.exists(name):
            os.remove(self.database)        
        if name.endswith('srp'):
            db = SRPDatabase(name)
        else:
            db = Database(name)

        db.add_user('foo', 'bar')
        db.save()
        
    def setUp(self):
        self.createDB(self.database)
        self.pid =  os.fork()
        if not self.pid:
            self.server = StorageServer(SOCKET, STORAGES,
                                        auth_protocol=self.protocol,
                                        auth_filename=self.database)
            LoopCallback.loop()

    def tearDown(self):
        os.kill(self.pid, 9)
        os.remove(self.database)
        os.remove(SOCKET)

        for file in glob.glob('auth-test.fs*'):
            os.remove(file)
            
    def check(self):
        # Sleep for 0.2 seconds to give the server some time to start up
        # seems to be needed before and after creating the storage
        time.sleep(0.2)
        cs = ClientStorage(SOCKET, wait=0, username='foo', password='bar')
        time.sleep(0.2)
        
        if cs._connection == None:
            raise AssertionError, \
                  "authentication for %s failed" % self.protocol
        
        cs._connection.poll()
        if not cs.is_connected():
             raise AssertionError, \
                  "authentication for %s failed" % self.protocol
            
class PlainTextAuth(BaseTest):
    protocol = 'plaintext'
    database = 'authdb.sha'
    
class SHAAuth(BaseTest):
    protocol = 'sha'
    database = 'authdb.sha'
    
#class SRPAuth(BaseTest):
#    protocol = 'srp'
#    database = 'authdb.srp'
    
test_classes = [PlainTextAuth, SHAAuth] # SRPAuth

def test_suite():
    suite = unittest.TestSuite()
    for klass in test_classes:
        sub = unittest.makeSuite(klass, 'check')
        suite.addTest(sub)
    return suite

if __name__ == "__main__":
    unittest.main(defaultTest='test_suite')

