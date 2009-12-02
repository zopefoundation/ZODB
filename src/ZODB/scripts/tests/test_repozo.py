#!/usr/bin/env python
##############################################################################
#
# Copyright (c) 2004-2009 Zope Corporation and Contributors.
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

import unittest
import os
import sys
import random
import time
import shutil

import ZODB
from ZODB import FileStorage
import transaction
from BTrees.OOBTree import OOBTree

from ZODB.scripts import tests as tests_module

REPOZO = os.path.join(os.path.dirname(sys.argv[0]), 'repozo')


class OurDB:

    def __init__(self, dir):
        self.dir = dir
        self.getdb()
        conn = self.db.open()
        conn.root()['tree'] = OOBTree()
        transaction.commit()
        self.close()

    def getdb(self):
        storage_filename = os.path.join(self.dir, 'Data.fs')
        storage = FileStorage.FileStorage(storage_filename)
        self.db = ZODB.DB(storage)

    def gettree(self):
        self.getdb()
        conn = self.db.open()
        return conn.root()['tree']

    def pack(self):
        self.getdb()
        self.db.pack()

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None

    def mutate(self):
        # Make random mutations to the btree in the database.
        tree = self.gettree()
        for dummy in range(100):
            if random.random() < 0.6:
                tree[random.randrange(100000)] = random.randrange(100000)
            else:
                keys = tree.keys()
                if keys:
                    del tree[keys[0]]
        transaction.commit()
        self.close()


class BasicRepozoTests(unittest.TestCase):

    def test_importability(self):
        from ZODB.scripts import repozo


class RepozoTests(unittest.TestCase):

    def setUp(self):
        # compute directory names
        self.basedir = os.path.dirname(tests_module.__file__)
        self.backupdir = os.path.join(self.basedir, 'backup')
        self.datadir = os.path.join(self.basedir, 'data')
        self.restoredir = os.path.join(self.basedir, 'restore')
        self.copydir = os.path.join(self.basedir, 'copy')
        self.currdir = os.getcwd()
        # ensure they have all been deleted
        self.cleanup()
        # create empty directories
        os.mkdir(self.backupdir)
        os.mkdir(self.datadir)
        os.mkdir(self.restoredir)
        os.mkdir(self.copydir)
        os.chdir(self.datadir)
        self.db = OurDB(self.datadir)

    def tearDown(self):
        self.cleanup()
        os.chdir(self.currdir)

    def testRepozo(self):
        self.saved_snapshots = []  # list of (name, time) pairs for copies.

        for i in range(100):
            self.mutate_pack_backup(i)

        # Verify snapshots can be reproduced exactly.
        for copyname, copytime in self.saved_snapshots:
            print "Checking that", copyname, "at", copytime, "is reproducible."
            self.assertRestored(copyname, copytime)

    def mutate_pack_backup(self, i):
        self.db.mutate()

        # Pack about each tenth time.
        if random.random() < 0.1:
            print "packing"
            self.db.pack()
            self.db.close()

        # Make an incremental backup, half the time with gzip (-z).
        if random.random() < 0.5:
            cmd = REPOZO + ' -vBQr %s -f Data.fs'
        else:
            cmd = REPOZO + ' -zvBQr %s -f Data.fs'
        os.system(cmd % self.backupdir)

        # Save snapshots to assert that dated restores are possible
        if i % 9 == 0:
            srcname = os.path.join(self.datadir, 'Data.fs')
            copytime = '%04d-%02d-%02d-%02d-%02d-%02d' % (time.gmtime()[:6])
            copyname = os.path.join(self.copydir, "Data%d.fs" % i)
            shutil.copyfile(srcname, copyname)
            self.saved_snapshots.append((copyname, copytime))

        # Make sure the clock moves at least a second.
        time.sleep(1.01)

        # Verify current Data.fs can be reproduced exactly.
        self.assertRestored()

    def assertRestored(self, correctpath='Data.fs', when=None):
    # Do recovery to time 'when', and check that it's identical to correctpath.
        if when is None:
            extra = ''
        else:
            extra = ' -D ' + when
        # restore to Restored.fs
        restoredfile = os.path.join(self.restoredir, 'Restored.fs')
        cmd = REPOZO + ' -vRr %s -o ' + restoredfile + extra
        os.system(cmd % self.backupdir)

        # check restored file content is equal to file that was backed up
        f = file(correctpath, 'rb')
        g = file(restoredfile, 'rb')
        fguts = f.read()
        gguts = g.read()
        f.close()
        g.close()
        msg = ("guts don't match\ncorrectpath=%r when=%r\n cmd=%r" %
            (correctpath, when, cmd))
        self.assertEquals(fguts, gguts, msg)

    def cleanup(self):
        for dir in [self.datadir, self.backupdir, self.restoredir,
                self.copydir]:
            if os.path.isdir(dir):
                for fname in os.listdir(dir):
                    os.remove(os.path.join(dir, fname))
                os.rmdir(dir)


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(BasicRepozoTests))
    suite.addTest(unittest.makeSuite(RepozoTests))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
