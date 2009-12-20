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
import ZODB.tests.util  # layer used at class scope

_NOISY = os.environ.get('NOISY_REPOZO_TEST_OUTPUT')

class OurDB:

    def __init__(self, dir):
        from BTrees.OOBTree import OOBTree
        import transaction
        self.dir = dir
        self.getdb()
        conn = self.db.open()
        conn.root()['tree'] = OOBTree()
        transaction.commit()
        self.close()

    def getdb(self):
        from ZODB import DB
        from ZODB.FileStorage import FileStorage
        storage_filename = os.path.join(self.dir, 'Data.fs')
        storage = FileStorage(storage_filename)
        self.db = DB(storage)

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
        import random
        import transaction
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


class RepozoTests(unittest.TestCase):

    layer = ZODB.tests.util.MininalTestLayer('repozo')

    def setUp(self):
        # compute directory names
        import tempfile
        self.basedir = tempfile.mkdtemp()
        self.backupdir = os.path.join(self.basedir, 'backup')
        self.datadir = os.path.join(self.basedir, 'data')
        self.restoredir = os.path.join(self.basedir, 'restore')
        self.copydir = os.path.join(self.basedir, 'copy')
        self.currdir = os.getcwd()
        # create empty directories
        os.mkdir(self.backupdir)
        os.mkdir(self.datadir)
        os.mkdir(self.restoredir)
        os.mkdir(self.copydir)
        os.chdir(self.datadir)
        self.db = OurDB(self.datadir)

    def tearDown(self):
        os.chdir(self.currdir)
        import shutil
        shutil.rmtree(self.basedir)

    def _callRepozoMain(self, argv):
        from ZODB.scripts.repozo import main
        main(argv)

    def testRepozo(self):
        self.saved_snapshots = []  # list of (name, time) pairs for copies.

        for i in range(100):
            self.mutate_pack_backup(i)

        # Verify snapshots can be reproduced exactly.
        for copyname, copytime in self.saved_snapshots:
            if _NOISY:
                print "Checking that", copyname,
                print "at", copytime, "is reproducible."
            self.assertRestored(copyname, copytime)

    def mutate_pack_backup(self, i):
        import random
        from shutil import copyfile
        from time import gmtime
        from time import sleep
        self.db.mutate()

        # Pack about each tenth time.
        if random.random() < 0.1:
            if _NOISY:
                print "packing"
            self.db.pack()
            self.db.close()

        # Make an incremental backup, half the time with gzip (-z).
        argv = ['-BQr', self.backupdir, '-f', 'Data.fs']
        if _NOISY:
            argv.insert(0, '-v')
        if random.random() < 0.5:
            argv.insert(0, '-z')
        self._callRepozoMain(argv)

        # Save snapshots to assert that dated restores are possible
        if i % 9 == 0:
            srcname = os.path.join(self.datadir, 'Data.fs')
            copytime = '%04d-%02d-%02d-%02d-%02d-%02d' % (gmtime()[:6])
            copyname = os.path.join(self.copydir, "Data%d.fs" % i)
            copyfile(srcname, copyname)
            self.saved_snapshots.append((copyname, copytime))

        # Make sure the clock moves at least a second.
        sleep(1.01)

        # Verify current Data.fs can be reproduced exactly.
        self.assertRestored()

    def assertRestored(self, correctpath='Data.fs', when=None):
    # Do recovery to time 'when', and check that it's identical to correctpath.
        # restore to Restored.fs
        restoredfile = os.path.join(self.restoredir, 'Restored.fs')
        argv = ['-Rr', self.backupdir, '-o', restoredfile]
        if _NOISY:
            argv.insert(0, '-v')
        if when is not None:
            argv.append('-D')
            argv.append(when)
        self._callRepozoMain(argv)

        # check restored file content is equal to file that was backed up
        f = file(correctpath, 'rb')
        g = file(restoredfile, 'rb')
        fguts = f.read()
        gguts = g.read()
        f.close()
        g.close()
        msg = ("guts don't match\ncorrectpath=%r when=%r\n cmd=%r" %
            (correctpath, when, ' '.join(argv)))
        self.assertEquals(fguts, gguts, msg)


def test_suite():
    return unittest.TestSuite([
        unittest.makeSuite(RepozoTests),
    ])
