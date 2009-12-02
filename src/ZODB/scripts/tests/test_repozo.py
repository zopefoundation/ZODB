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
import glob
import shutil

import ZODB
from ZODB import FileStorage
import transaction

from ZODB.scripts import tests as tests_module

REPOZO = os.path.join(os.path.dirname(sys.argv[0]), 'repozo')


def cleanup(basedir):
    globData = os.path.join(basedir, 'Data.*')
    globCopy = os.path.join(basedir, 'Copy.*')
    backupDir = os.path.join(basedir, 'backup')
    for fname in glob.glob(globData) + glob.glob(globCopy):
        os.remove(fname)

    if os.path.isdir(backupDir):
        for fname in os.listdir(backupDir):
            os.remove(os.path.join(backupDir, fname))
        os.rmdir(backupDir)


class OurDB:

    def __init__(self, basedir):
        from BTrees.OOBTree import OOBTree
        self.basedir = basedir
        self.getdb()
        conn = self.db.open()
        conn.root()['tree'] = OOBTree()
        transaction.commit()
        self.close()

    def getdb(self):
        storage_filename = os.path.join(self.basedir, 'Data.fs')
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


# Do recovery to time 'when', and check that it's identical to correctpath.
def check(correctpath='Data.fs', when=None):
    if when is None:
        extra = ''
    else:
        extra = ' -D ' + when
    cmd = REPOZO + ' -vRr backup -o Copy.fs' + extra
    os.system(cmd)
    f = file(correctpath, 'rb')
    g = file('Copy.fs', 'rb')
    fguts = f.read()
    gguts = g.read()
    f.close()
    g.close()
    if fguts != gguts:
        raise ValueError("guts don't match\n"
                         "    correctpath=%r when=%r\n"
                         "    cmd=%r" % (correctpath, when, cmd))


def main(basedir, d):
    # Every 9th time thru the loop, we save a full copy of Data.fs,
    # and at the end we ensure we can reproduce those too.
    saved_snapshots = []  # list of (name, time) pairs for copies.

    for i in range(100):
        print i
        # Make some mutations.
        d.mutate()

        # Pack about each tenth time.
        if random.random() < 0.1:
            print "packing"
            d.pack()
            d.close()

        # Make an incremental backup, half the time with gzip (-z).
        if random.random() < 0.5:
            os.system(REPOZO + ' -vBQr backup -f Data.fs')
        else:
            os.system(REPOZO + ' -zvBQr backup -f Data.fs')

        if i % 9 == 0:
            copytime = '%04d-%02d-%02d-%02d-%02d-%02d' % (time.gmtime()[:6])
            copyname = os.path.join(basedir, 'backup', "Data%d" % i) + '.fs'
            srcname = os.path.join(basedir, 'Data.fs')
            shutil.copyfile(srcname, copyname)
            saved_snapshots.append((copyname, copytime))

        # Make sure the clock moves at least a second.
        time.sleep(1.01)

        # Verify current Data.fs can be reproduced exactly.
        check()

    # Verify snapshots can be reproduced exactly.
    for copyname, copytime in saved_snapshots:
        print "Checking that", copyname, "at", copytime, "is reproducible."
        check(copyname, copytime)


class RepozoTest(unittest.TestCase):

    def setUp(self):
        self.basedir = os.path.dirname(tests_module.__file__)
        self.currdir = os.getcwd()
        os.chdir(self.basedir)
        cleanup(self.basedir)
        os.mkdir(os.path.join(self.basedir, 'backup'))
        self.d = OurDB(self.basedir)

    def tearDown(self):
        cleanup(self.basedir)
        os.chdir(self.currdir)

    def testDummy(self):
        main(self.basedir, self.d)


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(RepozoTest))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
