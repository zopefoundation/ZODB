#!/usr/bin/env python
##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
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

"""Test repozo.py.

This is a by-hand test.  It succeeds iff it doesn't blow up.  Run it with
its home directory as the current directory.  It will destroy all files
matching Data.* and Copy.* in this directory, and anything in a
subdirectory of name 'backup'.
"""

import os
import random
import time
import glob
import sys
import shutil

import ZODB
from ZODB import FileStorage
import transaction

PYTHON = sys.executable + ' '

def cleanup():
    for fname in glob.glob('Data.*') + glob.glob('Copy.*'):
        os.remove(fname)

    if os.path.isdir('backup'):
        for fname in os.listdir('backup'):
            os.remove(os.path.join('backup', fname))
        os.rmdir('backup')

class OurDB:
    def __init__(self):
        from BTrees.OOBTree import OOBTree
        self.getdb()
        conn = self.db.open()
        conn.root()['tree'] = OOBTree()
        transaction.commit()
        self.close()

    def getdb(self):
        storage = FileStorage.FileStorage('Data.fs')
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

# Do recovery to time 'when', and check that it's identical to correctpath.
def check(correctpath='Data.fs', when=None):
    if when is None:
        extra = ''
    else:
        extra = ' -D ' + when
    cmd = PYTHON + '../repozo.py -vRr backup -o Copy.fs' + extra
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

def mutatedb(db):
    # Make random mutations to the btree in the database.
    tree = db.gettree()
    for dummy in range(100):
        if random.random() < 0.6:
            tree[random.randrange(100000)] = random.randrange(100000)
        else:
            keys = tree.keys()
            if keys:
                del tree[keys[0]]
    transaction.commit()
    db.close()

def main():
    cleanup()
    os.mkdir('backup')
    d = OurDB()
    # Every 9th time thru the loop, we save a full copy of Data.fs,
    # and at the end we ensure we can reproduce those too.
    saved_snapshots = []  # list of (name, time) pairs for copies.

    for i in range(100):
        # Make some mutations.
        mutatedb(d)

        # Pack about each tenth time.
        if random.random() < 0.1:
            print "packing"
            d.pack()
            d.close()

        # Make an incremental backup, half the time with gzip (-z).
        if random.random() < 0.5:
            os.system(PYTHON + '../repozo.py -vBQr backup -f Data.fs')
        else:
            os.system(PYTHON + '../repozo.py -zvBQr backup -f Data.fs')

        if i % 9 == 0:
            copytime = '%04d-%02d-%02d-%02d-%02d-%02d' % (time.gmtime()[:6])
            copyname = os.path.join('backup', "Data%d" % i) + '.fs'
            shutil.copyfile('Data.fs', copyname)
            saved_snapshots.append((copyname, copytime))

        # Make sure the clock moves at least a second.
        time.sleep(1.01)

        # Verify current Data.fs can be reproduced exactly.
        check()

    # Verify snapshots can be reproduced exactly.
    for copyname, copytime in saved_snapshots:
        print "Checking that", copyname, "at", copytime, "is reproducible."
        check(copyname, copytime)

    # Tear it all down.
    cleanup()
    print 'Test passed!'

if __name__ == '__main__':
    main()
