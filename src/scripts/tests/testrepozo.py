#!/usr/bin/env python
##############################################################################
#
# Copyright (c) 2004 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
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

import ZODB
from ZODB import FileStorage

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
        get_transaction().commit()
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

# Do recovery to current time, and check that it's identical to Data.fs.
def check():
    os.system(PYTHON + '../repozo.py -vRr backup -o Copy.fs')
    f = file('Data.fs', 'rb')
    g = file('Copy.fs', 'rb')
    fguts = f.read()
    gguts = g.read()
    f.close()
    g.close()
    if fguts != gguts:
        raise ValueError("guts don't match")

def main():
    cleanup()
    os.mkdir('backup')
    d = OurDB()
    for dummy in range(100):
        # Make some mutations.
        tree = d.gettree()
        for dummy2 in range(100):
            if random.random() < 0.6:
                tree[random.randrange(100000)] = random.randrange(100000)
            else:
                keys = tree.keys()
                if keys:
                    del tree[keys[0]]
        get_transaction().commit()
        d.close()

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

        # Make sure the clock moves at least a second.
        time.sleep(1.01)

        # Verify current Data.fs can be reproduced exactly.
        check()

    # Tear it all down.
    cleanup()
    print 'Test passed!'

if __name__ == '__main__':
    main()
