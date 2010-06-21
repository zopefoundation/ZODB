#!/usr/bin/env python2.3

##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Tool to dump the last few transactions from a FileStorage."""

from ZODB.fstools import prev_txn

import binascii
import getopt
import sys

try:
    from hashlib import sha1
except ImportError:
    from sha import sha as sha1

def main(path, ntxn):
    f = open(path, "rb")
    f.seek(0, 2)
    th = prev_txn(f)
    i = ntxn
    while th and i > 0:
        hash = sha1(th.get_raw_data()).digest()
        l = len(str(th.get_timestamp())) + 1
        th.read_meta()
        print "%s: hash=%s" % (th.get_timestamp(),
                               binascii.hexlify(hash))
        print ("user=%r description=%r length=%d offset=%d"
               % (th.user, th.descr, th.length, th.get_data_offset()))
        print
        th = th.prev_txn()
        i -= 1

def Main():
    ntxn = 10
    opts, args = getopt.getopt(sys.argv[1:], "n:")
    path, = args
    for k, v in opts:
        if k == '-n':
            ntxn = int(v)
    main(path, ntxn)

if __name__ == "__main__":
    Main()
