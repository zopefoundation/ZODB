#! /usr/bin/env python
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
"""Tool to dump the last few transactions from a FileStorage."""

from ZODB.fstools import prev_txn, TxnHeader

import binascii
import getopt
import sha
import sys

def main(path, ntxn):
    f = open(path, "rb")
    f.seek(0, 2)
    th = prev_txn(f)
    i = ntxn
    while th and i > 0:
        hash = sha.sha(th.get_raw_data()).digest()
        l = len(str(th.get_timestamp())) + 1
        th.read_meta()
        print "%s: hash=%s" % (th.get_timestamp(),
                               binascii.hexlify(hash))
        print " " * l, ("user=%r description=%r length=%d"
                        % (th.user, th.descr, th.length))
        th = th.prev_txn()
        i -= 1

if __name__ == "__main__":
    ntxn = 10
    opts, args = getopt.getopt(sys.argv[1:], "n:")
    path, = args
    for k, v in opts:
        if k == '-n':
            ntxn = int(v)
    main(path, ntxn)
