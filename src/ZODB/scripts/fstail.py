#!/usr/bin/env python
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
from __future__ import print_function

import binascii
import getopt
import sys

from ZODB.fstools import prev_txn


try:
    from hashlib import sha1
except ImportError:
    from sha import sha as sha1


def main(path, ntxn):
    with open(path, "rb") as f:
        f.seek(0, 2)
        th = prev_txn(f)
        i = ntxn
        while th and i > 0:
            hash = sha1(th.get_raw_data()).digest()
            th.read_meta()
            print("%s: hash=%s" % (th.get_timestamp(),
                                   binascii.hexlify(hash).decode()))
            print(("user=%r description=%r length=%d offset=%d (+%d)"
                   % (th.user, th.descr, th.length, th.get_offset(), len(th))))
            print()
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
