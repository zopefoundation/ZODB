#! /usr/bin/env python

##############################################################################
#
# Copyright (c) 2002 Zope Corporation and Contributors.
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

"""Functional test to produce a dangling reference."""

import time

import transaction
from ZODB.FileStorage import FileStorage
from ZODB import DB

from persistent import Persistent

class P(Persistent):
    pass

def create_dangling_ref(db):
    rt = db.open().root()

    rt[1] = o1 = P()
    transaction.get().note("create o1")
    transaction.commit()

    rt[2] = o2 = P()
    transaction.get().note("create o2")
    transaction.commit()

    c = o1.child = P()
    transaction.get().note("set child on o1")
    transaction.commit()

    o1.child = P()
    transaction.get().note("replace child on o1")
    transaction.commit()

    time.sleep(2)
    # The pack should remove the reference to c, because it is no
    # longer referenced from o1.  But the object still exists and has
    # an oid, so a new commit of it won't create a new object.
    db.pack()

    print repr(c._p_oid)
    o2.child = c
    transaction.get().note("set child on o2")
    transaction.commit()

def main():
    fs = FileStorage("dangle.fs")
    db = DB(fs)
    create_dangling_ref(db)
    db.close()

if __name__ == "__main__":
    main()
