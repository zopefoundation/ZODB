##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
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

"""Functional test to produce a dangling reference."""
from __future__ import print_function

import time

import transaction
from persistent import Persistent

from ZODB import DB
from ZODB.FileStorage import FileStorage


class P(Persistent):
    pass


def create_dangling_ref(db):
    rt = db.open().root()

    rt[1] = o1 = P()
    transaction.get().note(u"create o1")
    transaction.commit()

    rt[2] = o2 = P()
    transaction.get().note(u"create o2")
    transaction.commit()

    c = o1.child = P()
    transaction.get().note(u"set child on o1")
    transaction.commit()

    o1.child = P()
    transaction.get().note(u"replace child on o1")
    transaction.commit()

    time.sleep(2)
    # The pack should remove the reference to c, because it is no
    # longer referenced from o1.  But the object still exists and has
    # an oid, so a new commit of it won't create a new object.
    db.pack()

    print(repr(c._p_oid))
    o2.child = c
    transaction.get().note(u"set child on o2")
    transaction.commit()


def main():
    fs = FileStorage(u"dangle.fs")
    db = DB(fs)
    create_dangling_ref(db)
    db.close()


if __name__ == "__main__":
    main()
