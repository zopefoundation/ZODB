##############################################################################
#
# Copyright (c) 2001 Zope Corporation and Contributors.
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

"""Convenience function extracted from ZODB4's zodb.storage.base."""


def splitrefs(refstr, oidlen=8):
    # refstr is a packed string of reference oids.  Always return a list of
    # oid strings.  Most storages use fixed oid lengths of 8 bytes, but if
    # the oids in refstr are a different size, use oidlen to specify.  This
    # does /not/ support variable length oids in refstr.
    if not refstr:
        return []
    num, extra = divmod(len(refstr), oidlen)
    fmt = '%ds' % oidlen
    assert extra == 0, refstr
    return list(struct.unpack('>' + (fmt * num), refstr))
