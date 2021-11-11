##############################################################################
#
# Copyright (c) 2021 Zope Foundation and Contributors.
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

from ZODB import DB
from ZODB.scripts.fsstats import rx_data
from ZODB.scripts.fsstats import rx_txn
from ZODB.tests.util import TestCase
from ZODB.tests.util import run_module_as_script


class FsdumpFsstatsTests(TestCase):
    def setUp(self):
        super(FsdumpFsstatsTests, self).setUp()
        # create (empty) storage ``data.fs``
        DB("data.fs").close()

    def test_fsdump(self):
        run_module_as_script("ZODB.FileStorage.fsdump", ["data.fs"])
        # verify that ``fsstats`` will understand the output
        with open("stdout") as f:
            tno = obno = 0
            for li in f:
                if li.startswith("  data"):
                    m = rx_data.search(li)
                    if m is None:
                        continue
                    oid, size, klass = m.groups()
                    int(size)
                    obno += 1
                elif li.startswith("Trans"):
                    m = rx_txn.search(li)
                    if not m:
                        continue
                    tid, size = m.groups()
                    size = int(size)
                    tno += 1
        self.assertEqual(tno, 1)
        self.assertEqual(obno, 1)

    def test_fsstats(self):
        # The ``fsstats`` output is complex
        # currently, we just check the first (summary) line
        run_module_as_script("ZODB.FileStorage.fsdump", ["data.fs"],
                             "data.dmp")
        run_module_as_script("ZODB.scripts.fsstats", ["data.dmp"])
        with open("stdout") as f:
            self.assertEqual(f.readline().strip(),
                             "Summary: 1 txns, 1 objects, 1 revisions")
