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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Test cases for objects implementing IDataManager.

This is a combo test between Connection and DB, since the two are
rather incestuous and the DB Interface is not defined that I was
able to find.

To do a full test suite one would probably want to write a dummy
storage that will raise errors as needed for testing.

I started this test suite to reproduce a very simple error (tpc_abort
had an error and wouldn't even run if called).  So it is *very*
incomplete, and even the tests that exist do not make sure that
the data actually gets written/not written to the storge.

Obviously this test suite should be expanded.

$Id: abstestIDataManager.py,v 1.4 2004/02/20 16:56:57 fdrake Exp $
"""

from unittest import TestCase
from transaction.interfaces import IRollback

class IDataManagerTests(TestCase, object):

    def setUp(self):
        self.datamgr = None # subclass should override
        self.obj = None # subclass should define Persistent object
        self.txn_factory = None

    def get_transaction(self):
        return self.txn_factory()

    ################################
    # IDataManager interface tests #
    ################################

    def testCommitObj(self):
        tran = self.get_transaction()
        self.datamgr.prepare(tran)
        self.datamgr.commit(tran)

    def testAbortTran(self):
        tran = self.get_transaction()
        self.datamgr.prepare(tran)
        self.datamgr.abort(tran)

    def testRollback(self):
        tran = self.get_transaction()
        rb = self.datamgr.savepoint(tran)
        self.assert_(IRollback.isImplementedBy(rb))
