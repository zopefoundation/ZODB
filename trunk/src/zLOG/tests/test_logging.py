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
"""Tests of the integration with the standard logging package."""

import logging
import unittest

from ZConfig.components.logger.tests.test_logger import LoggingTestBase

import zLOG

from zLOG.EventLogger import log_write


class CollectingHandler(logging.Handler):

    def __init__(self):
        logging.Handler.__init__(self)
        self.records = []

    def emit(self, record):
        self.records.append(record)


class LoggingIntegrationTestCase(LoggingTestBase):

    def setUp(self):
        LoggingTestBase.setUp(self)
        self.handler = CollectingHandler()
        self.records = self.handler.records
        self.logger = logging.getLogger()
        self.logger.addHandler(self.handler)

    def test_log_record(self):
        #log_write(subsystem, severity, summary, detail, error)
        log_write("sample.subsystem", zLOG.WARNING, "summary", "detail", None)
        self.assertEqual(len(self.records), 1)
        record = self.records[0]
        self.assertEqual(record.levelno, logging.WARN)
        self.assertEqual(record.name, "sample.subsystem")
        # Make sure both the message and the detail information appear
        # in the text that gets logged:
        record.msg.index("summary")
        record.msg.index("detail")


def test_suite():
    return unittest.makeSuite(LoggingIntegrationTestCase)

if __name__ == "__main__":
    unittest.main(defaultTest="test_suite")
