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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

"""Tests for zLOG configuration via ZConfig."""

import cStringIO as StringIO
import logging
import unittest

import ZConfig

from ZConfig.components.logger import loghandler


class TestzLOGConfig(unittest.TestCase):

    # XXX This tries to save and restore the state of logging around
    # the test.  Somewhat surgical; there may be a better way.

    def setUp(self):
        self._old_logger = logging.getLogger("event")
        self._old_level = self._old_logger.level
        self._old_handlers = self._old_logger.handlers[:]
        self._old_logger.handlers[:] = [loghandler.NullHandler()]

    def tearDown(self):
        for h in self._old_logger.handlers:
            self._old_logger.removeHandler(h)
        for h in self._old_handlers:
            self._old_logger.addHandler(h)
        self._old_logger.setLevel(self._old_level)

    _schema = None
    _schematext = """
      <schema>
        <import package='zLOG'/>
        <section type='eventlog' name='*' attribute='eventlog'/>
      </schema>
    """

    def get_schema(self):
        if self._schema is None:
            sio = StringIO.StringIO(self._schematext)
            self.__class__._schema = ZConfig.loadSchemaFile(sio)
        return self._schema

    def get_config(self, text):
        conf, handler = ZConfig.loadConfigFile(self.get_schema(),
                                               StringIO.StringIO(text))
        self.assert_(not handler)
        return conf

    def test_config_without_logger(self):
        conf = self.get_config("")
        self.assert_(conf.eventlog is None)

    def test_config_without_handlers(self):
        logger = self.check_simple_logger("<eventlog/>")
        # Make sure there's a NullHandler, since a warning gets
        # printed if there are no handlers:
        self.assertEqual(len(logger.handlers), 1)
        self.assert_(isinstance(logger.handlers[0],
                                loghandler.NullHandler))

    def check_simple_logger(self, text, level=logging.INFO):
        conf = self.get_config(text)
        self.assert_(conf.eventlog is not None)
        self.assertEqual(conf.eventlog.level, level)
        logger = conf.eventlog()
        self.assert_(isinstance(logger, logging.Logger))
        self.assertEqual(len(logger.handlers), 1)
        return logger


def test_suite():
    return unittest.makeSuite(TestzLOGConfig)

if __name__ == '__main__':
    unittest.main(defaultTest="test_suite")
