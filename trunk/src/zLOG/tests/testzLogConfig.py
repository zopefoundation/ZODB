##############################################################################
#
# Copyright (c) 2002, 2003 Zope Corporation and Contributors.
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
import zLOG.tests


class TestzLOGConfig(unittest.TestCase):

    _schema = None
    _schematext = """
      <schema>
        <import package='zLOG'/>
        <section type='logger' name='*' attribute='logger'/>
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
        self.assert_(conf.logger is None)

    def test_config_without_handlers(self):
        conf = self.get_config("<logger/>")
        self.assert_(conf.logger is not None)
        self.assertEqual(conf.logger.level, logging.INFO)
        logger = conf.logger()
        self.assert_(isinstance(logger, logging.Logger))
        self.assertEqual(logger.handlers, [])

    # XXX need to make sure each loghandler datatype gets exercised.


def test_suite():
    return unittest.makeSuite(TestzLOGConfig)

if __name__ == '__main__':
    unittest.main(defaultTest="test_suite")
