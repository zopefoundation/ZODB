##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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

"""Support code shared among the tests."""

import os
import StringIO
import unittest
import urllib

import ZConfig

from ZConfig.loader import ConfigLoader
from ZConfig.url import urljoin


try:
    __file__
except NameError:
    import sys
    __file__ = sys.argv[0]

d = os.path.abspath(os.path.join(os.path.dirname(__file__), "input"))
CONFIG_BASE = "file://%s/" % urllib.pathname2url(d)


class TestBase(unittest.TestCase):
    """Utility methods which can be used with the schema support."""

    def load_both(self, schema_url, conf_url):
        schema = self.load_schema(schema_url)
        conf = self.load_config(schema, conf_url)
        return schema, conf

    def load_schema(self, relurl):
        self.url = urljoin(CONFIG_BASE, relurl)
        self.schema = ZConfig.loadSchema(self.url)
        self.assert_(self.schema.issection())
        return self.schema

    def load_schema_text(self, text, url=None):
        sio = StringIO.StringIO(text)
        self.schema = ZConfig.loadSchemaFile(sio, url)
        return self.schema

    def load_config(self, schema, conf_url, num_handlers=0):
        conf_url = urljoin(CONFIG_BASE, conf_url)
        loader = self.create_config_loader(schema)
        self.conf, self.handlers = loader.loadURL(conf_url)
        self.assertEqual(len(self.handlers), num_handlers)
        return self.conf

    def load_config_text(self, schema, text, num_handlers=0, url=None):
        sio = StringIO.StringIO(text)
        loader = self.create_config_loader(schema)
        self.conf, self.handlers = loader.loadFile(sio, url)
        self.assertEqual(len(self.handlers), num_handlers)
        return self.conf

    def create_config_loader(self, schema):
        return ConfigLoader(schema)
