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
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Open database and storage from a configuration.

$Id: config.py,v 1.8 2003/01/13 16:28:29 fdrake Exp $"""

import os
import StringIO

import ZConfig

import ZODB

schema_path = os.path.join(ZODB.__path__[0], "config.xml")
_schema = None

def getSchema():
    global _schema
    if _schema is None:
        _schema = ZConfig.loadSchema(schema_path)
    return _schema

def databaseFromString(s):
    return databaseFromFile(StringIO.StringIO(s))

def databaseFromFile(f):
    config, handle = ZConfig.loadConfigFile(getSchema(), f)
    return databaseFromConfig(config.database)

def databaseFromURL(url):
    config, handler = ZConfig.loadConfig(getSchema(), url)
    return databaseFromConfig(config.database)

def databaseFromConfig(section):
    return section.open()


class BaseConfig:
    """Object representing a configured storage or database.

    Methods:

    open() -- open and return the configured object

    Attributes:

    name   -- name of the storage

    """

    def __init__(self, config):
        self.config = config
        self.name = config.getSectionName()

    def open(self):
        """Open and return the storage object."""
        raise NotImplementedError

class ZODBDatabase(BaseConfig):

    def open(self):
        section = self.config
        return ZODB.DB(section.storage.open(),
                       pool_size=section.pool_size,
                       cache_size=section.cache_size,
                       version_pool_size=section.version_pool_size,
                       version_cache_size=section.version_cache_size)

class MappingStorage(BaseConfig):

    def open(self):
        from ZODB.MappingStorage import MappingStorage
        return MappingStorage(self.config.name)

class DemoStorage(BaseConfig):

    def open(self):
        from ZODB.DemoStorage import DemoStorage
        if self.config.base:
            base = self.config.base.open()
        else:
            base = None
        return DemoStorage(self.config.name,
                           base=base,
                           quota=self.config.quota)

class FileStorage(BaseConfig):

    def open(self):
        from ZODB.FileStorage import FileStorage
        return FileStorage(self.config.path,
                           create=self.config.create,
                           read_only=self.config.read_only,
                           stop=self.config.stop,
                           quota=self.config.quota)

class ZEOClient(BaseConfig):

    def open(self):
        from ZEO.ClientStorage import ClientStorage
        # config.server is a multikey of socket-address values
        # where the value is a socket family, address tuple.
        L = [server.address for server in self.config.server]
        return ClientStorage(
            L,
            storage=self.config.storage,
            cache_size=self.config.cache_size,
            name=self.config.name,
            client=self.config.client,
            var=self.config.var,
            min_disconnect_poll=self.config.min_disconnect_poll,
            max_disconnect_poll=self.config.max_disconnect_poll,
            wait=self.config.wait,
            read_only=self.config.read_only,
            read_only_fallback=self.config.read_only_fallback)

class BDBStorage(BaseConfig):

    def open(self):
        from BDBStorage.BerkeleyBase import BerkeleyConfig
        from BDBStorage.BDBFullStorage import BDBFullStorage
        from BDBStorage.BDBMinimalStorage import BDBMinimalStorage
        # Figure out which class we want
        sectiontype = self.config.getSectionType()
        storageclass = {'fullstorage': BDBFullStorage,
                        'minimalstorage': BDBMinimalStorage,
                        }[sectiontype]
        bconf = BerkeleyConfig()
        for name in dir(BerkeleyConfig):
            if name.startswith('_'):
                continue
            setattr(bconf, name, getattr(self.config, name))
        return storageclass(self.config.name, config=bconf)
