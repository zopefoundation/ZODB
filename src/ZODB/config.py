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

$Id: config.py,v 1.2 2003/01/03 21:19:06 fdrake Exp $"""

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
    return databaseFromConfig(config)

def databaseFromURL(url):
    config, handler = ZConfig.loadConfig(getSchema(), url)
    return databaseFromConfig(config)

def databaseFromConfig(config):
    return ZODB.DB(config.storage.open(),
                   pool_size=config.pool_size,
                   cache_size=config.cache_size,
                   version_pool_size=config.version_pool_size,
                   version_cache_size=config.version_cache_size)

class StorageConfig:

    def __init__(self, config):
        self.config = config

    def open(self):
        raise NotImplementedError

class MappingStorage(StorageConfig):

    def open(self):
        from ZODB.MappingStorage import MappingStorage
        return MappingStorage(self.config.name)

class FileStorage(StorageConfig):

    def open(self):
        from ZODB.FileStorage import FileStorage
        return FileStorage(self.config.path,
                           create=self.config.create,
                           read_only=self.config.read_only,
                           stop=self.config.stop,
                           quota=self.config.quota)

class ZEOClient(StorageConfig):

    def open(self):
        from ZEO.ClientStorage import ClientStorage
        # config.server is a multikey of socket-address values
        # where the value is a socket family, address tuple.
        L = [addr for family, addr in self.config.server]
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

class BDBStorage(StorageConfig):
    
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
