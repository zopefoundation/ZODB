##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
"""Open database and storage from a configuration."""
import os

import ZConfig

import ZODB


try:
    from cStringIO import StringIO
except ImportError:
    # Py3
    from io import StringIO


db_schema_path = os.path.join(ZODB.__path__[0], "config.xml")
_db_schema = None

s_schema_path = os.path.join(ZODB.__path__[0], "storage.xml")
_s_schema = None


def getDbSchema():
    global _db_schema
    if _db_schema is None:
        _db_schema = ZConfig.loadSchema(db_schema_path)
    return _db_schema


def getStorageSchema():
    global _s_schema
    if _s_schema is None:
        _s_schema = ZConfig.loadSchema(s_schema_path)
    return _s_schema


def databaseFromString(s):
    """Create a database from a database-configuration string.

    The string must contain one or more :ref:`zodb
    <database-text-configuration>` sections.

    The database defined by the first section is returned.

    If :ref:`more than one zodb section is provided
    <multidatabase-text-configuration>`, a multi-database
    configuration will be created and all of the databases will be
    available in the returned database's ``databases`` attribute.
    """
    return databaseFromFile(StringIO(s))


def databaseFromFile(f):
    """Create a database from a file object that provides configuration.

    See :func:`databaseFromString`.
    """
    config, handle = ZConfig.loadConfigFile(getDbSchema(), f)
    return databaseFromConfig(config.database)


def databaseFromURL(url):
    """Load a database from URL (or file name) that provides configuration.

    See :func:`databaseFromString`.
    """
    config, handler = ZConfig.loadConfig(getDbSchema(), url)
    return databaseFromConfig(config.database)


def databaseFromConfig(database_factories):
    databases = {}
    first = None
    for factory in database_factories:
        db = factory.open(databases)
        if first is None:
            first = db

    return first


def storageFromString(s):
    """Create a storage from a storage-configuration string.
    """
    return storageFromFile(StringIO(s))


def storageFromFile(f):
    """Create a storage from a file object providing storage-configuration.
    """
    config, handle = ZConfig.loadConfigFile(getStorageSchema(), f)
    return storageFromConfig(config.storage)


def storageFromURL(url):
    """\
    Create a storage from a URL (or file name) providing storage-configuration.
    """
    config, handler = ZConfig.loadConfig(getStorageSchema(), url)
    return storageFromConfig(config.storage)


def storageFromConfig(section):
    return section.open()


class BaseConfig(object):
    """Object representing a configured storage or database.

    Methods:

    open() -- open and return the configured object

    Attributes:

    name   -- name of the storage

    """

    def __init__(self, config):
        self.config = config
        self.name = config.getSectionName()

    def open(self, database_name='unnamed', databases=None):
        """Open and return the storage object."""
        raise NotImplementedError


class ZODBDatabase(BaseConfig):

    def open(self, databases=None):
        section = self.config
        storage = section.storage.open()
        options = {}

        def _option(name, oname=None):
            v = getattr(section, name)
            if v is not None:
                if oname is None:
                    oname = name
                options[oname] = v

        _option('pool_timeout')
        _option('allow_implicit_cross_references', 'xrefs')
        _option('large_record_size')

        try:
            return ZODB.DB(
                storage,
                pool_size=section.pool_size,
                cache_size=section.cache_size,
                cache_size_bytes=section.cache_size_bytes,
                historical_pool_size=section.historical_pool_size,
                historical_cache_size=section.historical_cache_size,
                historical_cache_size_bytes=section.historical_cache_size_bytes,  # noqa: E501 line too long
                historical_timeout=section.historical_timeout,
                database_name=section.database_name or self.name or '',
                databases=databases,
                **options)
        except:  # noqa: E722 do not use bare 'except'
            storage.close()
            raise


class MappingStorage(BaseConfig):

    def open(self):
        from ZODB.MappingStorage import MappingStorage
        return MappingStorage(self.config.name)


class DemoStorage(BaseConfig):

    def open(self):
        base = changes = None
        for factory in self.config.factories:
            if factory.name == 'changes':
                changes = factory.open()
            else:
                if base is None:
                    base = factory.open()
                else:
                    raise ValueError("Too many base storages defined!")

        from ZODB.DemoStorage import DemoStorage
        return DemoStorage(self.config.name, base=base, changes=changes)


class FileStorage(BaseConfig):

    def open(self):
        from ZODB.FileStorage import FileStorage
        config = self.config
        options = {}
        if getattr(config, 'packer', None):
            packer = config.packer
            if ':' in packer:
                m, expr = packer.split(':', 1)
                m = __import__(m, {}, {}, ['*'])
                options['packer'] = eval(expr, m.__dict__)
            else:
                m, name = config.packer.rsplit('.', 1)
                m = __import__(m, {}, {}, ['*'])
                options['packer'] = getattr(m, name)

        for name in ('blob_dir', 'create', 'read_only', 'quota', 'pack_gc',
                     'pack_keep_old'):
            v = getattr(config, name, self)
            if v is not self:
                options[name] = v

        return FileStorage(config.path, **options)


class BlobStorage(BaseConfig):

    def open(self):
        from ZODB.blob import BlobStorage
        base = self.config.base.open()
        return BlobStorage(self.config.blob_dir, base)


class ZEOClient(BaseConfig):

    def open(self):
        from ZEO.ClientStorage import ClientStorage

        # config.server is a multikey of socket-connection-address values
        # where the value is a socket family, address tuple.
        L = [server.address for server in self.config.server]
        options = {}
        if self.config.blob_cache_size is not None:
            options['blob_cache_size'] = self.config.blob_cache_size
        if self.config.blob_cache_size_check is not None:
            options['blob_cache_size_check'] = (
                self.config.blob_cache_size_check)
        if self.config.client_label is not None:
            options['client_label'] = self.config.client_label

        return ClientStorage(
            L,
            blob_dir=self.config.blob_dir,
            shared_blob_dir=self.config.shared_blob_dir,
            storage=self.config.storage,
            cache_size=self.config.cache_size,
            name=self.config.name,
            client=self.config.client,
            var=self.config.var,
            min_disconnect_poll=self.config.min_disconnect_poll,
            max_disconnect_poll=self.config.max_disconnect_poll,
            wait=self.config.wait,
            read_only=self.config.read_only,
            read_only_fallback=self.config.read_only_fallback,
            drop_cache_rather_verify=self.config.drop_cache_rather_verify,
            username=self.config.username,
            password=self.config.password,
            realm=self.config.realm,
            **options)


class BDBStorage(BaseConfig):

    def open(self):
        from BDBStorage.BerkeleyBase import BerkeleyConfig
        storageclass = self.get_storageclass()
        bconf = BerkeleyConfig()
        for name in dir(BerkeleyConfig):
            if name.startswith('_'):
                continue
            setattr(bconf, name, getattr(self.config, name))
        return storageclass(self.config.envdir, config=bconf)


class BDBMinimalStorage(BDBStorage):

    def get_storageclass(self):
        import BDBStorage.BDBMinimalStorage
        return BDBStorage.BDBMinimalStorage.BDBMinimalStorage


class BDBFullStorage(BDBStorage):

    def get_storageclass(self):
        import BDBStorage.BDBFullStorage
        return BDBStorage.BDBFullStorage.BDBFullStorage
