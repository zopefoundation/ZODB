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
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Default storage types.

Adapted from DBTab/StorageTypes.py.
"""

import re

from ZConfig.Config import asBoolean


def convertFileStorageArgs(quota=None, stop=None, **kw):
    if kw.has_key('name'):
        # FileStorage doesn't accept a 'name' arg
        del kw['name']
    if quota is not None:
        kw['quota'] = long(quota) or None
    if stop is not None:
        stop = long(stop)
        if not stop:
            stop = None
        else:
            from ZODB.utils import p64
            stop = p64(stop)
        kw['stop'] = stop

    # Boolean args
    for name in (
        'create', 'read_only'
        ):
        if kw.has_key(name):
            kw[name] = asBoolean(kw[name])

    return kw


# Match URLs of the form 'zeo://zope.example.com:1234'
zeo_url_re = re.compile('zeo:/*(?P<host>[A-Za-z0-9\.-]+):(?P<port>[0-9]+)')

def convertAddresses(s):
    # Allow multiple addresses using semicolons as a split character.
    res = []
    for a in s.split(';'):
        a = a.strip()
        if a:
            mo = zeo_url_re.match(a)
            if mo is not None:
                # ZEO URL
                host, port = mo.groups()
                res.append((host, int(port)))
            else:
                # Socket file
                res.append(a)
    return res


def convertClientStorageArgs(addr=None, **kw):
    if addr is None:
        raise RuntimeError, 'An addr parameter is required for ClientStorage.'
    kw['addr'] = convertAddresses(addr)

    # Integer args
    for name in (
        'cache_size', 'min_disconnect_poll', 'max_disconnect_poll',
        ):
        if kw.has_key(name):
            kw[name] = int(kw[name])

    # Boolean args
    for name in (
        'wait', 'read_only', 'read_only_fallback',
        ):
        if kw.has_key(name):
            kw[name] = asBoolean(kw[name])

    # The 'client' parameter must be None to be false.  Yuck.
    if kw.has_key('client') and not kw['client']:
        kw['client'] = None

    return kw


def convertBDBStorageArgs(**kw):
    from bsddb3Storage.BerkeleyBase import BerkeleyConfig
    config = BerkeleyConfig()
    for name in dir(BerkeleyConfig):
        if name.startswith('_'):
            continue
        val = kw.get(name)
        if val is not None:
            if name != 'logdir':
                val = int(val)
            setattr(config, name, val)
            del kw[name]
    # XXX: Nobody ever passes in env
    assert not kw.has_key('env')
    kw['config'] = config
    return kw


storage_types = {
    'FileStorage': ('ZODB.FileStorage', convertFileStorageArgs),
    'DemoStorage': ('ZODB.DemoStorage', None),
    'MappingStorage': ('ZODB.MappingStorage', None),
    'TemporaryStorage': ('Products.TemporaryFolder.TemporaryStorage', None),
    'ClientStorage': ('ZEO.ClientStorage', convertClientStorageArgs),
    'Full': ('bsddb3Storage.Full', convertBDBStorageArgs),
    'Minimal': ('bsddb3Storage.Minimal', convertBDBStorageArgs),
    }
