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
"""Higher-level support for configuring storages.

Storages are configured a la DBTab.

A storage section has the form

  <Storage Name (dependent)>
    # For example
    type        FileStorage
    file_name   var/Data.fs
    read_only   1
  </Storage>

where Name and (dependent) are optional.  Once you have retrieved the
section object (probably with getSection("Storage", name), the
function creatStorage() in this module will create the storage object
for you.
"""

from StorageTypes import storage_types

def createStorage(section):
    """Create a storage specified by a configuration section."""
    klass, args = getStorageInfo(section)
    return klass(**args)

def getStorageInfo(section):
    """Extract a storage description from a configuration section.

    Return a tuple (klass, args) where klass is the storage class and
    args is a dictionary of keyword arguments.  To create the storage,
    call klass(**args).

    Adapted from DatabaseFactory.setStorageParams() in DBTab.py.
    """
    type = section.get("type")
    if not type:
        raise RuntimeError, "A storage type is required"
    module = None
    pos = type.rfind(".")
    if pos >= 0:
        # Specified the module
        module, type = type[:pos], type[pos+1:]
    converter = None
    if not module:
        # Use a default module and argument converter.
        info = storage_types.get(type)
        if not info:
            raise RuntimeError, "Unknown storage type: %s" % type
        module, converter = info
    m = __import__(module, {}, {}, [type])
    klass = getattr(m, type)

    args = {}
    for key in section.keys():
        if key.lower() != "type":
            args[key] = section.get(key)
    if converter is not None:
        args = converter(**args)
    return (klass, args)
