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

"""Support for command-line provision of settings.

This module provides an extension of the ConfigLoader class which adds
a way to add configuration settings from an alternate source.  Each
setting is described by a string of the form::

    some/path/to/key=value
"""

import ZConfig
import ZConfig.loader
import ZConfig.matcher


class ExtendedConfigLoader(ZConfig.loader.ConfigLoader):
    def __init__(self, schema):
        ZConfig.loader.ConfigLoader.__init__(self, schema)
        self.clopts = []   # [(optpath, value, source-position), ...]

    def addOption(self, spec, pos=None):
        if pos is None:
            pos = "<command-line option>", -1, -1
        if "=" not in spec:
            e = ZConfig.ConfigurationSyntaxError(
                "invalid configuration specifier", *pos)
            e.specifier = spec
            raise e
        # For now, just add it to the list; not clear that checking
        # against the schema at this point buys anything.
        opt, val = spec.split("=", 1)
        optpath = opt.split("/")
        if "" in optpath:
            # // is not allowed in option path
            e = ZConfig.ConfigurationSyntaxError(
                "'//' is not allowed in an option path", *pos)
            e.specifier = spec
            raise e
        self.clopts.append((optpath, val, pos))

    def createSchemaMatcher(self):
        if self.clopts:
            sm = ExtendedSchemaMatcher(self.schema)
            sm.set_optionbag(self.cook())
        else:
            sm = ZConfig.loader.ConfigLoader.createSchemaMatcher(self)
        return sm

    def cook(self):
        if self.clopts:
            return OptionBag(self.schema, self.schema, self.clopts)
        else:
            return None


class OptionBag:
    def __init__(self, schema, sectiontype, options):
        self.sectiontype = sectiontype
        self.schema = schema
        self.keypairs = {}
        self.sectitems = []
        self._basic_key = schema.registry.get("basic-key")
        for item in options:
            optpath, val, pos = item
            name = sectiontype.keytype(optpath[0])
            if len(optpath) == 1:
                self.add_value(name, val, pos)
            else:
                self.sectitems.append(item)

    def basic_key(self, s, pos):
        try:
            return self._basic_key(s)
        except ValueError:
            raise ZConfig.ConfigurationSyntaxError(
                "could not convert basic-key value", *pos)

    def add_value(self, name, val, pos):
        if self.keypairs.has_key(name):
            L = self.keypairs[name]
        else:
            L = []
            self.keypairs[name] = L
        L.append((val, pos))

    def has_key(self, name):
        return self.keypairs.has_key(name)

    def get_key(self, name):
        """Return a list of (value, pos) items for the key 'name'.

        The returned list may be empty.
        """
        L = self.keypairs.get(name)
        if L:
            del self.keypairs[name]
            return L
        else:
            return []

    def keys(self):
        return self.keypairs.keys()

    def get_section_info(self, type, name):
        L = []  # what pertains to the child section
        R = []  # what we keep
        for item in self.sectitems:
            optpath, val, pos = item
            s = optpath[0]
            bk = self.basic_key(s, pos)
            if name and s.lower() == name:
                L.append((optpath[1:], val, pos))
            elif bk == type:
                L.append((optpath[1:], val, pos))
            else:
                R.append(item)
        if L:
            self.sectitems[:] = R
            return OptionBag(self.schema, self.schema.gettype(type), L)
        else:
            return None

    def finish(self):
        if self.sectitems or self.keypairs:
            raise ZConfig.ConfigurationError(
                "not all command line options were consumed")


class MatcherMixin:
    def set_optionbag(self, bag):
        self.optionbag = bag

    def addValue(self, key, value, position):
        try:
            realkey = self.type.keytype(key)
        except ValueError, e:
            raise ZConfig.DataConversionError(e, key, position)
        if self.optionbag.has_key(realkey):
            return
        ZConfig.matcher.BaseMatcher.addValue(self, key, value, position)

    def createChildMatcher(self, type, name):
        sm = ZConfig.matcher.BaseMatcher.createChildMatcher(self, type, name)
        bag = self.optionbag.get_section_info(type.name, name)
        if bag is not None:
            sm = ExtendedSectionMatcher(
                sm.info, sm.type, sm.name, sm.handlers)
            sm.set_optionbag(bag)
        return sm

    def finish_optionbag(self):
        for key in self.optionbag.keys():
            for val, pos in self.optionbag.get_key(key):
                ZConfig.matcher.BaseMatcher.addValue(self, key, val, pos)
        self.optionbag.finish()


class ExtendedSectionMatcher(MatcherMixin, ZConfig.matcher.SectionMatcher):
    def finish(self):
        self.finish_optionbag()
        return ZConfig.matcher.SectionMatcher.finish(self)

class ExtendedSchemaMatcher(MatcherMixin, ZConfig.matcher.SchemaMatcher):
    def finish(self):
        self.finish_optionbag()
        return ZConfig.matcher.SchemaMatcher.finish(self)
