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
"""Objects that can describe a ZConfig schema."""

import copy

import ZConfig


class UnboundedThing:
    __metaclass__ = type
    __slots__ = ()

    def __lt__(self, other):
        return False

    def __le__(self, other):
        return isinstance(other, self.__class__)

    def __gt__(self, other):
        return True

    def __ge__(self, other):
        return True

    def __eq__(self, other):
        return isinstance(other, self.__class__)

    def __ne__(self, other):
        return not isinstance(other, self.__class__)

    def __repr__(self):
        return "<Unbounded>"

Unbounded = UnboundedThing()


class ValueInfo:
    __metaclass__ = type
    __slots__ = 'value', 'position'

    def __init__(self, value, position):
        self.value = value
        # position is (lineno, colno, url)
        self.position = position

    def convert(self, datatype):
        try:
            return datatype(self.value)
        except ValueError, e:
            raise ZConfig.DataConversionError(e, self.value, self.position)


class BaseInfo:
    """Information about a single configuration key."""

    description = None
    example = None
    metadefault = None

    def __init__(self, name, datatype, minOccurs, maxOccurs, handler,
                 attribute):
        if maxOccurs is not None and maxOccurs < 1:
            if maxOccurs < 1:
                raise ZConfig.SchemaError(
                    "maxOccurs must be at least 1")
            if minOccurs is not None and minOccurs < maxOccurs:
                raise ZConfig.SchemaError(
                    "minOccurs must be at least maxOccurs")
        self.name = name
        self.datatype = datatype
        self.minOccurs = minOccurs
        self.maxOccurs = maxOccurs
        self.handler = handler
        self.attribute = attribute

    def __repr__(self):
        clsname = self.__class__.__name__
        return "<%s for %s>" % (clsname, `self.name`)

    def isabstract(self):
        return False

    def ismulti(self):
        return self.maxOccurs > 1

    def issection(self):
        return False


class BaseKeyInfo(BaseInfo):

    _rawdefaults = None

    def __init__(self, name, datatype, minOccurs, maxOccurs, handler,
                 attribute):
        assert minOccurs is not None
        BaseInfo.__init__(self, name, datatype, minOccurs, maxOccurs,
                          handler, attribute)
        self._finished = False

    def finish(self):
        if self._finished:
            raise ZConfig.SchemaError(
                "cannot finish KeyInfo more than once")
        self._finished = True

    def adddefault(self, value, position, key=None):
        if self._finished:
            raise ZConfig.SchemaError(
                "cannot add default values to finished KeyInfo")
        # Check that the name/keyed relationship is right:
        if self.name == "+" and key is None:
            raise ZConfig.SchemaError(
                "default values must be keyed for name='+'")
        elif self.name != "+" and key is not None:
            raise ZConfig.SchemaError(
                "unexpected key for default value")
        self.add_valueinfo(ValueInfo(value, position), key)

    def add_valueinfo(self, vi, key):
        """Actually add a ValueInfo to this key-info object.

        The appropriate value of None-ness of key has already been
        checked with regard to the name of the key, and has been found
        permissible to add.

        This method is a requirement for subclasses, and should not be
        called by client code.
        """
        raise NotImplementedError(
            "add_valueinfo() must be implemented by subclasses of BaseKeyInfo")

    def prepare_raw_defaults(self):
        assert self.name == "+"
        if self._rawdefaults is None:
            self._rawdefaults = self._default
        self._default = {}


class KeyInfo(BaseKeyInfo):

    _default = None

    def __init__(self, name, datatype, minOccurs, handler, attribute):
        BaseKeyInfo.__init__(self, name, datatype, minOccurs, 1,
                             handler, attribute)
        if self.name == "+":
            self._default = {}

    def add_valueinfo(self, vi, key):
        if self.name == "+":
            if self._default.has_key(key):
                # not ideal: we're presenting the unconverted
                # version of the key
                raise ZConfig.SchemaError(
                    "duplicate default value for key %s" % `key`)
            self._default[key] = vi
        elif self._default is not None:
            raise ZConfig.SchemaError(
                "cannot set more than one default to key with maxOccurs == 1")
        else:
            self._default = vi

    def computedefault(self, keytype):
        self.prepare_raw_defaults()
        for k, vi in self._rawdefaults.iteritems():
            key = ValueInfo(k, vi.position).convert(keytype)
            self.add_valueinfo(vi, key)

    def getdefault(self):
        # Use copy.copy() to make sure we don't allow polution of
        # our internal data without having to worry about both the
        # list and dictionary cases:
        return copy.copy(self._default)


class MultiKeyInfo(BaseKeyInfo):

    def __init__(self, name, datatype, minOccurs, maxOccurs, handler,
                 attribute):
        BaseKeyInfo.__init__(self, name, datatype, minOccurs, maxOccurs,
                             handler, attribute)
        if self.name == "+":
            self._default = {}
        else:
            self._default = []

    def add_valueinfo(self, vi, key):
        if self.name == "+":
            # This is a keyed value, not a simple value:
            if key in self._default:
                self._default[key].append(vi)
            else:
                self._default[key] = [vi]
        else:
            self._default.append(vi)

    def computedefault(self, keytype):
        self.prepare_raw_defaults()
        for k, vlist in self._rawdefaults.iteritems():
            key = ValueInfo(k, vlist[0].position).convert(keytype)
            for vi in vlist:
                self.add_valueinfo(vi, key)

    def getdefault(self):
        return copy.copy(self._default)


class SectionInfo(BaseInfo):
    def __init__(self, name, sectiontype, minOccurs, maxOccurs, handler,
                 attribute):
        # name        - name of the section; one of '*', '+', or name1
        # sectiontype - SectionType instance
        # minOccurs   - minimum number of occurances of the section
        # maxOccurs   - maximum number of occurances; if > 1, name
        #               must be '*' or '+'
        # handler     - handler name called when value(s) must take effect,
        #               or None
        # attribute   - name of the attribute on the SectionValue object
        if maxOccurs > 1:
            if name not in ('*', '+'):
                raise ZConfig.SchemaError(
                    "sections which can occur more than once must"
                    " use a name of '*' or '+'")
            if not attribute:
                raise ZConfig.SchemaError(
                    "sections which can occur more than once must"
                    " specify a target attribute name")
        if sectiontype.isabstract():
            datatype = None
        else:
            datatype = sectiontype.datatype
        BaseInfo.__init__(self, name, datatype,
                          minOccurs, maxOccurs, handler, attribute)
        self.sectiontype = sectiontype

    def __repr__(self):
        clsname = self.__class__.__name__
        return "<%s for %s (%s)>" % (
            clsname, self.sectiontype.name, `self.name`)

    def issection(self):
        return True

    def allowUnnamed(self):
        return self.name == "*"

    def isAllowedName(self, name):
        if name == "*" or name == "+":
            return False
        elif self.name == "+":
            return name and True or False
        elif self.name == "*":
            return True
        else:
            return name == self.name

    def getdefault(self):
        # sections cannot have defaults
        if self.maxOccurs > 1:
            return []
        else:
            return None


class AbstractType:
    __metaclass__ = type
    __slots__ = '_subtypes', 'name', 'description'

    def __init__(self, name):
        self._subtypes = {}
        self.name = name
        self.description = None

    def addsubtype(self, type):
        self._subtypes[type.name] = type

    def getsubtype(self, name):
        try:
            return self._subtypes[name]
        except KeyError:
            raise ZConfig.SchemaError("no sectiontype %s in abstracttype %s"
                                      % (`name`, `self.name`))

    def hassubtype(self, name):
        """Return true iff this type has 'name' as a concrete manifestation."""
        return name in self._subtypes.keys()

    def getsubtypenames(self):
        """Return the names of all concrete types as a sorted list."""
        L = self._subtypes.keys()
        L.sort()
        return L

    def isabstract(self):
        return True


class SectionType:
    def __init__(self, name, keytype, valuetype, datatype, registry, types):
        # name      - name of the section, or '*' or '+'
        # datatype  - type for the section itself
        # keytype   - type for the keys themselves
        # valuetype - default type for key values
        self.name = name
        self.datatype = datatype
        self.keytype = keytype
        self.valuetype = valuetype
        self.handler = None
        self.description = None
        self.registry = registry
        self._children = []    # [(key, info), ...]
        self._attrmap = {}     # {attribute: info, ...}
        self._keymap = {}      # {key: info, ...}
        self._types = types

    def gettype(self, name):
        n = name.lower()
        try:
            return self._types[n]
        except KeyError:
            raise ZConfig.SchemaError("unknown type name: " + `name`)

    def gettypenames(self):
        return self._types.keys()

    def __len__(self):
        return len(self._children)

    def __getitem__(self, index):
        return self._children[index]

    def _add_child(self, key, info):
        # check naming constraints
        assert key or info.attribute
        if key and self._keymap.has_key(key):
            raise ZConfig.SchemaError(
                "child name %s already used" % key)
        if info.attribute and self._attrmap.has_key(info.attribute):
            raise ZConfig.SchemaError(
                "child attribute name %s already used" % info.attribute)
        # a-ok, add the item to the appropriate maps
        if info.attribute:
            self._attrmap[info.attribute] = info
        if key:
            self._keymap[key] = info
        self._children.append((key, info))

    def addkey(self, keyinfo):
        self._add_child(keyinfo.name, keyinfo)

    def addsection(self, name, sectinfo):
        assert name not in ("*", "+")
        self._add_child(name, sectinfo)

    def getinfo(self, key):
        if not key:
            raise ZConfig.ConfigurationError(
                "cannot match a key without a name")
        try:
            return self._keymap[key]
        except KeyError:
            raise ZConfig.ConfigurationError("no key matching " + `key`)

    def getrequiredtypes(self):
        d = {}
        if self.name:
            d[self.name] = 1
        stack = [self]
        while stack:
            info = stack.pop()
            for key, ci in info._children:
                if ci.issection():
                    t = ci.sectiontype
                    if not d.has_key(t.name):
                        d[t.name] = 1
                        stack.append(t)
        return d.keys()

    def getsectioninfo(self, type, name):
        for key, info in self._children:
            if key:
                if key == name:
                    if not info.issection():
                        raise ZConfig.ConfigurationError(
                            "section name %s already in use for key" % key)
                    st = info.sectiontype
                    if st.isabstract():
                        try:
                            st = st.getsubtype(type)
                        except ZConfig.ConfigurationError:
                            raise ZConfig.ConfigurationError(
                                "section type %s not allowed for name %s"
                                % (`type`, `key`))
                    if not st.name == type:
                        raise ZConfig.ConfigurationError(
                            "name %s must be used for a %s section"
                            % (`name`, `st.name`))
                    return info
            # else must be a sectiontype or an abstracttype:
            elif info.sectiontype.name == type:
                if not (name or info.allowUnnamed()):
                    raise ZConfig.ConfigurationError(
                        `type` + " sections must be named")
                return info
            elif info.sectiontype.isabstract():
                st = info.sectiontype
                if st.name == type:
                    raise ZConfig.ConfigurationError(
                        "cannot define section with an abstract type")
                try:
                    st = st.getsubtype(type)
                except ZConfig.ConfigurationError:
                    # not this one; maybe a different one
                    pass
                else:
                    return info
        raise ZConfig.ConfigurationError(
            "no matching section defined for type='%s', name='%s'" % (
            type, name))

    def isabstract(self):
        return False


class SchemaType(SectionType):
    def __init__(self, keytype, valuetype, datatype, handler, url,
                 registry):
        SectionType.__init__(self, None, keytype, valuetype, datatype,
                             registry, {})
        self._components = {}
        self.handler = handler
        self.url = url

    def addtype(self, typeinfo):
        n = typeinfo.name
        if self._types.has_key(n):
            raise ZConfig.SchemaError("type name cannot be redefined: "
                                      + `typeinfo.name`)
        self._types[n] = typeinfo

    def allowUnnamed(self):
        return True

    def isAllowedName(self, name):
        return False

    def issection(self):
        return True

    def getunusedtypes(self):
        alltypes = self.gettypenames()
        reqtypes = self.getrequiredtypes()
        for n in reqtypes:
            alltypes.remove(n)
        if self.name and self.name in alltypes:
            alltypes.remove(self.name)
        return alltypes

    def createSectionType(self, name, keytype, valuetype, datatype):
        t = SectionType(name, keytype, valuetype, datatype,
                        self.registry, self._types)
        self.addtype(t)
        return t

    def deriveSectionType(self, base, name, keytype, valuetype, datatype):
        if isinstance(base, SchemaType):
            raise ZConfig.SchemaError(
                "cannot derive sectiontype from top-level schema")
        t = self.createSectionType(name, keytype, valuetype, datatype)
        t._attrmap.update(base._attrmap)
        t._keymap.update(base._keymap)
        t._children.extend(base._children)
        for i in range(len(t._children)):
            key, info = t._children[i]
            if isinstance(info, BaseKeyInfo) and info.name == "+":
                # need to create a new info object and recompute the
                # default mapping based on the new keytype
                info = copy.copy(info)
                info.computedefault(t.keytype)
                t._children[i] = (key, info)
        return t

    def addComponent(self, name):
        if self._components.has_key(name):
            raise ZConfig.SchemaError("already have component %s" % name)
        self._components[name] = name

    def hasComponent(self, name):
        return self._components.has_key(name)


def createDerivedSchema(base):
    new = SchemaType(base.keytype, base.valuetype, base.datatype,
                     base.handler, base.url, base.registry)
    new._components.update(base._components)
    new.description = base.description
    new._children[:] = base._children
    new._attrmap.update(base._attrmap)
    new._keymap.update(base._keymap)
    new._types.update(base._types)
    return new
