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
"""Parser for ZConfig schemas."""

import os
import xml.sax

import ZConfig

from ZConfig import info
from ZConfig import url


def parseResource(resource, loader):
    parser = SchemaParser(loader, resource.url)
    xml.sax.parse(resource.file, parser)
    return parser._schema


def parseComponent(resource, loader, schema):
    parser = ComponentParser(loader, resource.url, schema)
    xml.sax.parse(resource.file, parser)


def _srepr(ob):
    if isinstance(ob, type(u'')):
        # drop the leading "u" from a unicode repr
        return `ob`[1:]
    else:
        return `ob`


class BaseParser(xml.sax.ContentHandler):

    _cdata_tags = "description", "metadefault", "example", "default"
    _handled_tags = ("import", "abstracttype", "sectiontype",
                     "key", "multikey", "section", "multisection")

    _allowed_parents = {
        "description": ["key", "section", "multikey", "multisection",
                        "sectiontype", "abstracttype",
                        "schema", "component"],
        "example": ["key", "section", "multikey", "multisection"],
        "metadefault": ["key", "section", "multikey", "multisection"],
        "default": ["key", "multikey"],
        "import": ["schema", "component"],
        "abstracttype": ["schema", "component"],
        "sectiontype": ["schema", "component"],
        "key": ["schema", "sectiontype"],
        "multikey": ["schema", "sectiontype"],
        "section": ["schema", "sectiontype"],
        "multisection": ["schema", "sectiontype"],
        }

    def __init__(self, loader, url):
        self._registry = loader.registry
        self._loader = loader
        self._basic_key = self._registry.get("basic-key")
        self._identifier = self._registry.get("identifier")
        self._cdata = None
        self._locator = None
        self._prefixes = []
        self._schema = None
        self._stack = []
        self._url = url
        self._elem_stack = []

    # SAX 2 ContentHandler methods

    def setDocumentLocator(self, locator):
        self._locator = locator

    def startElement(self, name, attrs):
        attrs = dict(attrs)
        if self._elem_stack:
            parent = self._elem_stack[-1]
            if not self._allowed_parents.has_key(name):
                self.error("Unknown tag " + name)
            if parent not in self._allowed_parents[name]:
                self.error("%s elements may not be nested in %s elements"
                           % (_srepr(name), _srepr(parent)))
        elif name != self._top_level:
            self.error("Unknown document type " + name)
        self._elem_stack.append(name)
        if name == self._top_level:
            if self._schema is not None:
                self.error("schema element improperly nested")
            getattr(self, "start_" + name)(attrs)
        elif name in self._handled_tags:
            if self._schema is None:
                self.error(name + " element outside of schema")
            getattr(self, "start_" + name)(attrs)
        elif name in self._cdata_tags:
            if self._schema is None:
                self.error(name + " element outside of schema")
            if self._cdata is not None:
                self.error(name + " element improperly nested")
            self._cdata = []
            self._position = None
            self._attrs = attrs

    def characters(self, data):
        if self._cdata is not None:
            if self._position is None:
                self._position = self.get_position()
            self._cdata.append(data)
        elif data.strip():
            self.error("unexpected non-blank character data: "
                       + `data.strip()`)

    def endElement(self, name):
        del self._elem_stack[-1]
        if name in self._handled_tags:
            getattr(self, "end_" + name)()
        else:
            data = ''.join(self._cdata).strip()
            self._cdata = None
            getattr(self, "characters_" + name)(data)

    def endDocument(self):
        if self._schema is None:
            self.error("no %s found" % self._top_level)

    # helper methods

    def get_position(self):
        if self._locator:
            return (self._locator.getLineNumber(),
                    self._locator.getColumnNumber(),
                    (self._locator.getSystemId() or self._url))
        else:
            return None, None, self._url

    def get_handler(self, attrs):
        v = attrs.get("handler")
        if v is None:
            return v
        else:
            return self.basic_key(v)

    def push_prefix(self, attrs):
        name = attrs.get("prefix")
        if name:
            if self._prefixes:
                convert = self._registry.get("dotted-suffix")
            else:
                convert = self._registry.get("dotted-name")
            try:
                name = convert(name)
            except ValueError, err:
                self.error("not a valid prefix: %s (%s)"
                           % (_srepr(name), str(err)))
            if name[0] == ".":
                prefix = self._prefixes[-1] + name
            else:
                prefix = name
        elif self._prefixes:
            prefix = self._prefixes[-1]
        else:
            prefix = ''
        self._prefixes.append(prefix)

    def pop_prefix(self):
        del self._prefixes[-1]

    def get_classname(self, name):
        name = str(name)
        if name.startswith("."):
            return self._prefixes[-1] + name
        else:
            return name

    def get_datatype(self, attrs, attrkey, default, base=None):
        if attrs.has_key(attrkey):
            dtname = self.get_classname(attrs[attrkey])
        else:
            convert = getattr(base, attrkey, None)
            if convert is not None:
                return convert
            dtname = default

        try:
            return self._registry.get(dtname)
        except ValueError, e:
            self.error(e[0])

    def get_sect_typeinfo(self, attrs, base=None):
        keytype = self.get_datatype(attrs, "keytype", "basic-key", base)
        valuetype = self.get_datatype(attrs, "valuetype", "string")
        datatype = self.get_datatype(attrs, "datatype", "null", base)
        return keytype, valuetype, datatype

    def get_required(self, attrs):
        if attrs.has_key("required"):
            v = attrs["required"]
            if v == "yes":
                return True
            elif v == "no":
                return False
            self.error("value for 'required' must be 'yes' or 'no'")
        else:
            return False

    def get_ordinality(self, attrs):
        min, max = 0, info.Unbounded
        if self.get_required(attrs):
            min = 1
        return min, max

    def get_sectiontype(self, attrs):
        type = attrs.get("type")
        if not type:
            self.error("section must specify type")
        return self._schema.gettype(type)

    def get_key_info(self, attrs, element):
        any, name, attribute = self.get_name_info(attrs, element)
        if any == '*':
            self.error(element + " may not specify '*' for name")
        if not name and any != '+':
            self.error(element + " name may not be omitted or empty")
        datatype = self.get_datatype(attrs, "datatype", "string")
        handler = self.get_handler(attrs)
        return name or any, datatype, handler, attribute

    def get_name_info(self, attrs, element):
        name = attrs.get("name")
        if not name:
            self.error(element + " name must be specified and non-empty")
        aname = attrs.get("attribute")
        if aname:
            aname = self.identifier(aname)
            if aname.startswith("getSection"):
                # reserved; used for SectionValue methods to get meta-info
                self.error("attribute names may not start with 'getSection'")
        if name in ("*", "+"):
            if not aname:
                self.error(
                    "container attribute must be specified and non-empty"
                    " when using '*' or '+' for a section name")
            return name, None, aname
        else:
            # run the keytype converter to make sure this is a valid key
            try:
                name = self._stack[-1].keytype(name)
            except ValueError, e:
                self.error("could not convert key name to keytype: " + str(e))
            if not aname:
                aname = self.basic_key(name)
                aname = self.identifier(aname.replace('-', '_'))
            return None, name, aname

    # schema loading logic

    def characters_default(self, data):
        key = self._attrs.get("key")
        self._stack[-1].adddefault(data, self._position, key)

    def characters_description(self, data):
        if self._stack[-1].description is not None:
            self.error(
                "at most one <description> may be used for each element")
        self._stack[-1].description = data

    def characters_example(self, data):
        self._stack[-1].example = data

    def characters_metadefault(self, data):
        self._stack[-1].metadefault = data

    def start_import(self, attrs):
        src = attrs.get("src", "").strip()
        pkg = attrs.get("package", "").strip()
        file = attrs.get("file", "").strip()
        if not (src or pkg):
            self.error("import must specify either src or package")
        if src and pkg:
            self.error("import may only specify one of src or package")
        if src:
            if file:
                self.error("import may not specify file and src")
            src = url.urljoin(self._url, src)
            src, fragment = url.urldefrag(src)
            if fragment:
                self.error("import src many not include"
                           " a fragment identifier")
            schema = self._loader.loadURL(src)
            for n in schema.gettypenames():
                self._schema.addtype(schema.gettype(n))
        else:
            if os.path.dirname(file):
                self.error("file may not include a directory part")
            pkg = self.get_classname(pkg)
            src = self._loader.schemaComponentSource(pkg, file)
            if not self._schema.hasComponent(src):
                self._schema.addComponent(src)
                self.loadComponent(src)

    def loadComponent(self, src):
        r = self._loader.openResource(src)
        parser = ComponentParser(self._loader, src, self._schema)
        try:
            xml.sax.parse(r.file, parser)
        finally:
            r.close()

    def end_import(self):
        pass

    def start_sectiontype(self, attrs):
        name = attrs.get("name")
        if not name:
            self.error("sectiontype name must not be omitted or empty")
        name = self.basic_key(name)
        self.push_prefix(attrs)
        if attrs.has_key("extends"):
            basename = self.basic_key(attrs["extends"])
            base = self._schema.gettype(basename)
            if base.isabstract():
                self.error("sectiontype cannot extend an abstract type")
            keytype, valuetype, datatype = self.get_sect_typeinfo(attrs, base)
            sectinfo = self._schema.deriveSectionType(
                base, name, keytype, valuetype, datatype)
        else:
            keytype, valuetype, datatype = self.get_sect_typeinfo(attrs)
            sectinfo = self._schema.createSectionType(
                name, keytype, valuetype, datatype)
        if attrs.has_key("implements"):
            ifname = self.basic_key(attrs["implements"])
            interface = self._schema.gettype(ifname)
            if not interface.isabstract():
                self.error(
                    "type specified by implements is not an abstracttype")
            interface.addsubtype(sectinfo)
        self._stack.append(sectinfo)

    def end_sectiontype(self):
        self.pop_prefix()
        self._stack.pop()

    def start_section(self, attrs):
        sectiontype = self.get_sectiontype(attrs)
        handler = self.get_handler(attrs)
        min = self.get_required(attrs) and 1 or 0
        any, name, attribute = self.get_name_info(attrs, "section")
        if any and not attribute:
            self.error(
                "attribute must be specified if section name is '*' or '+'")
        section = info.SectionInfo(any or name, sectiontype,
                                   min, 1, handler, attribute)
        self._stack[-1].addsection(name, section)
        self._stack.append(section)

    def end_section(self):
        self._stack.pop()

    def start_multisection(self, attrs):
        sectiontype = self.get_sectiontype(attrs)
        min, max = self.get_ordinality(attrs)
        any, name, attribute = self.get_name_info(attrs, "multisection")
        if any not in ("*", "+"):
            self.error("multisection must specify '*' or '+' for the name")
        handler = self.get_handler(attrs)
        section = info.SectionInfo(any or name, sectiontype,
                                   min, max, handler, attribute)
        self._stack[-1].addsection(name, section)
        self._stack.append(section)

    def end_multisection(self):
        self._stack.pop()

    def start_abstracttype(self, attrs):
        name = attrs.get("name")
        if not name:
            self.error("abstracttype name must not be omitted or empty")
        name = self.basic_key(name)
        abstype = info.AbstractType(name)
        self._schema.addtype(abstype)
        self._stack.append(abstype)

    def end_abstracttype(self):
        self._stack.pop()

    def start_key(self, attrs):
        name, datatype, handler, attribute = self.get_key_info(attrs, "key")
        min = self.get_required(attrs) and 1 or 0
        key = info.KeyInfo(name, datatype, min, handler, attribute)
        if attrs.has_key("default"):
            if min:
                self.error("required key cannot have a default value")
            key.adddefault(str(attrs["default"]).strip(),
                           self.get_position())
        if name != "+":
            key.finish()
        self._stack[-1].addkey(key)
        self._stack.append(key)

    def end_key(self):
        key = self._stack.pop()
        if key.name == "+":
            key.computedefault(self._stack[-1].keytype)
            key.finish()

    def start_multikey(self, attrs):
        if attrs.has_key("default"):
            self.error("default values for multikey must be given using"
                       " 'default' elements")
        name, datatype, handler, attribute = self.get_key_info(attrs,
                                                               "multikey")
        min, max = self.get_ordinality(attrs)
        key = info.MultiKeyInfo(name, datatype, min, max, handler, attribute)
        self._stack[-1].addkey(key)
        self._stack.append(key)

    def end_multikey(self):
        multikey = self._stack.pop()
        if multikey.name == "+":
            multikey.computedefault(self._stack[-1].keytype)
        multikey.finish()

    # datatype conversion wrappers

    def basic_key(self, s):
        try:
            return self._basic_key(s)
        except ValueError, e:
            self.error(e[0])

    def identifier(self, s):
        try:
            return self._identifier(s)
        except ValueError, e:
            self.error(e[0])

    # exception setup helpers

    def initerror(self, e):
        if self._locator is not None:
            e.colno = self._locator.getColumnNumber()
            e.lineno = self._locator.getLineNumber()
            e.url = self._locator.getSystemId()
        return e

    def error(self, message):
        raise self.initerror(ZConfig.SchemaError(message))


class SchemaParser(BaseParser):

    # needed by startElement() and endElement()
    _handled_tags = BaseParser._handled_tags + ("schema",)
    _top_level = "schema"

    def __init__(self, loader, url, extending_parser=None):
        BaseParser.__init__(self, loader, url)
        self._extending_parser = extending_parser
        self._base_keytypes = []
        self._base_datatypes = []

    def start_schema(self, attrs):
        self.push_prefix(attrs)
        handler = self.get_handler(attrs)
        keytype, valuetype, datatype = self.get_sect_typeinfo(attrs)

        if self._extending_parser is None:
            # We're not being inherited, so we need to create the schema
            self._schema = info.SchemaType(keytype, valuetype, datatype,
                                           handler, self._url, self._registry)
        else:
            # Parse into the extending ("subclass") parser's schema
            self._schema = self._extending_parser._schema

        self._stack = [self._schema]

        if attrs.has_key("extends"):
            sources = attrs["extends"].split()
            sources.reverse()

            for src in sources:
                src = url.urljoin(self._url, src)
                src, fragment = url.urldefrag(src)
                if fragment:
                    self.error("schema extends many not include"
                               " a fragment identifier")
                self.extendSchema(src)

            # Inherit keytype from bases, if unspecified and not conflicting
            if self._base_keytypes and not attrs.has_key("keytype"):
                keytype = self._base_keytypes[0]
                for kt in self._base_keytypes[1:]:
                    if kt is not keytype:
                        self.error("base schemas have conflicting keytypes,"
                                   " but no keytype was specified in the"
                                   " extending schema")

            # Inherit datatype from bases, if unspecified and not conflicting
            if self._base_datatypes and not attrs.has_key("datatype"):
                datatype = self._base_datatypes[0]
                for dt in self._base_datatypes[1:]:
                    if dt is not datatype:
                        self.error("base schemas have conflicting datatypes,"
                                   " but no datatype was specified in the"
                                   " extending schema")

        # Reset the schema types to our own, while we parse the schema body
        self._schema.keytype = keytype
        self._schema.valuetype = valuetype
        self._schema.datatype = datatype

        # Update base key/datatypes for the "extending" parser
        if self._extending_parser is not None:
            self._extending_parser._base_keytypes.append(keytype)
            self._extending_parser._base_datatypes.append(datatype)


    def extendSchema(self,src):
        parser = SchemaParser(self._loader, src, self)
        r = self._loader.openResource(src)
        try:
            xml.sax.parse(r.file, parser)
        finally:
            r.close()

    def end_schema(self):
        del self._stack[-1]
        assert not self._stack
        self.pop_prefix()
        assert not self._prefixes


class ComponentParser(BaseParser):

    _handled_tags = BaseParser._handled_tags + ("component",)
    _top_level = "component"

    def __init__(self, loader, url, schema):
        BaseParser.__init__(self, loader, url)
        self._parent = schema

    def characters_description(self, data):
        if self._stack:
            self._stack[-1].description = data

    def start_key(self, attrs):
        self._check_not_toplevel("key")
        BaseParser.start_key(self, attrs)

    def start_multikey(self, attrs):
        self._check_not_toplevel("multikey")
        BaseParser.start_multikey(self, attrs)

    def start_section(self, attrs):
        self._check_not_toplevel("section")
        BaseParser.start_section(self, attrs)

    def start_multisection(self, attrs):
        self._check_not_toplevel("multisection")
        BaseParser.start_multisection(self, attrs)

    def start_component(self, attrs):
        self._schema = self._parent
        self.push_prefix(attrs)

    def end_component(self):
        self.pop_prefix()

    def _check_not_toplevel(self, what):
        if not self._stack:
            self.error("cannot define top-level %s in a schema %s"
                       % (what, self._top_level))
