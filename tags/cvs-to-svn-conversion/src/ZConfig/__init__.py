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
"""Configuration data structures and loader for the ZRS.

$Id: __init__.py,v 1.18 2004/04/15 20:33:32 fdrake Exp $
"""
version_info = (2, 2)
__version__ = ".".join([str(n) for n in version_info])

from ZConfig.loader import loadConfig, loadConfigFile
from ZConfig.loader import loadSchema, loadSchemaFile


class ConfigurationError(Exception):
    """Base class for ZConfig exceptions."""

    def __init__(self, msg, url=None):
        self.message = msg
        self.url = url
        Exception.__init__(self, msg)

    def __str__(self):
        return self.message


class _ParseError(ConfigurationError):
    def __init__(self, msg, url, lineno, colno=None):
        self.lineno = lineno
        self.colno = colno
        ConfigurationError.__init__(self, msg, url)

    def __str__(self):
        s = self.message
        if self.url:
            s += "\n("
        elif (self.lineno, self.colno) != (None, None):
            s += " ("
        if self.lineno:
            s += "line %d" % self.lineno
            if self.colno is not None:
                s += ", column %d" % self.colno
            if self.url:
                s += " in %s)" % self.url
            else:
                s += ")"
        elif self.url:
            s += self.url + ")"
        return s


class SchemaError(_ParseError):
    """Raised when there's an error in the schema itself."""

    def __init__(self, msg, url=None, lineno=None, colno=None):
        _ParseError.__init__(self, msg, url, lineno, colno)


class SchemaResourceError(SchemaError):
    """Raised when there's an error locating a resource required by the schema.
    """

    def __init__(self, msg, url=None, lineno=None, colno=None,
                 path=None, package=None, filename=None):
        self.filename = filename
        self.package = package
        if path is not None:
            path = path[:]
        self.path = path
        SchemaError.__init__(self, msg, url, lineno, colno)

    def __str__(self):
        s = SchemaError.__str__(self)
        if self.package is not None:
            s += "\n  Package name: " + repr(self.package)
        if self.filename is not None:
            s += "\n  File name: " + repr(self.filename)
        if self.package is not None:
            s += "\n  Package path: " + repr(self.path)
        return s


class ConfigurationSyntaxError(_ParseError):
    """Raised when there's a syntax error in a configuration file."""


class DataConversionError(ConfigurationError, ValueError):
    """Raised when a data type conversion function raises ValueError."""

    def __init__(self, exception, value, position):
        ConfigurationError.__init__(self, str(exception))
        self.exception = exception
        self.value = value
        self.lineno, self.colno, self.url = position

    def __str__(self):
        s = "%s (line %s" % (self.message, self.lineno)
        if self.colno is not None:
            s += ", %s" % self.colno
        if self.url:
            s += ", in %s)" % self.url
        else:
            s += ")"
        return s


class SubstitutionSyntaxError(ConfigurationError):
    """Raised when interpolation source text contains syntactical errors."""


class SubstitutionReplacementError(ConfigurationSyntaxError, LookupError):
    """Raised when no replacement is available for a reference."""

    def __init__(self, source, name, url=None, lineno=None):
        self.source = source
        self.name = name
        ConfigurationSyntaxError.__init__(
            self, "no replacement for " + `name`, url, lineno)
