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

"""ZConfig datatypes for logging support."""

import sys

from zLOG.factory import Factory

# log-related datatypes

_logging_levels = {
    "critical": 50,
    "fatal": 50,
    "error": 40,
    "warn": 30,
    "info": 20,
    "debug": 10,
    "all": 0,
    }

def logging_level(value):
    s = str(value).lower()
    if _logging_levels.has_key(s):
        return _logging_levels[s]
    else:
        v = int(s)
        if v < 0 or v > 50:
            raise ValueError("log level not in range: " + `v`)
        return v

_log_format_variables = {
    'name': '',
    'levelno': '3',
    'levelname': 'DEBUG',
    'pathname': 'apath',
    'filename': 'afile',
    'module': 'amodule',
    'lineno': 1,
    'created': 1.1,
    'asctime': 'atime',
    'msecs': 1,
    'relativeCreated': 1,
    'thread': 1,
    'message': 'amessage',
    }

def log_format(value):
    value = ctrl_char_insert(value)
    try:
        # Make sure the format string uses only names that will be
        # provided, and has reasonable type flags for each, and does
        # not expect positional args.
        value % _log_format_variables
    except (ValueError, KeyError):
        raise ValueError, 'Invalid log format string %s' % value
    return value

_control_char_rewrites = {r'\n': '\n', r'\t': '\t', r'\b': '\b',
                          r'\f': '\f', r'\r': '\r'}.items()

def ctrl_char_insert(value):
    for pattern, replacement in _control_char_rewrites:
        value = value.replace(pattern, replacement)
    return value

def file_handler(section):
    path = section.path

    def callback(inst,
                 format=section.format,
                 dateformat=section.dateformat,
                 level=section.level):
        import logging
        inst.setFormatter(logging.Formatter(format, dateformat))
        inst.setLevel(level)

    if path == "STDERR":
        # XXX should pick up sys.stderr when the factory is invoked
        return Factory('zLOG.LogHandlers.StreamHandler', callback, sys.stderr)
    else:
        return Factory('zLOG.LogHandlers.FileHandler', callback, path)

_syslog_facilities = {
    "auth": 1,
    "authpriv": 1,
    "cron": 1,
    "daemon": 1,
    "kern": 1,
    "lpr": 1,
    "mail": 1,
    "news": 1,
    "security": 1,
    "syslog": 1,
    "user": 1,
    "uucp": 1,
    "local0": 1,
    "local1": 1,
    "local2": 1,
    "local3": 1,
    "local4": 1,
    "local5": 1,
    "local6": 1,
    "local7": 1,
    }

def syslog_facility(value):
    value = value.lower()
    if not _syslog_facilities.has_key(value):
        raise ValueError(
            "Syslog facility must be one of "
            + ", ".join(_syslog_facilities.keys()))
    return value

def syslog_handler(section):
    def callback(inst,
                 format=section.format,
                 dateformat=section.dateformat,
                 level=section.level):
        import logging
        inst.setFormatter(logging.Formatter(format, dateformat))
        inst.setLevel(level)

    return Factory('zLOG.LogHandlers.SysLogHandler', callback,
                   section.address.address,
                   section.facility)

## def nteventlog_handler(section):
##     appname = section.appname
##     format = section.format
##     dateformat = section.dateformat
##     level = section.level
    
##     formatter = Factory('logging.Formatter', None, format, dateformat)

##     def callback(inst, formatter=formatter, level=level):
##         inst.setFormatter(formatter())
##         inst.setLevel(level)

##     return Factory('zLOG.LogHandlers.NTEventLogHandler', callback, appname)

## def http_handler_url(value):
##     import urlparse
##     scheme, netloc, path, query, fragment = urlparse.urlsplit(value)
##     if scheme != 'http':
##         raise ValueError, 'url must be an http url'
##     if not netloc:
##         raise ValueError, 'url must specify a location'
##     if not path:
##         raise ValueError, 'url must specify a path'
##     q = []
##     if query:
##         q.append('?')
##         q.append(query)
##     if fragment:
##         q.append('#')
##         q.append(fragment)
##     return (netloc, path + ''.join(q))

## def get_or_post(value):
##     value = value.upper()
##     if value not in ('GET', 'POST'):
##         raise ValueError, ('method must be "GET" or "POST", instead received '
##                            '%s' % repr(value))
##     return value

## def http_handler(section):
##     host, url = section.url
##     method     = section.method
##     format     = section.format
##     dateformat = section.dateformat
##     level      = section.level
    
##     formatter = Factory('logging.Formatter', None, format, dateformat)

##     def callback(inst, formatter=formatter, level=level):
##         inst.setFormatter(formatter())
##         inst.setLevel(level)

##     return Factory('zLOG.LogHandlers.HTTPHandler', callback, host, url, method)

## def smtp_handler(section):
##     fromaddr   = section.fromaddr
##     toaddrs    = section.toaddrs
##     subject    = section.subject
##     host, port = section.host
##     format     = section.format
##     dateformat = section.dateformat
##     level      = section.level
    
##     if not port:
##         mailhost = host
##     else:
##         mailhost = host, port
##     formatter = Factory('logging.Formatter', None, format, dateformat)

##     def callback(inst, formatter=formatter, level=level):
##         inst.setFormatter(formatter())
##         inst.setLevel(level)

##     return Factory('zLOG.LogHandlers.SMTPHandler', callback,
##                    mailhost, fromaddr, toaddrs, subject)

## def null_handler(section):
##     return Factory('zLOG.LogHandlers.NullHandler', None)

## def custom_handler(section):
##     formatter_klass, formatter_pos, formatter_kw = section.formatter
##     handler_klass, handler_pos, handler_kw = section.constructor
##     level = section.level

##     formatter = Factory(formatter_klass, None, formatter_pos, formatter_kw)

##     def callback(inst, formatter=formatter, level=level):
##         inst.setFormatter(formatter())
##         inst.setLevel(level)

##     return Factory(handler_klass, callback, *handler_pos, **handler_kw)

def logger(section):
    return LoggerWrapper(section.level, section.handlers)

_marker = []

class LoggerWrapper:
    """
    A wrapper used to create loggers while delaying actual logger
    instance construction.  We need to do this because we may
    want to reference a logger before actually instantiating it (for example,
    to allow the app time to set an effective user).
    An instance of this wrapper is a callable which, when called, returns a
    logger object.
    """
    def __init__(self, level, handler_factories):
        self.level = level
        self.handler_factories = handler_factories
        self.resolved = _marker

    def __call__(self):
        if self.resolved is _marker:
            # set the logger up
            import logging
            logger = logging.getLogger("event")
            logger.handlers = []
            logger.propagate = 0
            logger.setLevel(self.level)
            if self.handler_factories:
                for handler_factory in self.handler_factories:
                    handler =  handler_factory()
                    logger.addHandler(handler)
            else:
                from zLOG.LogHandlers import NullHandler
                logger.addHandler(NullHandler())
            self.resolved = logger
        return self.resolved
