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

    # XXX should pick up sys.{stderr,stdout} when the factory is invoked
    if path == "STDERR":
        return Factory('zLOG.LogHandlers.StreamHandler', callback, sys.stderr)
    elif path == "STDOUT":
        return Factory('zLOG.LogHandlers.StreamHandler', callback, sys.stdout)
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
        L = _syslog_facilities.keys()
        L.sort()
        raise ValueError("Syslog facility must be one of " + ", ".join(L))
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

def nteventlog_handler(section):
    def callback(inst,
                 format=section.format,
                 dateformat=section.dateformat,
                 level=section.level):
        import logging
        inst.setFormatter(logging.Formatter(format, dateformat))
        inst.setLevel(level)

    return Factory('zLOG.LogHandlers.NTEventLogHandler', callback,
                   section.appname)

def http_handler_url(value):
    import urlparse
    scheme, netloc, path, param, query, fragment = urlparse.urlparse(value)
    if scheme != 'http':
        raise ValueError, 'url must be an http url'
    if not netloc:
        raise ValueError, 'url must specify a location'
    if not path:
        raise ValueError, 'url must specify a path'
    q = []
    if param:
        q.append(';')
        q.append(param)
    if query:
        q.append('?')
        q.append(query)
    if fragment:
        q.append('#')
        q.append(fragment)
    return (netloc, path + ''.join(q))

def get_or_post(value):
    value = value.upper()
    if value not in ('GET', 'POST'):
        raise ValueError('method must be "GET" or "POST", instead received: '
                         + repr(value))
    return value

def http_handler(section):
    def callback(inst,
                 format=section.format,
                 dateformat=section.dateformat,
                 level=section.level):
        import logging
        inst.setFormatter(logging.Formatter(format, dateformat))
        inst.setLevel(level)

    host, selector = section.url
    return Factory('zLOG.LogHandlers.HTTPHandler',
                   callback, host, selector, section.method)

def smtp_handler(section):
    def callback(inst,
                 format=section.format,
                 dateformat=section.dateformat,
                 level=section.level):
        import logging
        inst.setFormatter(logging.Formatter(format, dateformat))
        inst.setLevel(level)

    host, port = section.smtp_server
    if not port:
        mailhost = host
    else:
        mailhost = host, port

    return Factory('zLOG.LogHandlers.SMTPHandler',
                   callback,
                   mailhost,
                   section.fromaddr,
                   section.toaddrs,
                   section.subject)


_marker = []

class EventLogFactory:
    """
    A wrapper used to create loggers while delaying actual logger
    instance construction.  We need to do this because we may
    want to reference a logger before actually instantiating it (for example,
    to allow the app time to set an effective user).
    An instance of this wrapper is a callable which, when called, returns a
    logger object.
    """
    def __init__(self, section):
        self.level = section.level
        self.handler_factories = section.handlers
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
                    handler = handler_factory()
                    logger.addHandler(handler)
            else:
                from zLOG.LogHandlers import NullHandler
                logger.addHandler(NullHandler())
            self.resolved = logger
        return self.resolved
