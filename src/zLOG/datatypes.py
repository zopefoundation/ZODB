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

"""ZConfig datatypes for logging support."""

import sys

from zLOG.factory import Factory

# log-related datatypes

_logging_levels = {
    "critical": 50,
    "fatal": 50,
    "error": 40,
    "warn": 30,
    "warning": 30,
    "info": 20,
    "blather": 15,
    "debug": 10,
    "trace": 5,
    "all": 1,
    "notset": 0,
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


class HandlerFactory(Factory):
    def __init__(self, section):
        Factory.__init__(self)
        self.section = section

    def create_loghandler(self):
        raise NotImplementedError(
            "subclasses must override create_loghandler()")

    def create(self):
        import logging
        logger = self.create_loghandler()
        logger.setFormatter(logging.Formatter(self.section.format,
                                              self.section.dateformat))
        logger.setLevel(self.section.level)
        return logger

    def getLevel(self):
        return self.section.level

class FileHandlerFactory(HandlerFactory):
    def create_loghandler(self):
        from zLOG.LogHandlers import StreamHandler, FileHandler
        path = self.section.path
        if path == "STDERR":
            return StreamHandler(sys.stderr)
        if path == "STDOUT":
            return StreamHandler(sys.stdout)
        return FileHandler(path)

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

class SyslogHandlerFactory(HandlerFactory):
    def create_loghandler(self):
        from zLOG.LogHandlers import SysLogHandler
        return SysLogHandler(self.section.address.address,
                             self.section.facility)

class Win32EventLogFactory(HandlerFactory):
    def create_loghandler(self):
        from zLOG.LogHandlers import Win32EventLogHandler
        return Win32EventLogHandler(self.section.appname)

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

class HTTPHandlerFactory(HandlerFactory):
    def create_loghandler(self):
        from zLOG.LogHandlers import HTTPHandler
        host, selector = self.section.url
        return HTTPHandler(host, selector, self.section.method)

class SMTPHandlerFactory(HandlerFactory):
    def create_loghandler(self):
        from zLOG.LogHandlers import SMTPHandler
        host, port = self.section.smtp_server
        if not port:
            mailhost = host
        else:
            mailhost = host, port
        return SMTPHandler(mailhost, self.section.fromaddr,
                           self.section.toaddrs, self.section.subject)


class EventLogFactory(Factory):
    """
    A wrapper used to create loggers while delaying actual logger
    instance construction.  We need to do this because we may
    want to reference a logger before actually instantiating it (for example,
    to allow the app time to set an effective user).
    An instance of this wrapper is a callable which, when called, returns a
    logger object.
    """
    def __init__(self, section):
        Factory.__init__(self)
        self.level = section.level
        self.handler_factories = section.handlers

    def create(self):
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
        return logger

    def getLowestHandlerLevel(self):
        """ Return the lowest log level provided by any of our handlers
        (used by Zope startup logger code to decide what to send
        to stderr during startup) """
        lowest = self.level
        for factory in self.handler_factories:
            handler_level = factory.getLevel()
            if handler_level < lowest:
                lowest = factory.getLevel()
        return lowest
