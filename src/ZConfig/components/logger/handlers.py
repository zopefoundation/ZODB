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
"""ZConfig factory datatypes for log handlers."""

import sys

from ZConfig.components.logger.factory import Factory


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
    'process': 1,
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
        from ZConfig.components.logger import loghandler
        path = self.section.path
        if path == "STDERR":
            handler = loghandler.StreamHandler(sys.stderr)
        elif path == "STDOUT":
            handler = loghandler.StreamHandler(sys.stdout)
        else:
            handler = loghandler.FileHandler(path)
        return handler

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
        from ZConfig.components.logger import loghandler
        return loghandler.SysLogHandler(self.section.address.address,
                                        self.section.facility)

class Win32EventLogFactory(HandlerFactory):
    def create_loghandler(self):
        from ZConfig.components.logger import loghandler
        return loghandler.Win32EventLogHandler(self.section.appname)

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
        from ZConfig.components.logger import loghandler
        host, selector = self.section.url
        return loghandler.HTTPHandler(host, selector, self.section.method)

class SMTPHandlerFactory(HandlerFactory):
    def create_loghandler(self):
        from ZConfig.components.logger import loghandler
        host, port = self.section.smtp_server
        if not port:
            mailhost = host
        else:
            mailhost = host, port
        return loghandler.SMTPHandler(mailhost,
                                      self.section.fromaddr,
                                      self.section.toaddrs,
                                      self.section.subject)
