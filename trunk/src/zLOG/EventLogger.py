##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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
"""
A logging module which handles event messages.

This uses Vinay Sajip's PEP 282 logging module.
"""

__version__='$Revision$'[11:-2]

import logging
import time

# Custom logging levels
CUSTOM_BLATHER = 15 # Mapping for zLOG.BLATHER
CUSTOM_TRACE = 5 # Mapping for zLOG.TRACE
logging.addLevelName("BLATHER", CUSTOM_BLATHER)
logging.addLevelName("TRACE", CUSTOM_TRACE)


def log_write(subsystem, severity, summary, detail, error):
    level = (zlog_to_pep282_severity_cache_get(severity) or
             zlog_to_pep282_severity(severity))

    msg = summary
    if detail:
        msg = "%s\n%s" % (msg, detail)

    logger = logging.getLogger(subsystem)
    logger.log(level, msg, exc_info=(error is not None))


def severity_string(severity, mapping={
    -300: 'TRACE',
    -200: 'DEBUG',
    -100: 'BLATHER',
       0: 'INFO',
     100: 'PROBLEM',
     200: 'ERROR',
     300: 'PANIC',
    }):
    """Convert a severity code to a string."""
    s = mapping.get(int(severity), '')
    return "%s(%s)" % (s, severity)


def zlog_to_pep282_severity(zlog_severity):
    """
    We map zLOG severities to PEP282 severities here.
    This is how they are mapped:

    zLOG severity                      PEP282 severity
    -------------                      ---------------
    PANIC (300)                        FATAL, CRITICAL (50)
    ERROR (200)                        ERROR (40)
    WARNING, PROBLEM (100)             WARN (30)
    INFO (0)                           INFO (20)
    BLATHER (-100)                     BLATHER (15) [*]
    DEBUG (-200)                       DEBUG (10)
    TRACE (-300)                       TRACE (5) [*]

    [*] BLATHER and TRACE are custom logging levels.
    """
    sev = zlog_severity
    if sev >= 300:
        return logging.FATAL
    if sev >= 200:
        return logging.ERROR
    if sev >= 100:
        return logging.WARN
    if sev >= 0:
        return logging.INFO
    if sev >= -100:
        return CUSTOM_BLATHER
    if sev >= -200:
        return logging.DEBUG
    return CUSTOM_TRACE

zlog_to_pep282_severity_cache = {}
for _sev in range(-300, 301, 100):
    zlog_to_pep282_severity_cache[_sev] = zlog_to_pep282_severity(_sev)
zlog_to_pep282_severity_cache_get = zlog_to_pep282_severity_cache.get


def log_time():
    """Return a simple time string without spaces suitable for logging."""
    return ("%4.4d-%2.2d-%2.2dT%2.2d:%2.2d:%2.2d"
            % time.localtime()[:6])
