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
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
__version__='$Revision: 1.20 $'[11:-2]

import os, sys, time
import zLOG

try:
    import textwrap
except ImportError:
    textwrap = None

from traceback import format_exception

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

def log_time():
    """Return a simple time string without spaces suitable for logging."""
    return ("%4.4d-%2.2d-%2.2dT%2.2d:%2.2d:%2.2d"
            % time.localtime()[:6])

def _set_log_dest(dest):
    global _log_dest
    _log_dest = dest

_log_dest = None
_stupid_severity = None

class stupid_log_write:

    def __init__(self):
        self.initialize()

    def initialize(self):
        path, severity = self.get_environment_info()
        self.initialize_log(path, severity)

    def initialize_with_config(self, config):
        """Initialize logging with information from ZConfig."""
        path, severity = self.get_environment_info()

        if config is not None:
            loginfo = config.getSection("log")
            if loginfo is not None:
                if path is None:
                    path = loginfo.get("path")
                if severity is None:
                    severity = loginfo.get("level")

        self.initialize_log(path, severity)

    def initialize_log(self, path, severity):
        global _log_level

        if path is None:
            _set_log_dest(None)
        elif path:
            _set_log_dest(open(path, 'a'))
        else:
            _set_log_dest(sys.stderr)

        if severity:
            _log_level = zLOG.severity(severity)
        else:
            _log_level = 0 # INFO

    def get_environment_info(self):
        eget = os.environ.get

        # EVENT_LOG_FILE is the preferred envvar, but we accept
        # STUPID_LOG_FILE also
        path = eget('EVENT_LOG_FILE')
        if path is None:
            path = eget('STUPID_LOG_FILE')

        # EVENT_LOG_SEVERITY is the preferred envvar, but we accept
        # STUPID_LOG_SEVERITY also
        severity = eget('EVENT_LOG_SEVERITY')
        if severity is None:
            severity = eget('STUPID_LOG_SEVERITY')

        return path, severity

    def log(self, subsystem, severity, summary, detail, error):
        if _log_dest is None or severity < _log_level:
            return
        buf = ["------"]
        line = ("%s %s %s %s" %
                (log_time(), severity_string(severity), subsystem, summary))
        if not textwrap or len(line) < 80:
            buf.append(line)
        else:
            buf.extend(textwrap.wrap(line,
                                     width=79,
                                     subsequent_indent=" "*20,
                                     break_long_words=0))

        if detail:
            buf.append(str(detail))

        if error:
            try:
                lines = format_exception(error[0], error[1], error[2],
                                         limit=100)
                buf.append(''.join(lines))
            except:
                buf.append("%s: %s" % error[:2])

        if buf[-1] and buf[-1][-1] != "\n":
            buf.append("") # Cause a final \n to be appended

        _log_dest.write("\n".join(buf))
        _log_dest.flush()


_log = stupid_log_write()
log_write = _log.log
initialize = _log.initialize
initialize_with_config = _log.initialize_with_config
