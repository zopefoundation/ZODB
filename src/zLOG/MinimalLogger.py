##############################################################################
#
# Copyright (c) 2001 Zope Corporation and Contributors. All Rights Reserved.
# 
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
# 
##############################################################################
__version__='$Revision: 1.5 $'[11:-2]

import os,  sys, time

from FormatException import format_exception

def severity_string(severity, mapping={
    -300: 'TRACE',
    -200: 'DEBUG',
    -100: 'BLATHER',
       0: 'INFO',       
     100: 'PROBLEM', 
     200: 'ERROR',    
     300: 'PANIC', 
    }):
    """Convert a severity code to a string
    """
    s=int(severity)
    if mapping.has_key(s): s=mapping[s]
    else: s=''
    return "%s(%s)" % (s, severity)

def log_time():
    """Return a simple time string without spaces suitable for logging
    """
    return ("%4.4d-%2.2d-%2.2dT%2.2d:%2.2d:%2.2d"
            % time.gmtime(time.time())[:6])

def _set_log_dest(dest):
    global _log_dest
    _log_dest = dest

_log_dest = None
_stupid_severity = None
_no_stupid_log = []

class stupid_log_write:

    def __init__(self):
        self.initialize()

    def initialize(self):
        global _log_dest, _log_level

        path = os.environ.get('STUPID_LOG_FILE', None)
        if path is None:
            _log_dest = None
        else:
            if path:
                _log_dest = open(path, 'a')
            else:
                _log_dest = sys.stderr

        severity = os.environ.get('STUPID_LOG_SEVERITY', None)
        if severity:
            _log_level = int(severity)
        else:
            _log_level = 0 # INFO
    
    def log(self, subsystem, severity, summary, detail, error):
        if _log_dest is None or severity < _log_level:
            return

        if detail:
            buf = ("------\n"
                   "%s %s %s %s\n%s" % (log_time(), severity_string(severity),
                                        subsystem, summary, detail))
        else:
            buf = ("------\n"
                   "%s %s %s %s" % (log_time(), severity_string(severity),
                                    subsystem, summary))
        print >> _log_dest, buf

        if error:
            try:
                lines = format_exception(error[0], error[1], error[2],
                                         trailer="\n", limit=100)
                print >> _log_dest, lines
            except:
                print >> _log_dest, "%s: %s" % error[:2]
        _log_dest.flush()


_log = stupid_log_write()
log_write = _log.log
