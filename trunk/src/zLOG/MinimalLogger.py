##############################################################################
# 
# Zope Public License (ZPL) Version 1.0
# -------------------------------------
# 
# Copyright (c) Digital Creations.  All rights reserved.
# 
# This license has been certified as Open Source(tm).
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
# 1. Redistributions in source code must retain the above copyright
#    notice, this list of conditions, and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions, and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
# 
# 3. Digital Creations requests that attribution be given to Zope
#    in any manner possible. Zope includes a "Powered by Zope"
#    button that is installed by default. While it is not a license
#    violation to remove this button, it is requested that the
#    attribution remain. A significant investment has been put
#    into Zope, and this effort will continue if the Zope community
#    continues to grow. This is one way to assure that growth.
# 
# 4. All advertising materials and documentation mentioning
#    features derived from or use of this software must display
#    the following acknowledgement:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    In the event that the product being advertised includes an
#    intact Zope distribution (with copyright and license included)
#    then this clause is waived.
# 
# 5. Names associated with Zope or Digital Creations must not be used to
#    endorse or promote products derived from this software without
#    prior written permission from Digital Creations.
# 
# 6. Modified redistributions of any form whatsoever must retain
#    the following acknowledgment:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    Intact (re-)distributions of any official Zope release do not
#    require an external acknowledgement.
# 
# 7. Modifications are encouraged but must be packaged separately as
#    patches to official Zope releases.  Distributions that do not
#    clearly separate the patches from the original work must be clearly
#    labeled as unofficial distributions.  Modifications which do not
#    carry the name Zope may be packaged in any form, as long as they
#    conform to all of the clauses above.
# 
# 
# Disclaimer
# 
#   THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND ANY
#   EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
#   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL CREATIONS OR ITS
#   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
#   USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
#   OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
#   SUCH DAMAGE.
# 
# 
# This software consists of contributions made by Digital Creations and
# many individuals on behalf of Digital Creations.  Specific
# attributions are listed in the accompanying credits file.
# 
##############################################################################
__version__='$Revision: 1.1 $'[11:-2]

import string, sys, time

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

def _set_stupid_dest(dest):
    global _stupid_dest
    _stupid_dest = dest

_stupid_dest=None
_stupid_severity=None
_no_stupid_log=[]
class stupid_log_write:

    def reinitialize(self):
        global _stupid_dest
        _stupid_dest = None        
    
    def __call__(self, subsystem, severity, summary, detail, error):

        global _stupid_dest
        if _stupid_dest is None:
            import os
            if os.environ.has_key('STUPID_LOG_FILE'):
                f=os.environ['STUPID_LOG_FILE']
                if f: _stupid_dest=open(f,'a')
                else:
                    _stupid_dest=sys.stderr
            else:
                _stupid_dest=_no_stupid_log

        if _stupid_dest is _no_stupid_log: return

        global _stupid_severity
        if _stupid_severity is None:
            try: _stupid_severity=string.atoi(os.environ['STUPID_LOG_SEVERITY'])
            except: _stupid_severity=0

        if severity < _stupid_severity: return

        _stupid_dest.write(
            "------\n"
            "%s %s %s %s\n%s"
            %
            (log_time(),
             severity_string(severity),
             subsystem,
             summary,
             detail,
             )
            )
        _stupid_dest.flush()

        if error:
            try:
                _stupid_dest.write(format_exception(
                    error[0], error[1], error[2],
                    trailer='\n', limit=100))
            except:
                _stupid_dest.write("%s: %s\n" % error[:2])
        _stupid_dest.flush()


log_write=stupid_log_write()
