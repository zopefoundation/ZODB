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
__version__='$Revision: 1.5 $'[11:-2]

import sys

format_exception_only = None

def format_exception(etype, value, tb, limit=None, delimiter='\n',
                     header='', trailer=''):
    global format_exception_only
    if format_exception_only is None:
        import traceback
        format_exception_only = traceback.format_exception_only
        
    result=['Traceback (innermost last):']
    if header: result.insert(0, header)
    if limit is None:
        if hasattr(sys, 'tracebacklimit'):
            limit = sys.tracebacklimit
    n = 0
    while tb is not None and (limit is None or n < limit):
        f = tb.tb_frame
        lineno = tb.tb_lineno
        co = f.f_code
        filename = co.co_filename
        name = co.co_name
        locals = f.f_locals
        result.append('  File %s, line %d, in %s'
                      % (filename, lineno, name))
        try: result.append('    (Object: %s)' %
                           locals[co.co_varnames[0]].__name__)
        except: pass
        try: result.append('    (Info: %s)' %
                           str(locals['__traceback_info__']))
        except: pass
        tb = tb.tb_next
        n = n+1
    result.append(' '.join(format_exception_only(etype, value)))
    if trailer: result.append(trailer)
    
    return delimiter.join(result)
