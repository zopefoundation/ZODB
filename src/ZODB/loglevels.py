##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Supplies custom logging levels BLATHER and TRACE.

$Revision: 1.1 $
"""

import logging


__all__ = ["BLATHER", "TRACE"]

# In the days of zLOG, there were 7 standard log levels, and ZODB/ZEO used
# all of them.  Here's how they map to the logging package's 5 standard
# levels:
#
#    zLOG                         logging
#    -------------                ---------------
#    PANIC (300)                  FATAL, CRITICAL (50)
#    ERROR (200)                  ERROR (40)
#    WARNING, PROBLEM (100)       WARN (30)
#    INFO (0)                     INFO (20)
#    BLATHER (-100)               none -- defined here as BLATHER (15)
#    DEBUG (-200)                 DEBUG (10)
#    TRACE (-300)                 none -- defined here as TRACE (5)
#
# TRACE is used by ZEO for extremely verbose trace output, enabled only
# when chasing bottom-level communications bugs.  It really should be at
# a lower level than DEBUG.
#
# BLATHER is a harder call, and various instances could probably be folded
# into INFO or DEBUG without real harm.

BLATHER = 15
TRACE = 5
logging.addLevelName(BLATHER, "BLATHER")
logging.addLevelName(TRACE, "TRACE")
