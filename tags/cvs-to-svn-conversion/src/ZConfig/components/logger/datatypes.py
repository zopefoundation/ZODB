##############################################################################
#
# Copyright (c) 2002 Zope Corporation and Contributors. All Rights Reserved.
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
