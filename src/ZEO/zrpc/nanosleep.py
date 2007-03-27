##############################################################################
#
# Copyright (c) 2006 Lovely Systems and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""
Because time.sleep() has a system dependend minimum sleep time the ZEO
connection can't use the full speed of the network because sleep is used to
wait for packages from ZEO.

We use nanosleep which allowes the use of a system independent sleep time.

To be able to use nanosleep we use ctypes from python 2.5. Because we can not
use python 2.5 with zope the ctypes packe needs to be installed separately.

$Id$
"""
__docformat__ = 'restructuredtext'

import logging

logger = logging.getLogger('ZEO.ClientStorage')

try:
    import ctypes
    try:
        libc = ctypes.CDLL("libc.so")
    except OSError:
        libc = None
    if libc is None:
        # MAC OS-X
        try:
            libc = ctypes.CDLL("libc.dylib", ctypes.RTLD_GLOBAL)
        except OSError:
            raise ImportError

    class timespec(ctypes.Structure):
        _fields_ = [('secs', ctypes.c_long),
                    ('nsecs', ctypes.c_long),
                   ]

    libc.nanosleep.argtypes = \
            [ctypes.POINTER(timespec), ctypes.POINTER(timespec)]

    logger.info('Connection using nanosleep!')
    def nanosleep(sec, nsec):
        sleeptime = timespec()
        sleeptime.secs = sec
        sleeptime.nsecs = nsec
        remaining = timespec()
        libc.nanosleep(sleeptime, remaining)
        return (remaining.secs, remaining.nsecs)

except ImportError:
    # if ctypes is not available or no reasonable library is found we provide
    # a dummy which uses time.sleep
    logger.info('Connection using time.sleep!')
    import time
    def nanosleep(sec, nsec):
        time.sleep(sec + (nsec * 0.000000001))

