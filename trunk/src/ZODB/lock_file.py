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

import os
import errno
import logging

try:
    import fcntl
except ImportError:
    try:
        from winlock import LockFile as _LockFile
        from winlock import UnlockFile as _UnlockFile
    except ImportError:
        import zLOG
        def lock_file(file):
            zLOG.LOG('ZODB', zLOG.INFO,
                     'No file-locking support on this platform')

    # Windows
    def lock_file(file):
        # Lock just the first byte
        _LockFile(file.fileno(), 0, 0, 1, 0)

    def unlock_file(file):
        _UnlockFile(file.fileno(), 0, 0, 1, 0)
else:
    # Unix
    _flags = fcntl.LOCK_EX | fcntl.LOCK_NB

    def lock_file(file):
        fcntl.flock(file.fileno(), _flags)

    def unlock_file(file):
        # File is automatically unlocked on close
        pass

log = logging.getLogger("LockFile")


# This is a better interface to use than the lockfile.lock_file() interface.
# Creating the instance acquires the lock.  The file remains open.  Calling
# close both closes and unlocks the lock file.
class LockFile:
    def __init__(self, path):
        self._path = path
        try:
            self._fp = open(path, 'r+')
        except IOError, e:
            if e.errno <> errno.ENOENT: raise
            self._fp = open(path, 'w+')
        # Acquire the lock and piss on the hydrant
        try:
            lock_file(self._fp)
        except:
            log.exception("Error locking file %s" % path)
            raise
        print >> self._fp, os.getpid()
        self._fp.flush()

    def close(self):
        if self._fp is not None:
            unlock_file(self._fp)
            self._fp.close()
            os.unlink(self._path)
            self._fp = None
