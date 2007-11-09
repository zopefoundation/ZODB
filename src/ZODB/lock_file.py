##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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

import os
import errno
import logging
logger = logging.getLogger("ZODB.lock_file")

class LockError(Exception):
    """Couldn't lock a file
    """

try:
    import fcntl
except ImportError:
    try:
        import msvcrt
    except ImportError:
        def _lock_file(file):
            raise TypeError('No file-locking support on this platform')
        def _unlock_file(file):
            raise TypeError('No file-locking support on this platform')

    else:
        # Windows
        def _lock_file(file):
            # Lock just the first byte
            try:
                msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
            except IOError:
                raise LockError("Couldn't lock %r" % file.name)

        def _unlock_file(file):
            try:
                file.seek(0)
                msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
            except IOError:
                raise LockError("Couldn't unlock %r" % file.name)
                
else:
    # Unix
    _flags = fcntl.LOCK_EX | fcntl.LOCK_NB

    def _lock_file(file):
        try:
            fcntl.flock(file.fileno(), _flags)
        except IOError:
            raise LockError("Couldn't lock %r" % file.name)
            

    def _unlock_file(file):
        # File is automatically unlocked on close
        pass



# This is a better interface to use than the lockfile.lock_file() interface.
# Creating the instance acquires the lock.  The file remains open.  Calling
# close both closes and unlocks the lock file.
class LockFile:

    _fp = None

    def __init__(self, path):
        self._path = path
        fp = open(path, 'w+')

        try:
            _lock_file(fp)
        except:
            fp.seek(1)
            pid = fp.read().strip()[:20]
            fp.close()
            logger.exception("Error locking file", path, pid)
            raise

        self._fp = fp
        fp.write(" %s\n" % os.getpid())
        fp.truncate()
        fp.flush()

    def close(self):
        if self._fp is not None:
            _unlock_file(self._fp)
            self._fp.close()
            self._fp = None
