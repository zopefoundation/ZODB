##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
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
import os, sys, unittest
from zope.testing import doctest

import ZODB.lock_file, time, threading
    

def inc():
    while 1:
        try:
            lock = ZODB.lock_file.LockFile('f.lock')
        except ZODB.lock_file.LockError:
            continue
        else:
            break
    f = open('f', 'r+b')
    v = int(f.readline().strip())
    time.sleep(0.01)
    v += 1
    f.seek(0)
    f.write('%d\n' % v)
    f.close()
    lock.close()

def many_threads_read_and_write():
    r"""
    >>> open('f', 'w+b').write('0\n')
    >>> open('f.lock', 'w+b').write('0\n')

    >>> n = 50
    >>> threads = [threading.Thread(target=inc) for i in range(n)]
    >>> _ = [thread.start() for thread in threads]
    >>> _ = [thread.join() for thread in threads]
    >>> saved = int(open('f', 'rb').readline().strip())
    >>> saved == n
    True

    >>> os.remove('f')
    >>> os.remove('f.lock')
    """

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(doctest.DocFileSuite(os.path.join('..', 'lock_file.txt')))
    suite.addTest(doctest.DocTestSuite())
    return suite
