##############################################################################
#
# Copyright (c) Zope Foundation and Contributors.
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
"""Experimental support for thread pools
"""

import logging
import Queue
import sys
import threading
import ZEO.zrpc.connection

logger = logging.getLogger(__name__)

queue = Queue.Queue()

stop = object()
def run():
    while 1:
        try:
            delay = queue.get()
            if delay is stop:
                break
            delay()
        except:
            logger.critical('Error in thready job %r', delay,
                            exc_info=sys.exc_info())

nthread = 0
def start_thread():
    global nthread
    nthread += 1
    t = threading.Thread(target=run, name='thready-%s' % nthread)
    t.setDaemon(True)
    t.start()

for i in range(4):
    start_thread()

def stop_thread():
    queue.put(stop)

class delayed(object):

    def __init__(self, func):
        self.func = func

    def __get__(self, inst, class_):
        if inst is None:
            return self

        return lambda *args: Delay(self.func, (inst,)+args)

    def __call__(self, *args):
        return Delay(self.func, args)

class Delay(ZEO.zrpc.connection.Delay):

    def __init__(self, func, args):
        self.func = func
        self.args = args

    def set_sender(self, msgid, send_reply, return_error):
        ZEO.zrpc.connection.Delay.set_sender(
            self, msgid, send_reply, return_error)
        queue.put(self)

    def __call__(self):
        try:
            r = self.func(*self.args)
        except MemoryError:
            raise
        except Exception:
            self.error(sys.exc_info())
        else:
            self.reply(r)
