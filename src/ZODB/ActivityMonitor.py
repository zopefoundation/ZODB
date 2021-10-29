##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
"""ZODB transfer activity monitoring
"""

import time

from . import utils


class ActivityMonitor(object):
    """ZODB load/store activity monitor

    This simple implementation just keeps a small log in memory
    and iterates over the log when getActivityAnalysis() is called.

    It assumes that log entries are added in chronological sequence.
    """

    def __init__(self, history_length=3600):
        self.history_length = history_length  # Number of seconds
        self.log = []                     # [(time, loads, stores)]
        self.trim_lock = utils.Lock()

    def closedConnection(self, conn):
        log = self.log
        now = time.time()
        loads, stores = conn.getTransferCounts(1)
        log.append((now, loads, stores))
        self.trim(now)

    def trim(self, now):
        with self.trim_lock:
            log = self.log
            cutoff = now - self.history_length
            n = 0
            loglen = len(log)
            while n < loglen and log[n][0] < cutoff:
                n = n + 1
            if n:
                del log[:n]

    def setHistoryLength(self, history_length):
        self.history_length = history_length
        self.trim(time.time())

    def getHistoryLength(self):
        return self.history_length

    def getActivityAnalysis(self, start=0, end=0, divisions=10):
        res = []
        now = time.time()
        if start == 0:
            start = now - self.history_length
        if end == 0:
            end = now
        for n in range(divisions):
            res.append({
                'start': start + (end - start) * n / divisions,
                'end': start + (end - start) * (n + 1) / divisions,
                'loads': 0,
                'stores': 0,
                'connections': 0,
            })

        div = res[0]
        div_end = div['end']
        div_index = 0
        connections = 0
        total_loads = 0
        total_stores = 0
        for t, loads, stores in self.log:
            if t < start:
                # We could use a binary search to find the start.
                continue
            elif t > end:
                # We could use a binary search to find the end also.
                break
            while t > div_end:
                div['loads'] = total_loads
                div['stores'] = total_stores
                div['connections'] = connections
                total_loads = 0
                total_stores = 0
                connections = 0
                div_index = div_index + 1
                if div_index < divisions:
                    div = res[div_index]
                    div_end = div['end']
            connections = connections + 1
            total_loads = total_loads + loads
            total_stores = total_stores + stores

        div['stores'] = div['stores'] + total_stores
        div['loads'] = div['loads'] + total_loads
        div['connections'] = div['connections'] + connections

        return res
