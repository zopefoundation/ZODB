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
"""ZODB transfer activity monitoring

$Id: ActivityMonitor.py,v 1.2 2002/06/10 20:20:44 shane Exp $"""
__version__='$Revision: 1.2 $'[11:-2]

import time


class ActivityMonitor:
    """ZODB load/store activity monitor

    This simple implementation just keeps a small log in memory
    and iterates over the log when getActivityAnalysis() is called.

    It assumes that log entries are added in chronological sequence,
    which is only guaranteed because DB.py holds a lock when calling
    the closedConnection() method.
    """

    def __init__(self, history_length=3600):
        self.history_length = history_length  # Number of seconds
        self.log = []                     # [(time, loads, stores)]

    def closedConnection(self, conn):
        log = self.log
        now = time.time()
        loads, stores = conn.getTransferCounts(1)
        log.append((now, loads, stores))
        self.trim(now)

    def trim(self, now):
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
        log = self.log
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
                })

        div = res[0]
        div_start = div['start']
        div_end = div['end']
        div_index = 0
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
                total_loads = 0
                total_stores = 0
                div_index = div_index + 1
                if div_index < divisions:
                    div = res[div_index]
                    div_start = div['start']
                    div_end = div['end']
            total_loads = total_loads + loads
            total_stores = total_stores + stores

        div['stores'] = div['stores'] + total_stores
        div['loads'] = div['loads'] + total_loads

        return res

