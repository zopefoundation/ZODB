#! /usr/bin/env python
"""Report on the number of currently waiting clients in the ZEO queue."""

import fileinput
import re
import sys
import time

rx_time = re.compile('(\d\d\d\d-\d\d-\d\d)T(\d\d:\d\d:\d\d)')

def parse_time(line):
    """Return the time portion of a zLOG line in seconds or None."""
    mo = rx_time.match(line)
    if mo is None:
        return None
    date, time_ = mo.group(1, 2)
    date_l = [int(elt) for elt in date.split('-')]
    time_l = [int(elt) for elt in time_.split(':')]
    return int(time.mktime(date_l + time_l + [0, 0, 0]))

class Txn:
    """Track status of single transaction."""

    def __init__(self, tid):
        self.tid = tid
        self.hint = None
        self.begin = None
        self.vote = None
        self.abort = None
        self.finish = None

    def isactive(self):
        if self.begin and not (self.abort or self.finish):
            return 1
        else:
            return 0

class Status:
    """Track status of ZEO server by replaying log records.

    We want to keep track of several events:

    - The last committed transaction.
    - The last committed or aborted transaction.
    - The last transaction that got the lock but didn't finish.
    - The number of currently active transactions.
    - The number of reported queued transactions.
    - Client restarts.
    - Number of current connections. XXX (This might not be useful.)

    We can observe these events by reading the following sorts of log
    entries:

    2002-12-16T06:16:05 BLATHER(-100) zrpc:12649 calling
    tpc_begin('\x03I\x90((\xdbp\xd5', '', 'QueueCatal...

    2002-12-16T06:16:06 BLATHER(-100) zrpc:12649 calling
    vote('\x03I\x90((\xdbp\xd5')

    2002-12-16T06:16:06 BLATHER(-100) zrpc:12649 calling
    tpc_finish('\x03I\x90((\xdbp\xd5')

    2002-12-16T10:46:10 INFO(0) ZSS:12649:1 Transaction blocked waiting
    for storage. Clients waiting: 1.

    2002-12-16T06:15:57 BLATHER(-100) zrpc:12649 connect from
    ('10.0.26.54', 48983): <ManagedServerConnection ('10.0.26.54', 48983)>

    2002-12-16T10:30:09 INFO(0) ZSS:12649:1 disconnected
    """

    def __init__(self):
        self.reset()

    def reset(self):
        self.commit = None
        self.commit_or_abort = None
        self.n_active = 0
        self.n_blocked = 0
        self.n_conns = 0
        self.t_restart = None
        self.txns = {}

    def report(self):
        print "Blocked transactions:", self.n_blocked
        if not VERBOSE:
            return
        if self.t_restart:
            print "Server started:", time.ctime(self.t_restart)

        if self.commit is not None:
            t = self.commit_or_abort.finish
            if t is None:
                t = self.commit_or_abort.abort
            print "Last finished transaction:", time.ctime(t)
            
        # the blocked transaction should be the first one that calls vote
        L = [(txn.begin, txn) for txn in self.txns.values()]
        L.sort()

        first_txn = L[0][1]
        if first_txn.isactive():
            began = first_txn.begin
            print "Blocked transaction began at:", time.ctime(began)
            print "Hint:", first_txn.hint
            print "Idle time: %d sec" % int(time.time() - began)

    def process(self, line):
        if line.find("calling") != -1:
            self.process_call(line)
        elif line.find("connect") != -1:
            self.process_connect(line)
        elif line.find("locked") != -1:
            # test for "locked" because word may start with "B" or "b"
            self.process_block(line)
        elif line.find("Starting") != -1:
            self.process_start(line)

    rx_call = re.compile("calling (\w+)\(\'(\S+)\'(.*)")

    def process_call(self, line):
        mo = self.rx_call.search(line)
        if mo is None:
            return
        called_method = mo.group(1)
        
        # XXX exit earlier if we've got zeoLoad, because it's the most
        # frequently called method and we don't use it.
        if called_method == "zeoLoad":
            return

        t = parse_time(line)
        meth = getattr(self, "call_%s" % called_method, None)
        if meth is None:
            return
        tid = mo.group(2)
        rest = mo.group(3)
        meth(t, tid, rest)

    def process_connect(self, line):
        pass

    rx_waiting = re.compile("Clients waiting: (\d+)")

    def process_block(self, line):
        mo = self.rx_waiting.search(line)
        if mo is None:
            # assume that this was a restart message for the last blocked
            # transaction.
            self.n_blocked = 0
        else:
            self.n_blocked = int(mo.group(1))

    def process_start(self, line):
        if line.find("Starting ZEO server") != -1:
            self.reset()
            self.t_restart = parse_time(line)

    def call_tpc_begin(self, t, tid, rest):
        txn = Txn(tid)
        txn.begin = t
        if rest[0] == ',':
            i = 1
            while rest[i].isspace():
                i += 1
            rest = rest[i:]
        txn.hint = rest
        self.txns[tid] = txn
        self.n_active += 1

    def call_vote(self, t, tid, rest):
        txn = self.txns.get(tid)
        if txn is None:
            print "Oops!"
            txn = self.txns[tid] = Txn(tid)
        txn.vote = t

    def call_tpc_abort(self, t, tid, rest):
        txn = self.txns.get(tid)
        if txn is None:
            print "Oops!"
            txn = self.txns[tid] = Txn(tid)
        txn.abort = t
        self.n_active -= 1
        
        if self.commit_or_abort:
            # delete the old transaction
            try:
                del self.txns[self.commit_or_abort.tid]
            except KeyError:
                pass
        self.commit_or_abort = txn

    def call_tpc_finish(self, t, tid, rest):
        txn = self.txns.get(tid)
        if txn is None:
            print "Oops!"
            txn = self.txns[tid] = Txn(tid)
        txn.finish = t
        self.n_active -= 1

        if self.commit:
            # delete the old transaction
            try:
                del self.txns[self.commit.tid]
            except KeyError:
                pass
        if self.commit_or_abort:
            # delete the old transaction
            try:
                del self.txns[self.commit_or_abort.tid]
            except KeyError:
                pass
        self.commit = self.commit_or_abort = txn

def main():
    global VERBOSE
    # decide whether -v was passed on the command line
    try:
        i = sys.argv.index("-v")
    except ValueError:
        VERBOSE = 0
    else:
        VERBOSE = 1
        # fileinput assumes all of sys.argv[1:] is files it should read
        del sys.argv[i]

    s = Status()
    for line in fileinput.input():
        s.process(line)
    s.report()

if __name__ == "__main__":
    main()

