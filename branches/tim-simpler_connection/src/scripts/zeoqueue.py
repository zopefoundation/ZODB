#!/usr/bin/env python2.3

"""Report on the number of currently waiting clients in the ZEO queue.

Usage: %(PROGRAM)s [options] logfile

Options:
    -h / --help
        Print this help text and exit.

    -v / --verbose
        Verbose output

    -f file
    --file file
        Use the specified file to store the incremental state as a pickle.  If
        not given, %(STATEFILE)s is used.

    -r / --reset
        Reset the state of the tool.  This blows away any existing state
        pickle file and then exits -- it does not parse the file.  Use this
        when you rotate log files so that the next run will parse from the
        beginning of the file.
"""

import os
import re
import sys
import time
import errno
import getopt
import cPickle as pickle

COMMASPACE = ', '
STATEFILE = 'zeoqueue.pck'
PROGRAM = sys.argv[0]

try:
    True, False
except NameError:
    True = 1
    False = 0



tcre = re.compile(r"""
    (?P<ymd>
     \d{4}-      # year
     \d{2}-      # month
     \d{2})      # day
    T            # separator
    (?P<hms>
     \d{2}:      # hour
     \d{2}:      # minute
     \d{2})      # second
     """, re.VERBOSE)

ccre = re.compile(r"""
    zrpc-conn:(?P<addr>\d+.\d+.\d+.\d+:\d+)\s+
    calling\s+
    (?P<method>
     \w+)        # the method
    \(           # args open paren
      \'         # string quote start
        (?P<tid>
         \S+)    # first argument -- usually the tid
      \'         # end of string
    (?P<rest>
     .*)         # rest of line
    """, re.VERBOSE)

wcre = re.compile(r'Clients waiting: (?P<num>\d+)')



def parse_time(line):
    """Return the time portion of a zLOG line in seconds or None."""
    mo = tcre.match(line)
    if mo is None:
        return None
    date, time_ = mo.group('ymd', 'hms')
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
        self.voters = []

    def isactive(self):
        if self.begin and not (self.abort or self.finish):
            return True
        else:
            return False



class Status:
    """Track status of ZEO server by replaying log records.

    We want to keep track of several events:

    - The last committed transaction.
    - The last committed or aborted transaction.
    - The last transaction that got the lock but didn't finish.
    - The client address doing the first vote of a transaction.
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
        self.lineno = 0
        self.pos = 0
        self.reset()

    def reset(self):
        self.commit = None
        self.commit_or_abort = None
        self.last_unfinished = None
        self.n_active = 0
        self.n_blocked = 0
        self.n_conns = 0
        self.t_restart = None
        self.txns = {}

    def iscomplete(self):
        # The status report will always be complete if we encounter an
        # explicit restart.
        if self.t_restart is not None:
            return True
        # If we haven't seen a restart, assume that seeing a finished
        # transaction is good enough.
        return self.commit is not None

    def process_file(self, fp):
        if self.pos:
            if VERBOSE:
                print 'seeking to file position', self.pos
            fp.seek(self.pos)
        while True:
            line = fp.readline()
            if not line:
                break
            self.lineno += 1
            self.process(line)
        self.pos = fp.tell()

    def process(self, line):
        if line.find("calling") != -1:
            self.process_call(line)
        elif line.find("connect") != -1:
            self.process_connect(line)
        # test for "locked" because word may start with "B" or "b"
        elif line.find("locked") != -1:
            self.process_block(line)
        elif line.find("Starting") != -1:
            self.process_start(line)

    def process_call(self, line):
        mo = ccre.search(line)
        if mo is None:
            return
        called_method = mo.group('method')
        # XXX exit earlier if we've got zeoLoad, because it's the most
        # frequently called method and we don't use it.
        if called_method == "zeoLoad":
            return
        t = parse_time(line)
        meth = getattr(self, "call_%s" % called_method, None)
        if meth is None:
            return
        client = mo.group('addr')
        tid = mo.group('tid')
        rest = mo.group('rest')
        meth(t, client, tid, rest)

    def process_connect(self, line):
        pass

    def process_block(self, line):
        mo = wcre.search(line)
        if mo is None:
            # assume that this was a restart message for the last blocked
            # transaction.
            self.n_blocked = 0
        else:
            self.n_blocked = int(mo.group('num'))

    def process_start(self, line):
        if line.find("Starting ZEO server") != -1:
            self.reset()
            self.t_restart = parse_time(line)

    def call_tpc_begin(self, t, client, tid, rest):
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
        self.last_unfinished = txn

    def call_vote(self, t, client, tid, rest):
        txn = self.txns.get(tid)
        if txn is None:
            print "Oops!"
            txn = self.txns[tid] = Txn(tid)
        txn.vote = t
        txn.voters.append(client)

    def call_tpc_abort(self, t, client, tid, rest):
        txn = self.txns.get(tid)
        if txn is None:
            print "Oops!"
            txn = self.txns[tid] = Txn(tid)
        txn.abort = t
        txn.voters = []
        self.n_active -= 1
        if self.commit_or_abort:
            # delete the old transaction
            try:
                del self.txns[self.commit_or_abort.tid]
            except KeyError:
                pass
        self.commit_or_abort = txn

    def call_tpc_finish(self, t, client, tid, rest):
        txn = self.txns.get(tid)
        if txn is None:
            print "Oops!"
            txn = self.txns[tid] = Txn(tid)
        txn.finish = t
        txn.voters = []
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

        for x, txn in L:
            if txn.isactive():
                began = txn.begin
                if txn.voters:
                    print "Blocked client (first vote):", txn.voters[0]
                print "Blocked transaction began at:", time.ctime(began)
                print "Hint:", txn.hint
                print "Idle time: %d sec" % int(time.time() - began)
                break



def usage(code, msg=''):
    print >> sys.stderr, __doc__ % globals()
    if msg:
        print >> sys.stderr, msg
    sys.exit(code)


def main():
    global VERBOSE

    VERBOSE = 0
    file = STATEFILE
    reset = False
    # -0 is a secret option used for testing purposes only
    seek = True
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'vhf:r0',
                                   ['help', 'verbose', 'file=', 'reset'])
    except getopt.error, msg:
        usage(1, msg)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage(0)
        elif opt in ('-v', '--verbose'):
            VERBOSE += 1
        elif opt in ('-f', '--file'):
            file = arg
        elif opt in ('-r', '--reset'):
            reset = True
        elif opt == '-0':
            seek = False

    if reset:
        # Blow away the existing state file and exit
        try:
            os.unlink(file)
            if VERBOSE:
                print 'removing pickle state file', file
        except OSError, e:
            if e.errno <> errno.ENOENT:
                raise
        return

    if not args:
        usage(1, 'logfile is required')
    if len(args) > 1:
        usage(1, 'too many arguments: %s' % COMMASPACE.join(args))

    path = args[0]

    # Get the previous status object from the pickle file, if it is available
    # and if the --reset flag wasn't given.
    status = None
    try:
        statefp = open(file, 'rb')
        try:
            status = pickle.load(statefp)
            if VERBOSE:
                print 'reading status from file', file
        finally:
            statefp.close()
    except IOError, e:
        if e.errno <> errno.ENOENT:
            raise
    if status is None:
        status = Status()
        if VERBOSE:
            print 'using new status'

    if not seek:
        status.pos = 0

    fp = open(path, 'rb')
    try:
        status.process_file(fp)
    finally:
        fp.close()
    # Save state
    statefp = open(file, 'wb')
    pickle.dump(status, statefp, 1)
    statefp.close()
    # Print the report and return the number of blocked clients in the exit
    # status code.
    status.report()
    sys.exit(status.n_blocked)


if __name__ == "__main__":
    main()
