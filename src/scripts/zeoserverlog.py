#!python
##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
"""Tools for analyzing ZEO Server logs.

This script contains a number of commands, implemented by command
functions. To run a command, give the command name and it's arguments
as arguments to this script.

Commands:

  blocked_times file threshold

     Output a summary of episodes where thransactions were blocked
     when the episode lasted at least threshold seconds.

     The file may be a file name or - to read from standard input.
     The file may also be a command:

       script blocked_times 'bunzip2 <foo.log.bz2' 60

     If the file is a command, it must contain at least a single
     space.

     The columns of output are:

     - The time the episode started

     - The seconds from the start of the episode until the blocking
       transaction finished.

     - The client id (host and port) of the blocking transaction.

     - The seconds from the start of the episode until the end of the
       episode.

  time_calls file threshold

     Time how long calls took. Note that this is normally combined
     with grep to time just a particulat kind of call:

       script time_calls 'bunzip2 <foo.log.bz2 | grep tpc_finish' 10

       time_trans threshold

     The columns of output are:

     - The time of the call invocation

     - The seconds from the call to the return

     - The client that made the call.

  time_trans file threshold

    Output a summary of transactions that held the global transaction
    lock for at least threshold seconds. (This is the time from when
    voting starts until the transaction is completed by the server.)

    The columns of output are:

    - time that the vote started.

    - client id

    - number of objects written / number of objects updated

    - seconds from tpc_begin to vote start

    - seconds spent voting

    - vote status: n=normal, d=delayed, e=error

    - seconds wating between vote return and finish call

    - time spent finishing or 'abort' if the transaction aborted

  minute file

    Compute production statistics by minute

    The columns of output are:

    - date/time

    - Number of active clients

    - number of reads

    - number of stores

    - number of commits (finish)

    - number of aborts

    - number of transactions (commits + aborts)

    Summary statistics are printed at the end

  minutes file

    Show just the summary statistics for production by minute.

  hour file

    Compute production statistics by hour

  hours file

    Show just the summary statistics for production by hour.

  day file

    Compute production statistics by day

  days file

    Show just the summary statistics for production by day.

  verify file

    Compute verification statistics

    The columns of output are:

    - client id
    - verification start time
    - number of object's verified
    - wall time to verify
    - average miliseconds to verify per object.

$Id: zeoserverlog.py,v 1.4 2003/10/24 15:29:13 jeremy Exp $
"""

import datetime, sys, re, os


def time(line):
    d = line[:10]
    t = line[11:19]
    y, mo, d = map(int, d.split('-'))
    h, mi, s = map(int, t.split(':'))
    return datetime.datetime(y, mo, d, h, mi, s)


def sub(t1, t2):
    delta = t2 - t1
    return delta.days*86400.0+delta.seconds+delta.microseconds/1000000.0



waitre = re.compile(r'Clients waiting: (\d+)')
idre = re.compile(r' ZSS:\d+/(\d+.\d+.\d+.\d+:\d+) ')
def blocked_times(args):
    f, thresh = args

    t1 = t2 = cid = blocking = waiting = 0
    last_blocking = False

    thresh = int(thresh)

    for line in xopen(f):
        line = line.strip()

        if line.endswith('Blocked transaction restarted.'):
            blocking = False
            waiting = 0
        else:
            s = waitre.search(line)
            if not s:
                continue
            waiting = int(s.group(1))
            blocking = line.find(
                'Transaction blocked waiting for storage') >= 0

        if blocking and waiting == 1:
            t1 = time(line)
            t2 = t1

        if not blocking and last_blocking:
            last_wait = 0
            t2 = time(line)
            cid = idre.search(line).group(1)

        if waiting == 0:
            d = sub(t1, time(line))
            if d >= thresh:
                print t1, sub(t1, t2), cid, d
            t1 = t2 = cid = blocking = waiting = last_wait = max_wait = 0

        last_blocking = blocking

connidre = re.compile(r' zrpc-conn:(\d+.\d+.\d+.\d+:\d+) ')
def time_calls(f):
    f, thresh = f
    if f == '-':
        f = sys.stdin
    else:
        f = xopen(f)

    thresh = float(thresh)
    t1 = None
    maxd = 0

    for line in f:
        line = line.strip()

        if ' calling ' in line:
            t1 = time(line)
        elif ' returns ' in line and t1 is not None:
            d = sub(t1, time(line))
            if d >= thresh:
                print t1, d, connidre.search(line).group(1)
            maxd = max(maxd, d)
            t1 = None

    print maxd

def xopen(f):
    if f == '-':
        return sys.stdin
    if ' ' in f:
        return os.popen(f, 'r')
    return open(f)

def time_tpc(f):
    f, thresh = f
    if f == '-':
        f = sys.stdin
    else:
        f = xopen(f)

    thresh = float(thresh)
    transactions = {}

    for line in f:
        line = line.strip()

        if ' calling vote(' in line:
            cid = connidre.search(line).group(1)
            transactions[cid] = time(line),
        elif ' vote returns None' in line:
            cid = connidre.search(line).group(1)
            transactions[cid] += time(line), 'n'
        elif ' vote() raised' in line:
            cid = connidre.search(line).group(1)
            transactions[cid] += time(line), 'e'
        elif ' vote returns ' in line:
            # delayed, skip
            cid = connidre.search(line).group(1)
            transactions[cid] += time(line), 'd'
        elif ' calling tpc_abort(' in line:
            cid = connidre.search(line).group(1)
            if cid in transactions:
                t1, t2, vs = transactions[cid]
                t = time(line)
                d = sub(t1, t)
                if d >= thresh:
                    print 'a', t1, cid, sub(t1, t2), vs, sub(t2, t)
                del transactions[cid]
        elif ' calling tpc_finish(' in line:
            if cid in transactions:
                cid = connidre.search(line).group(1)
                transactions[cid] += time(line),
        elif ' tpc_finish returns ' in line:
            if cid in transactions:
                t1, t2, vs, t3 = transactions[cid]
                t = time(line)
                d = sub(t1, t)
                if d >= thresh:
                    print 'c', t1, cid, sub(t1, t2), vs, sub(t2, t3), sub(t3, t)
                del transactions[cid]


newobre = re.compile(r"storea\(.*, '\\x00\\x00\\x00\\x00\\x00")
def time_trans(f):
    f, thresh = f
    if f == '-':
        f = sys.stdin
    else:
        f = xopen(f)

    thresh = float(thresh)
    transactions = {}

    for line in f:
        line = line.strip()

        if ' calling tpc_begin(' in line:
            cid = connidre.search(line).group(1)
            transactions[cid] = time(line), [0, 0]
        if ' calling storea(' in line:
            cid = connidre.search(line).group(1)
            if cid in transactions:
                transactions[cid][1][0] += 1
                if not newobre.search(line):
                    transactions[cid][1][1] += 1

        elif ' calling vote(' in line:
            cid = connidre.search(line).group(1)
            if cid in transactions:
                transactions[cid] += time(line),
        elif ' vote returns None' in line:
            cid = connidre.search(line).group(1)
            if cid in transactions:
                transactions[cid] += time(line), 'n'
        elif ' vote() raised' in line:
            cid = connidre.search(line).group(1)
            if cid in transactions:
                transactions[cid] += time(line), 'e'
        elif ' vote returns ' in line:
            # delayed, skip
            cid = connidre.search(line).group(1)
            if cid in transactions:
                transactions[cid] += time(line), 'd'
        elif ' calling tpc_abort(' in line:
            cid = connidre.search(line).group(1)
            if cid in transactions:
                try:
                    t0, (stores, old), t1, t2, vs = transactions[cid]
                except ValueError:
                    pass
                else:
                    t = time(line)
                    d = sub(t1, t)
                    if d >= thresh:
                        print t1, cid, "%s/%s" % (stores, old), \
                              sub(t0, t1), sub(t1, t2), vs, \
                              sub(t2, t), 'abort'
                del transactions[cid]
        elif ' calling tpc_finish(' in line:
            if cid in transactions:
                cid = connidre.search(line).group(1)
                transactions[cid] += time(line),
        elif ' tpc_finish returns ' in line:
            if cid in transactions:
                t0, (stores, old), t1, t2, vs, t3 = transactions[cid]
                t = time(line)
                d = sub(t1, t)
                if d >= thresh:
                    print t1, cid, "%s/%s" % (stores, old), \
                          sub(t0, t1), sub(t1, t2), vs, \
                          sub(t2, t3), sub(t3, t)
                del transactions[cid]

def minute(f, slice=16, detail=1, summary=1):
    f, = f

    if f == '-':
        f = sys.stdin
    else:
        f = xopen(f)

    cols = ["time", "reads", "stores", "commits", "aborts", "txns"]
    fmt = "%18s %6s %6s %7s %6s %6s"
    print fmt % cols
    print fmt % ["-"*len(col) for col in cols]

    mlast = r = s = c = a = cl = None
    rs = []
    ss = []
    cs = []
    as = []
    ts = []
    cls = []

    for line in f:
        line = line.strip()
        if (line.find('returns') > 0
            or line.find('storea') > 0
            or line.find('tpc_abort') > 0
            ):
            client = connidre.search(line).group(1)
            m = line[:slice]
            if m != mlast:
                if mlast:
                    if detail:
                        print fmt % (mlast, len(cl), r, s, c, a, a+c)
                    cls.append(len(cl))
                    rs.append(r)
                    ss.append(s)
                    cs.append(c)
                    as.append(a)
                    ts.append(c+a)
                mlast = m
                r = s = c = a = 0
                cl = {}
            if line.find('zeoLoad') > 0:
                r += 1
                cl[client] = 1
            elif line.find('storea') > 0:
                s += 1
                cl[client] = 1
            elif line.find('tpc_finish') > 0:
                c += 1
                cl[client] = 1
            elif line.find('tpc_abort') > 0:
                a += 1
                cl[client] = 1

    if mlast:
        if detail:
            print fmt % (mlast, len(cl), r, s, c, a, a+c)
        cls.append(len(cl))
        rs.append(r)
        ss.append(s)
        cs.append(c)
        as.append(a)
        ts.append(c+a)

    if summary:
        print
        print 'Summary:     \t', '\t'.join(('min', '10%', '25%', 'med',
                                            '75%', '90%', 'max', 'mean'))
        print "n=%6d\t" % len(cls), '-'*62
        print 'Clients: \t', '\t'.join(map(str,stats(cls)))
        print 'Reads:   \t', '\t'.join(map(str,stats( rs)))
        print 'Stores:  \t', '\t'.join(map(str,stats( ss)))
        print 'Commits: \t', '\t'.join(map(str,stats( cs)))
        print 'Aborts:  \t', '\t'.join(map(str,stats( as)))
        print 'Trans:   \t', '\t'.join(map(str,stats( ts)))

def stats(s):
    s.sort()
    min = s[0]
    max = s[-1]
    n = len(s)
    out = [min]
    ni = n + 1
    for p in .1, .25, .5, .75, .90:
        lp = ni*p
        l = int(lp)
        if lp < 1 or lp > n:
            out.append('-')
        elif abs(lp-l) < .00001:
            out.append(s[l-1])
        else:
            out.append(int(s[l-1] + (lp - l) * (s[l] - s[l-1])))

    mean = 0.0
    for v in s:
        mean += v

    out.extend([max, int(mean/n)])

    return out

def minutes(f):
    minute(f, 16, detail=0)

def hour(f):
    minute(f, 13)

def day(f):
    minute(f, 10)

def hours(f):
    minute(f, 13, detail=0)

def days(f):
    minute(f, 10, detail=0)


new_connection_idre = re.compile(r"new connection \('(\d+.\d+.\d+.\d+)', (\d+)\):")
def verify(f):
    f, = f

    if f == '-':
        f = sys.stdin
    else:
        f = xopen(f)

    t1 = None
    nv = {}
    for line in f:
        if line.find('new connection') > 0:
            m = new_connection_idre.search(line)
            cid = "%s:%s" % (m.group(1), m.group(2))
            nv[cid] = [time(line), 0]
        elif line.find('calling zeoVerify(') > 0:
            cid = connidre.search(line).group(1)
            nv[cid][1] += 1
        elif line.find('calling endZeoVerify()') > 0:
            cid = connidre.search(line).group(1)
            t1, n = nv[cid]
            if n:
                d = sub(t1, time(line))
                print cid, t1, n, d, n and (d*1000.0/n) or '-'

def recovery(f):
    f, = f

    if f == '-':
        f = sys.stdin
    else:
        f = xopen(f)

    last = ''
    trans = []
    n = 0
    for line in f:
        n += 1
        if line.find('RecoveryServer') < 0:
            continue
        l = line.find('sending transaction ')
        if l > 0 and last.find('sending transaction ') > 0:
            trans.append(line[l+20:].strip())
        else:
            if trans:
                if len(trans) > 1:
                    print "  ... %s similar records skipped ..." % (
                        len(trans) - 1)
                    print n, last.strip()
                trans=[]
            print n, line.strip()
        last = line

    if len(trans) > 1:
        print "  ... %s similar records skipped ..." % (
            len(trans) - 1)
        print n, last.strip()



if __name__ == '__main__':
    globals()[sys.argv[1]](sys.argv[2:])
