#!python
# Based on a transaction analyzer by Matt Kromer.

import pickle
import re
import sys
import types
from ZODB.FileStorage import FileStorage

class Report:
    def __init__(self):
        self.OIDMAP = {}
        self.TYPEMAP = {}
        self.TYPESIZE = {}
        self.FREEMAP = {}
        self.USEDMAP = {}
        self.TIDS = 0
        self.OIDS = 0
        self.DBYTES = 0
        self.COIDS = 0
        self.CBYTES = 0
        self.FOIDS = 0
        self.FBYTES = 0

def shorten(s, n):
    l = len(s)
    if l <= n:
        return s
    while len(s) + 3 > n: # account for ...
        i = s.find(".")
        if i == -1:
            # In the worst case, just return the rightmost n bytes
            return s[-n:]
        else:
            s = s[i + 1:]
            l = len(s)
    return "..." + s

def report(rep):
    print "Processed %d records in %d transactions" % (rep.OIDS, rep.TIDS)
    print "Average record size is %7.2f bytes" % (rep.DBYTES * 1.0 / rep.OIDS)
    print ("Average transaction size is %7.2f bytes" %
           (rep.DBYTES * 1.0 / rep.TIDS))

    print "Types used:"
    fmt = "%-46s %7s %9s %6s %7s"
    fmtp = "%-46s %7d %9d %5.1f%% %7.2f" # per-class format
    fmts = "%46s %7d %8dk %5.1f%% %7.2f" # summary format
    print fmt % ("Class Name", "Count", "TBytes", "Pct", "AvgSize")
    print fmt % ('-'*46, '-'*7, '-'*9, '-'*5, '-'*7)
    typemap = rep.TYPEMAP.keys()
    typemap.sort()
    cumpct = 0.0
    for t in typemap:
        pct = rep.TYPESIZE[t] * 100.0 / rep.DBYTES
        cumpct += pct
        print fmtp % (shorten(t, 46), rep.TYPEMAP[t], rep.TYPESIZE[t],
                      pct, rep.TYPESIZE[t] * 1.0 / rep.TYPEMAP[t])

    print fmt % ('='*46, '='*7, '='*9, '='*5, '='*7)
    print "%46s %7d %9s %6s %6.2fk" % ('Total Transactions', rep.TIDS, ' ',
        ' ', rep.DBYTES * 1.0 / rep.TIDS / 1024.0)
    print fmts % ('Total Records', rep.OIDS, rep.DBYTES / 1024.0, cumpct,
                  rep.DBYTES * 1.0 / rep.OIDS)

    print fmts % ('Current Objects', rep.COIDS, rep.CBYTES / 1024.0,
                  rep.CBYTES * 100.0 / rep.DBYTES,
                  rep.CBYTES * 1.0 / rep.COIDS)
    if rep.FOIDS:
        print fmts % ('Old Objects', rep.FOIDS, rep.FBYTES / 1024.0,
                      rep.FBYTES * 100.0 / rep.DBYTES,
                      rep.FBYTES * 1.0 / rep.FOIDS)

def analyze(path):
    fs = FileStorage(path, read_only=1)
    fsi = fs.iterator()
    report = Report()
    while 1:
        try:
            transaction = fsi.next()
        except IndexError:
            break
        analyze_trans(report, transaction)
    return report

def analyze_trans(report, txn):
    report.TIDS += 1
    while 1:
        try:
            rec = txn.next()
        except IndexError:
            break
        analyze_rec(report, rec)

def get_type(record):
    try:
        classinfo = pickle.loads(record.data)[0]
    except SystemError, err:
        s = str(err)
        mo = re.match('Failed to import class (\S+) from module (\S+)', s)
        if mo is None:
            raise
        else:
            klass, mod = mo.group(1, 2)
            return "%s.%s" % (mod, klass)
    if isinstance(classinfo, types.TupleType):
        mod, klass = classinfo
        return "%s.%s" % (mod, klass)
    else:
        return str(classinfo)

def analyze_rec(report, record):
    oid = record.oid
    report.OIDS += 1
    try:
        size = len(record.data) # Ignores various overhead
        report.DBYTES += size
        if not report.OIDMAP.has_key(oid):
            type = get_type(record)
            report.OIDMAP[oid] = type
            report.USEDMAP[oid] = size
            report.COIDS += 1
            report.CBYTES += size
        else:
            type = report.OIDMAP[oid]
            fsize = report.USEDMAP[oid]
            report.FREEMAP[oid] = report.FREEMAP.get(oid, 0) + fsize
            report.USEDMAP[oid] = size
            report.FOIDS += 1
            report.FBYTES += fsize
            report.CBYTES += size - fsize
        report.TYPEMAP[type] = report.TYPEMAP.get(type, 0) + 1
        report.TYPESIZE[type] = report.TYPESIZE.get(type, 0) + size
    except Exception, err:
        print err

if __name__ == "__main__":
    path = sys.argv[1]
    report(analyze(path))
