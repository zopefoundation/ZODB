#! /usr/bin/env python
"""Update version numbers and release dates for the next release.

usage: release.py version date

version should be a string like "3.2c1"
date should be a string like "23-Sep-2003"

The following files are updated:
    - setup.py
    - NEWS.txt
    - doc/guide/zodb.tex
    - src/ZEO/__init__.py
    - src/ZEO/version.txt
    - src/ZODB/__init__.py
"""

import fileinput
import os
import re

# In file filename, replace the first occurrence of regexp pat with
# string repl.
def replace(filename, pat, repl):
    foundone = False
    for line in fileinput.input([filename], inplace=True, backup="~"):
        if foundone:
            print line,
        else:
            new = re.sub(pat, repl, line)
            if new != line:
                foundone = True
                print "In %r, replaced:" % filename
                print "   ", line
                print "by:"
                print "   ", new
            print new,

    if not foundone:
        print "*" * 60, "Oops!"
        print "    Failed to find %r in %r" % (pat, filename)

def compute_zeoversion(version):
    # ZEO version's trail ZODB versions by one full revision.
    # ZODB 3.2c1 corresponds to ZEO 2.2c1
    major, rest = version.split(".", 1)
    major = int(major) - 1
    return "%s.%s" % (major, rest)

def write_zeoversion(path, version):
    f = file(path, "w")
    print >> f, version
    f.close()

def main(args):
    version, date = args
    zeoversion = compute_zeoversion(version)

    replace("setup.py",
            r'version="\S+"',
            'version="%s"' % version)
    replace("src/ZODB/__init__.py",
            r'__version__ = "\S+"',
            '__version__ = "%s"' % version)
    replace("src/ZEO/__init__.py",
            r'version = "\S+"',
            'version = "%s"' % zeoversion)
    write_zeoversion("src/ZEO/version.txt", zeoversion)
    replace("NEWS.txt",
            r"^Release date: .*",
            "Release date: %s" % date)

if __name__ == "__main__":
    import sys
    main(sys.argv[1:])
