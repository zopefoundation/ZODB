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
    from sys import stderr as e # fileinput hijacks sys.stdout
    foundone = False
    for line in fileinput.input([filename], inplace=True, backup="~"):
        if foundone:
            print line,
        else:
            match = re.search(pat, line)
            if match is not None:
                foundone = True

                new = re.sub(pat, repl, line)
                print new,

                print >> e, "In %s, replaced:" % filename
                print >> e, "   ", repr(line)
                print >> e, "   ", repr(new)

            else:
                print line,

    if not foundone:
        print >> e, "*" * 60, "Oops!"
        print >> e, "    Failed to find %r in %r" % (pat, filename)

# Nothing in our codebase cares about ZEO/version.txt.  Jeremy said
# someone asked for it so that a shell script could read up the ZEO
# version easily.
# Before ZODB 3.4, the ZEO version was one smaller than the ZODB version;
# e.g., ZEO 2.2.7 shipped with ZODB 3.2.7.
def write_zeoversion(path, version):
    f = file(path, "w")
    print >> f, version
    f.close()

def main(args):
    version, date = args

    replace("setup.py",
            r'version="\S+"',
            'version="%s"' % version)
    replace("src/ZODB/__init__.py",
            r'__version__ = "\S+"',
            '__version__ = "%s"' % version)
    replace("src/ZEO/__init__.py",
            r'version = "\S+"',
            'version = "%s"' % version)
    write_zeoversion("src/ZEO/version.txt", version)
    replace("NEWS.txt",
            r"^Release date: .*",
            "Release date: %s" % date)
    replace("doc/guide/zodb.tex",
            r"release{\S+}",
            "release{%s}" % version)
if __name__ == "__main__":
    import sys
    main(sys.argv[1:])
