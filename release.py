#! /usr/bin/env python
"""Update version numbers and release dates for the next release.

usage: release.py version date

version should be a string like "3.2c1"
date should be a string like "23-Sep-2003"

The following files are updated:
    - setup.py gets a version number
"""

import fileinput
import os
import re

def fixpath(path):
    parts = path.split("/")
    return os.sep.join(parts)

def replace(filename, pat, repl):
    parts = filename.split("/")
    filename = os.sep.join(parts)
    for line in fileinput.input([filename], inplace=True, backup="~"):
        print re.sub(pat, repl, line),

def compute_zeoversion(version):
    # ZEO version's trail ZODB versions by one full revision.
    # ZODB 3.2c1 corresponds to ZEO 2.2c1
    major, rest = version.split(".", 1)
    major = int(major) - 1
    return "%s.%s" % (major, rest)

def write_zeoversion(path, version):
    f = open(fixpath(path), "wb")
    print >> f, version
    f.close()

def main(args):
    version, date = args
    zeoversion = compute_zeoversion(version)

    replace("setup.py", 'version="\S+"', 'version="%s"' % version)
    replace("README.txt", "'\d+\.\d+[a-z]?\d*'", "'%s'" % version)
    replace("src/ZODB/__init__.py",
            "__version__ = '\S+'", "__version__ = '%s'" % version)
    replace("src/ZEO/__init__.py",
            'version = "\S+"', 'version = "%s"' % zeoversion)
    write_zeoversion("src/ZEO/version.txt", zeoversion)
    replace("NEWS.txt",
            "Release date: XX-\S+-\S+", "Release date: %s" % date)

if __name__ == "__main__":
    import sys
    main(sys.argv[1:])
