#!/usr/bin/env python2.3

"""Print a text summary of the contents of a FileStorage."""

from ZODB.FileStorage.fsdump import fsdump

if __name__ == "__main__":
    import sys
    fsdump(sys.argv[1])
