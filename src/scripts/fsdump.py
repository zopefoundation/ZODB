#! /usr/bin/env python
"""Print a text summary of the contents of a FileStorage."""

from ZODB.fsdump import fsdump

if __name__ == "__main__":
    import sys
    fsdump(sys.argv[1])
