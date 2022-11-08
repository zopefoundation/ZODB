##############################################################################
#
# Copyright (c) 2008 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""A script to migrate a blob directory into a different layout.
"""
from __future__ import print_function

import logging
import optparse
import os
import shutil

from ZODB.blob import FilesystemHelper
from ZODB.utils import oid_repr


# Check if we actually have link
try:
    os.link
except AttributeError:
    link_or_copy = shutil.copy
else:
    def link_or_copy(f1, f2):
        try:
            os.link(f1, f2)
        except OSError:
            shutil.copy(f1, f2)


def migrate(source, dest, layout):
    source_fsh = FilesystemHelper(source)
    source_fsh.create()
    dest_fsh = FilesystemHelper(dest, layout)
    dest_fsh.create()
    print("Migrating blob data from `%s` (%s) to `%s` (%s)" % (
        source, source_fsh.layout_name, dest, dest_fsh.layout_name))
    for oid, path in source_fsh.listOIDs():
        dest_path = dest_fsh.getPathForOID(oid, create=True)
        files = os.listdir(path)
        for file in files:
            source_file = os.path.join(path, file)
            dest_file = os.path.join(dest_path, file)
            link_or_copy(source_file, dest_file)
        print("\tOID: %s - %s files " % (oid_repr(oid), len(files)))


def main(source=None, dest=None, layout="bushy"):
    usage = "usage: %prog [options] <source> <dest> <layout>"
    description = ("Create the new directory <dest> and migrate all blob "
                   "data <source> to <dest> while using the new <layout> for "
                   "<dest>")

    parser = optparse.OptionParser(usage=usage, description=description)
    parser.add_option("-l", "--layout",
                      default=layout, type='choice',
                      choices=['bushy', 'lawn'],
                      help="Define the layout to use for the new directory "
                      "(bushy or lawn). Default: %default")
    options, args = parser.parse_args()

    if not len(args) == 2:
        parser.error("source and destination must be given")

    logging.getLogger().addHandler(logging.StreamHandler())
    logging.getLogger().setLevel(0)

    source, dest = args
    migrate(source, dest, options.layout)


if __name__ == '__main__':
    main()
