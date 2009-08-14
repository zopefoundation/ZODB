##############################################################################
#
# Copyright Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

import optparse
import sys
import ZODB.BaseStorage
import ZODB.config
import ZODB.FileStorage

parser = optparse.OptionParser("""%prog input output

Remove version records by copying input to output, stripping any
version records seen in input.

The input and output arguments are file-storage file names.  If the -c
option is used, then the input and output arguments must be storage
configuration files.
""")
parser.add_option(
    "-c", "--config", dest="config", action="store_true",
    help=
    'Treat the input and output names as configuration file names,'
    ' rather than file-storage file names')

class Record:

    def __init__(self, transaction, it):
        self.__transaction = transaction
        self.__it = it

    def __getattr__(self, name):
        return getattr(self.__transaction, name)

    def __iter__(self):
        it = self.__it
        for record in self.__transaction:
            if record.version:
                it.versioned += 1
                continue  # Strip version records
            it.copied += 1
            if hasattr(record, 'data_txn'):
                record.data_txn = None
            yield record

class Iterator:

    versioned = copied = 0

    def __init__(self, iterator):
        self.close = iterator.close
        def it():
            for transaction in iterator:
                yield Record(transaction, self)
        self.__it = it()

    def next(self):
        return self.__it.next()

    def __iter__(self):
        return self
    iterator = __iter__

def main(args=None):
    if args is None:
        args = sys.argv[1:]

    (options, args) = parser.parse_args(args)

    if len(args) != 2:
        parser.parse_args(['-h'])

    input, output = args
    toclose = []
    if options.config:
        input_storage = ZODB.config.storageFromFile(open(input))
        it = input_storage.iterator()
        toclose.append(input_storage)
        output_storage = ZODB.config.storageFromFile(open(output))
    else:
        it  = ZODB.FileStorage.FileIterator(input)
        output_storage = ZODB.FileStorage.FileStorage(output)
    toclose.append(output_storage)

    it = Iterator(it)
    ZODB.BaseStorage.copy(it, output_storage)

    for s in toclose:
        s.close()

    print 'Copied', it.copied, 'records.'
    print 'Removed', it.versioned, 'versioned records.'
