#!/usr/bin/env python2.3

# repozo.py -- incremental and full backups of a Data.fs file.
#
# Originally written by Anthony Baxter
# Significantly modified by Barry Warsaw

"""repozo.py -- incremental and full backups of a Data.fs file.

Usage: %(program)s [options]
Where:

    Exactly one of -B or -R must be specified:

    -B / --backup
        Backup current ZODB file.

    -R / --recover
        Restore a ZODB file from a backup.

    -v / --verbose
        Verbose mode.

    -h / --help
        Print this text and exit.

    -r dir
    --repository=dir
        Repository directory containing the backup files.  This argument
        is required.

Options for -B/--backup:
    -f file
    --file=file
        Source Data.fs file.  This argument is required.

    -F / --full
        Force a full backup.  By default, an incremental backup is made
        if possible (e.g., if a pack has occurred since the last
        incremental backup, a full backup is necessary).

    -Q / --quick
        Verify via md5 checksum only the last incremental written.  This
        significantly reduces the disk i/o at the (theoretical) cost of
        inconsistency.  This is a probabilistic way of determining whether
        a full backup is necessary.

    -z / --gzip
        Compress with gzip the backup files.  Uses the default zlib
        compression level.  By default, gzip compression is not used.

Options for -R/--recover:
    -D str
    --date=str
        Recover state as of this date.  str is in the format
            yyyy-mm-dd[-hh[-mm]]
        By default, current time is used.

    -o filename
    --output=filename
        Write recovered ZODB to given file.  By default, the file is
        written to stdout.
"""

import os
import sys
import md5
import gzip
import time
import errno
import getopt

from ZODB.FileStorage import FileStorage

program = sys.argv[0]

try:
    True, False
except NameError:
    True = 1
    False = 0

BACKUP = 1
RECOVER = 2

COMMASPACE = ', '
READCHUNK = 16 * 1024
VERBOSE = False



def usage(code, msg=''):
    outfp = sys.stderr
    if code == 0:
        outfp = sys.stdout

    print >> outfp, __doc__ % globals()
    if msg:
        print >> outfp, msg

    sys.exit(code)


def log(msg, *args):
    if VERBOSE:
        # Use stderr here so that -v flag works with -R and no -o
        print >> sys.stderr, msg % args



def parseargs():
    global VERBOSE
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'BRvhf:r:FD:o:Qz',
                                   ['backup', 'recover', 'verbose', 'help',
                                    'file=', 'repository=', 'full', 'date=',
                                    'output=', 'quick', 'gzip'])
    except getopt.error, msg:
        usage(1, msg)

    class Options:
        mode = None         # BACKUP or RECOVER
        file = None         # name of input Data.fs file
        repository = None   # name of directory holding backups
        full = False        # True forces full backup
        date = None         # -D argument, if any
        output = None       # where to write recovered data; None = stdout
        quick = False       # -Q flag state
        gzip = False        # -z flag state

    options = Options()

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage(0)
        elif opt in ('-v', '--verbose'):
            VERBOSE = True
        elif opt in ('-R', '--recover'):
            if options.mode is not None:
                usage(1, '-B and -R are mutually exclusive')
            options.mode = RECOVER
        elif opt in ('-B', '--backup'):
            if options.mode is not None:
                usage(1, '-B and -R are mutually exclusive')
            options.mode = BACKUP
        elif opt in ('-Q', '--quick'):
            options.quick = True
        elif opt in ('-f', '--file'):
            options.file = arg
        elif opt in ('-r', '--repository'):
            options.repository = arg
        elif opt in ('-F', '--full'):
            options.full = True
        elif opt in ('-D', '--date'):
            options.date = arg
        elif opt in ('-o', '--output'):
            options.output = arg
        elif opt in ('-z', '--gzip'):
            options.gzip = True
        else:
            assert False, (opt, arg)

    # Any other arguments are invalid
    if args:
        usage(1, 'Invalid arguments: ' + COMMASPACE.join(args))

    # Sanity checks
    if options.mode is None:
        usage(1, 'Either --backup or --recover is required')
    if options.repository is None:
        usage(1, '--repository is required')
    if options.mode == BACKUP:
        if options.date is not None:
            log('--date option is ignored in backup mode')
            options.date = None
        if options.output is not None:
            log('--output option is ignored in backup mode')
            options.output = None
    else:
        assert options.mode == RECOVER
        if options.file is not None:
            log('--file option is ignored in recover mode')
            options.file = None
    return options



# Read bytes (no more than n, or to EOF if n is None) in chunks from the
# current position in file fp.  Pass each chunk as an argument to func().
# Return the total number of bytes read == the total number of bytes
# passed in all to func().  Leaves the file position just after the
# last byte read.
def dofile(func, fp, n=None):
    bytesread = 0L
    while n is None or n > 0:
        if n is None:
            todo = READCHUNK
        else:
            todo = min(READCHUNK, n)
        data = fp.read(todo)
        if not data:
            break
        func(data)
        nread = len(data)
        bytesread += nread
        if n is not None:
            n -= nread
    return bytesread


def checksum(fp, n):
    # Checksum the first n bytes of the specified file
    sum = md5.new()
    def func(data):
        sum.update(data)
    dofile(func, fp, n)
    return sum.hexdigest()


def copyfile(options, dst, start, n):
    # Copy bytes from file src, to file dst, starting at offset start, for n
    # length of bytes
    sum = md5.new()
    ifp = open(options.file, 'rb')
    ifp.seek(start)
    if options.gzip:
        ofp = gzip.open(dst, 'wb')
    else:
        ofp = open(dst, 'wb')
    def func(data):
        sum.update(data)
        ofp.write(data)
    ndone = dofile(func, ifp, n)
    ofp.close()
    ifp.close()
    assert ndone == n
    return sum.hexdigest()


def concat(files, ofp=None):
    # Concatenate a bunch of files from the repository, output to `outfile' if
    # given.  Return the number of bytes written and the md5 checksum of the
    # bytes.
    sum = md5.new()
    def func(data):
        sum.update(data)
        if ofp:
            ofp.write(data)
    bytesread = 0
    for f in files:
        # Auto uncompress
        if f.endswith('fsz'):
            ifp = gzip.open(f, 'rb')
        else:
            ifp = open(f, 'rb')
        bytesread += dofile(func, ifp)
        ifp.close()
    if ofp:
        ofp.close()
    return bytesread, sum.hexdigest()


def gen_filename(options, ext=None):
    if ext is None:
        if options.full:
            ext = '.fs'
        else:
            ext = '.deltafs'
        if options.gzip:
            ext += 'z'
    t = time.gmtime()[:6] + (ext,)
    return '%04d-%02d-%02d-%02d-%02d-%02d%s' % t


def find_files(options):
    def rootcmp(x, y):
        # This already compares in reverse order
        return cmp(os.path.splitext(y)[0], os.path.splitext(x)[0])
    # Return a list of files needed to reproduce state at time `when'
    when = options.date
    if not when:
        when = gen_filename(options, '')
    log('looking for files b/w last full backup and %s...', when)
    all = os.listdir(options.repository)
    all.sort(rootcmp)
    # Find the last full backup before date, then include all the incrementals
    # between when and that full backup.
    needed = []
    for file in all:
        root, ext = os.path.splitext(file)
        if root <= when:
            needed.append(file)
        if ext in ('.fs', '.fsz'):
            break
    # Make the file names relative to the repository directory
    needed = [os.path.join(options.repository, f) for f in needed]
    # Restore back to chronological order
    needed.reverse()
    if needed:
        log('files needed to recover state as of %s:', when)
        for f in needed:
            log('\t%s', f)
    else:
        log('no files found')
    return needed

# Scan the .dat file corresponding to the last full backup performed.
# Return
#
#     filename, startpos, endpos, checksum
#
# of the last incremental.  If there is no .dat file, or the .dat file
# is empty, return
#
#     None, None, None, None

def scandat(repofiles):
    fullfile = repofiles[0]
    datfile = os.path.splitext(fullfile)[0] + '.dat'
    fn = startpos = endpos = sum = None # assume .dat file missing or empty
    try:
        fp = open(datfile)
    except IOError, e:
        if e.errno <> errno.ENOENT:
            raise
    else:
        # We only care about the last one.
        lines = fp.readlines()
        fp.close()
        if lines:
            fn, startpos, endpos, sum = lines[-1].split()
            startpos = long(startpos)
            endpos = long(endpos)

    return fn, startpos, endpos, sum



def do_full_backup(options):
    # Find the file position of the last completed transaction.
    fs = FileStorage(options.file, read_only=True)
    # Note that the FileStorage ctor calls read_index() which scans the file
    # and returns "the position just after the last valid transaction record".
    # getSize() then returns this position, which is exactly what we want,
    # because we only want to copy stuff from the beginning of the file to the
    # last valid transaction record.
    pos = fs.getSize()
    fs.close()
    options.full = True
    dest = os.path.join(options.repository, gen_filename(options))
    if os.path.exists(dest):
        print >> sys.stderr, 'Cannot overwrite existing file:', dest
        sys.exit(2)
    log('writing full backup: %s bytes to %s', pos, dest)
    sum = copyfile(options, dest, 0, pos)
    # Write the data file for this full backup
    datfile = os.path.splitext(dest)[0] + '.dat'
    fp = open(datfile, 'w')
    print >> fp, dest, 0, pos, sum
    fp.close()


def do_incremental_backup(options, reposz, repofiles):
    # Find the file position of the last completed transaction.
    fs = FileStorage(options.file, read_only=True)
    # Note that the FileStorage ctor calls read_index() which scans the file
    # and returns "the position just after the last valid transaction record".
    # getSize() then returns this position, which is exactly what we want,
    # because we only want to copy stuff from the beginning of the file to the
    # last valid transaction record.
    pos = fs.getSize()
    fs.close()
    options.full = False
    dest = os.path.join(options.repository, gen_filename(options))
    if os.path.exists(dest):
        print >> sys.stderr, 'Cannot overwrite existing file:', dest
        sys.exit(2)
    log('writing incremental: %s bytes to %s',  pos-reposz, dest)
    sum = copyfile(options, dest, reposz, pos - reposz)
    # The first file in repofiles points to the last full backup.  Use this to
    # get the .dat file and append the information for this incrementatl to
    # that file.
    fullfile = repofiles[0]
    datfile = os.path.splitext(fullfile)[0] + '.dat'
    # This .dat file better exist.  Let the exception percolate if not.
    fp = open(datfile, 'a')
    print >> fp, dest, reposz, pos, sum
    fp.close()


def do_backup(options):
    repofiles = find_files(options)
    # See if we need to do a full backup
    if options.full or not repofiles:
        log('doing a full backup')
        do_full_backup(options)
        return
    srcsz = os.path.getsize(options.file)
    if options.quick:
        fn, startpos, endpos, sum = scandat(repofiles)
        # If the .dat file was missing, or was empty, do a full backup
        if (fn, startpos, endpos, sum) == (None, None, None, None):
            log('missing or empty .dat file (full backup)')
            do_full_backup(options)
            return
        # Has the file shrunk, possibly because of a pack?
        if srcsz < endpos:
            log('file shrunk, possibly because of a pack (full backup)')
            do_full_backup(options)
            return
        # Now check the md5 sum of the source file, from the last
        # incremental's start and stop positions.
        srcfp = open(options.file, 'rb')
        srcfp.seek(startpos)
        srcsum = checksum(srcfp, endpos-startpos)
        srcfp.close()
        log('last incremental file: %s', fn)
        log('last incremental checksum: %s', sum)
        log('source checksum range: [%s..%s], sum: %s',
            startpos, endpos, srcsum)
        if sum == srcsum:
            if srcsz == endpos:
                log('No changes, nothing to do')
                return
            log('doing incremental, starting at: %s', endpos)
            do_incremental_backup(options, endpos, repofiles)
            return
    else:
        # This was is much slower, and more disk i/o intensive, but it's also
        # more accurate since it checks the actual existing files instead of
        # the information in the .dat file.
        #
        # See if we can do an incremental, based on the files that already
        # exist.  This call of concat() will not write an output file.
        reposz, reposum = concat(repofiles)
        log('repository state: %s bytes, md5: %s', reposz, reposum)
        # Get the md5 checksum of the source file, up to two file positions:
        # the entire size of the file, and up to the file position of the last
        # incremental backup.
        srcfp = open(options.file, 'rb')
        srcsum = checksum(srcfp, srcsz)
        srcfp.seek(0)
        srcsum_backedup = checksum(srcfp, reposz)
        srcfp.close()
        log('current state   : %s bytes, md5: %s', srcsz, srcsum)
        log('backed up state : %s bytes, md5: %s', reposz, srcsum_backedup)
        # Has nothing changed?
        if srcsz == reposz and srcsum == reposum:
            log('No changes, nothing to do')
            return
        # Has the file shrunk, probably because of a pack?
        if srcsz < reposz:
            log('file shrunk, possibly because of a pack (full backup)')
            do_full_backup(options)
            return
        # The source file is larger than the repository.  If the md5 checksums
        # match, then we know we can do an incremental backup.  If they don't,
        # then perhaps the file was packed at some point (or a
        # non-transactional undo was performed, but this is deprecated).  Only
        # do a full backup if forced to.
        #
        # XXX For ZODB4, this needs to take into account the storage metadata
        # header that FileStorage has grown at the front of the file.
        if reposum == srcsum_backedup:
            log('doing incremental, starting at: %s', reposz)
            do_incremental_backup(options, reposz, repofiles)
            return
    # The checksums don't match, meaning the front of the source file has
    # changed.  We'll need to do a full backup in that case.
    log('file changed, possibly because of a pack (full backup)')
    do_full_backup(options)



def do_recover(options):
    # Find the first full backup at or before the specified date
    repofiles = find_files(options)
    if not repofiles:
        if options.date:
            log('No files in repository before %s', options.date)
        else:
            log('No files in repository')
        return
    if options.output is None:
        log('Recovering file to stdout')
        outfp = sys.stdout
    else:
        log('Recovering file to %s', options.output)
        outfp = open(options.output, 'wb')
    reposz, reposum = concat(repofiles, outfp)
    if outfp <> sys.stdout:
        outfp.close()
    log('Recovered %s bytes, md5: %s', reposz, reposum)



def main():
    options = parseargs()
    if options.mode == BACKUP:
        do_backup(options)
    else:
        assert options.mode == RECOVER
        do_recover(options)


if __name__ == '__main__':
    main()
