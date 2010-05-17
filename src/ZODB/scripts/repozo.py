#!/usr/bin/env python2.3

# repozo.py -- incremental and full backups of a Data.fs file.
#
# Originally written by Anthony Baxter
# Significantly modified by Barry Warsaw

"""repozo.py -- incremental and full backups of a Data.fs file and index.

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
        is required.  The directory must already exist.  You should not
        edit the files in this directory, or add your own files to it.

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

    -k / --kill-old-on-full
        If a full backup is created, remove any prior full or incremental
        backup files (and associated metadata files) from the repository
        directory.

Options for -R/--recover:
    -D str
    --date=str
        Recover state as of this date.  Specify UTC (not local) time.
            yyyy-mm-dd[-hh[-mm[-ss]]]
        By default, current time is used.

    -o filename
    --output=filename
        Write recovered ZODB to given file.  By default, the file is
        written to stdout.

        Note:  for the stdout case, the index file will **not** be restored
        automatically.
"""

import os
import shutil
import sys
try:
    # the hashlib package is available from Python 2.5
    from hashlib import md5
except ImportError:
    # the md5 package is deprecated in Python 2.6
    from md5 import new as md5
import gzip
import time
import errno
import getopt

from ZODB.FileStorage import FileStorage

program = sys.argv[0]

BACKUP = 1
RECOVER = 2

COMMASPACE = ', '
READCHUNK = 16 * 1024
VERBOSE = False


class WouldOverwriteFiles(Exception):
    pass


class NoFiles(Exception):
    pass


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


def parseargs(argv):
    global VERBOSE
    try:
        opts, args = getopt.getopt(argv, 'BRvhr:f:FQzkD:o:',
                                   ['backup',
                                    'recover',
                                    'verbose',
                                    'help',
                                    'repository=',
                                    'file=',
                                    'full',
                                    'quick',
                                    'gzip',
                                    'kill-old-on-full',
                                    'date=',
                                    'output=',
                                   ])
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
        killold = False     # -k flag state

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
        elif opt in ('-k', '--kill-old-on-full'):
            options.killold = True
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
        if options.killold is not None:
            log('--kill-old-on-full option is ignored in recover mode')
            options.killold = None
    return options


# afile is a Python file object, or created by gzip.open().  The latter
# doesn't have a fileno() method, so to fsync it we need to reach into
# its underlying file object.
def fsync(afile):
    afile.flush()
    fileobject = getattr(afile, 'fileobj', afile)
    os.fsync(fileobject.fileno())

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
    sum = md5()
    def func(data):
        sum.update(data)
    dofile(func, fp, n)
    return sum.hexdigest()


def copyfile(options, dst, start, n):
    # Copy bytes from file src, to file dst, starting at offset start, for n
    # length of bytes.  For robustness, we first write, flush and fsync
    # to a temp file, then rename the temp file at the end.
    sum = md5()
    ifp = open(options.file, 'rb')
    ifp.seek(start)
    tempname = os.path.join(os.path.dirname(dst), 'tmp.tmp')
    if options.gzip:
        ofp = gzip.open(tempname, 'wb')
    else:
        ofp = open(tempname, 'wb')

    def func(data):
        sum.update(data)
        ofp.write(data)

    ndone = dofile(func, ifp, n)
    assert ndone == n

    ifp.close()
    fsync(ofp)
    ofp.close()
    os.rename(tempname, dst)
    return sum.hexdigest()


def concat(files, ofp=None):
    # Concatenate a bunch of files from the repository, output to `outfile' if
    # given.  Return the number of bytes written and the md5 checksum of the
    # bytes.
    sum = md5()
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
    # Hook for testing
    now = getattr(options, 'test_now', time.gmtime()[:6])
    t = now + (ext,)
    return '%04d-%02d-%02d-%02d-%02d-%02d%s' % t

# Return a list of files needed to reproduce state at time options.date.
# This is a list, in chronological order, of the .fs[z] and .deltafs[z]
# files, from the time of the most recent full backup preceding
# options.date, up to options.date.

import re
is_data_file = re.compile(r'\d{4}(?:-\d\d){5}\.(?:delta)?fsz?$').match
del re

def find_files(options):
    when = options.date
    if not when:
        when = gen_filename(options, '')
    log('looking for files between last full backup and %s...', when)
    all = filter(is_data_file, os.listdir(options.repository))
    all.sort()
    all.reverse()   # newest file first
    # Find the last full backup before date, then include all the
    # incrementals between that full backup and "when".
    needed = []
    for fname in all:
        root, ext = os.path.splitext(fname)
        if root <= when:
            needed.append(fname)
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

def delete_old_backups(options):
    # Delete all full backup files except for the most recent full backup file
    all = filter(is_data_file, os.listdir(options.repository))
    all.sort()

    deletable = []
    full = []
    for fname in all:
        root, ext = os.path.splitext(fname)
        if ext in ('.fs', '.fsz'):
            full.append(fname)
        if ext in ('.fs', '.fsz', '.deltafs', '.deltafsz'):
            deletable.append(fname)

    # keep most recent full
    if not full:
        return

    recentfull = full.pop(-1)
    deletable.remove(recentfull)
    root, ext = os.path.splitext(recentfull)
    dat = root + '.dat'
    if dat in deletable:
        deletable.remove(dat)

    for fname in deletable:
        log('removing old backup file %s (and .dat)', fname)
        root, ext = os.path.splitext(fname)
        try:
            os.unlink(os.path.join(options.repository, root + '.dat'))
        except OSError:
            pass
        os.unlink(os.path.join(options.repository, fname))

def do_full_backup(options):
    options.full = True
    dest = os.path.join(options.repository, gen_filename(options))
    if os.path.exists(dest):
        raise WouldOverwriteFiles('Cannot overwrite existing file: %s' % dest)
    # Find the file position of the last completed transaction.
    fs = FileStorage(options.file, read_only=True)
    # Note that the FileStorage ctor calls read_index() which scans the file
    # and returns "the position just after the last valid transaction record".
    # getSize() then returns this position, which is exactly what we want,
    # because we only want to copy stuff from the beginning of the file to the
    # last valid transaction record.
    pos = fs.getSize()
    # Save the storage index into the repository
    index_file = os.path.join(options.repository,
                              gen_filename(options, '.index'))
    log('writing index')
    fs._index.save(pos, index_file)
    fs.close()
    log('writing full backup: %s bytes to %s', pos, dest)
    sum = copyfile(options, dest, 0, pos)
    # Write the data file for this full backup
    datfile = os.path.splitext(dest)[0] + '.dat'
    fp = open(datfile, 'w')
    print >> fp, dest, 0, pos, sum
    fp.flush()
    os.fsync(fp.fileno())
    fp.close()
    if options.killold:
        delete_old_backups(options)


def do_incremental_backup(options, reposz, repofiles):
    options.full = False
    dest = os.path.join(options.repository, gen_filename(options))
    if os.path.exists(dest):
        raise WouldOverwriteFiles('Cannot overwrite existing file: %s' % dest)
    # Find the file position of the last completed transaction.
    fs = FileStorage(options.file, read_only=True)
    # Note that the FileStorage ctor calls read_index() which scans the file
    # and returns "the position just after the last valid transaction record".
    # getSize() then returns this position, which is exactly what we want,
    # because we only want to copy stuff from the beginning of the file to the
    # last valid transaction record.
    pos = fs.getSize()
    log('writing index')
    index_file = os.path.join(options.repository,
                              gen_filename(options, '.index'))
    fs._index.save(pos, index_file)
    fs.close()
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
    fp.flush()
    os.fsync(fp.fileno())
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
            raise NoFiles('No files in repository before %s', options.date)
        else:
            raise NoFiles('No files in repository')
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

    if options.output is not None:
        last_base = os.path.splitext(repofiles[-1])[0]
        source_index = '%s.index' % last_base
        target_index = '%s.index' % options.output
        if os.path.exists(source_index):
            log('Restoring index file %s to %s', source_index, target_index)
            shutil.copyfile(source_index, target_index)
        else:
            log('No index file to restore: %s', source_index)


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    options = parseargs(argv)
    if options.mode == BACKUP:
        try:
            do_backup(options)
        except WouldOverwriteFiles, e:
            print >> sys.stderr, str(e)
            sys.exit(1)
    else:
        assert options.mode == RECOVER
        try:
            do_recover(options)
        except NoFiles, e:
            print >> sys.stderr, str(e)
            sys.exit(1)


if __name__ == '__main__':
    main()
