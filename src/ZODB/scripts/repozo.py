#!/usr/bin/env python

# repozo.py -- incremental and full backups of a Data.fs file.
#
# Originally written by Anthony Baxter
# Significantly modified by Barry Warsaw

"""repozo.py -- incremental and full backups of a Data.fs file and index.

Usage: %(program)s [options]
Where:

    Exactly one of -B, -R, or -V must be specified:

    -B / --backup
        Backup current ZODB file.

    -R / --recover
        Restore a ZODB file from a backup.

    -V / --verify
        Verify backup integrity.

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

    -w
    --with-verify
        Verify on the fly the backup files on recovering. This option runs
        the same checks as when repozo is run in -V/--verify mode, and
        allows to verify and recover a backup in one single step. If a sanity
        check fails, the partially recovered ZODB will be left in place.

Options for -V/--verify:
    -Q / --quick
        Verify file sizes only (skip md5 checksums).
"""
from __future__ import print_function

import errno
import getopt
import gzip
import os
import re
import shutil
import sys
import time
from hashlib import md5

from six.moves import filter

from ZODB.FileStorage import FileStorage


program = sys.argv[0]

BACKUP = 1
RECOVER = 2
VERIFY = 3

COMMASPACE = ', '
READCHUNK = 16 * 1024
VERBOSE = False


class RepozoError(Exception):
    pass


class WouldOverwriteFiles(RepozoError):
    pass


class NoFiles(RepozoError):
    pass


class VerificationFail(RepozoError):
    pass


class _GzipCloser(object):

    def __init__(self, fqn, mode):
        self._opened = gzip.open(fqn, mode)

    def __enter__(self):
        return self._opened

    def __exit__(self, exc_type, exc_value, traceback):
        self._opened.close()


def usage(code, msg=''):
    outfp = sys.stderr
    if code == 0:
        outfp = sys.stdout

    print(__doc__ % globals(), file=outfp)
    if msg:
        print(msg, file=outfp)

    sys.exit(code)


def log(msg, *args):
    if VERBOSE:
        # Use stderr here so that -v flag works with -R and no -o
        print(msg % args, file=sys.stderr)


def error(msg, *args):
    print(msg % args, file=sys.stderr)


def parseargs(argv):
    global VERBOSE
    try:
        opts, args = getopt.getopt(argv, 'BRVvhr:f:FQzkD:o:w',
                                   ['backup',
                                    'recover',
                                    'verify',
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
                                    'with-verify',
                                    ])
    except getopt.error as msg:
        usage(1, msg)

    class Options(object):
        mode = None         # BACKUP, RECOVER or VERIFY
        file = None         # name of input Data.fs file
        repository = None   # name of directory holding backups
        full = False        # True forces full backup
        date = None         # -D argument, if any
        output = None       # where to write recovered data; None = stdout
        quick = False       # -Q flag state
        gzip = False        # -z flag state
        killold = False     # -k flag state
        withverify = False  # -w flag state

    options = Options()

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage(0)
        elif opt in ('-v', '--verbose'):
            VERBOSE = True
        elif opt in ('-R', '--recover'):
            if options.mode is not None:
                usage(1, '-B, -R, and -V are mutually exclusive')
            options.mode = RECOVER
        elif opt in ('-B', '--backup'):
            if options.mode is not None:
                usage(1, '-B, -R, and -V are mutually exclusive')
            options.mode = BACKUP
        elif opt in ('-V', '--verify'):
            if options.mode is not None:
                usage(1, '-B, -R, and -V are mutually exclusive')
            options.mode = VERIFY
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
        elif opt in ('-w', '--with-verify'):
            options.withverify = True
        else:
            assert False, (opt, arg)

    # Any other arguments are invalid
    if args:
        usage(1, 'Invalid arguments: ' + COMMASPACE.join(args))

    # Sanity checks
    if options.mode is None:
        usage(1, 'Either --backup, --recover or --verify is required')
    if options.repository is None:
        usage(1, '--repository is required')
    if options.mode == BACKUP:
        if options.date is not None:
            log('--date option is ignored in backup mode')
            options.date = None
        if options.output is not None:
            log('--output option is ignored in backup mode')
            options.output = None
        if options.withverify is not None:
            log('--with-verify option is ignored in backup mode')
            options.withverify = None
        if not options.file:
            usage(1, '--file is required in backup mode')
    elif options.mode == RECOVER:
        if options.file is not None:
            log('--file option is ignored in recover mode')
            options.file = None
        if options.killold:
            log('--kill-old-on-full option is ignored in recover mode')
            options.killold = False
    else:
        assert options.mode == VERIFY
        if options.date is not None:
            log("--date option is ignored in verify mode")
            options.date = None
        if options.output is not None:
            log('--output option is ignored in verify mode')
            options.output = None
        if options.full:
            log('--full option is ignored in verify mode')
            options.full = False
        if options.gzip:
            log('--gzip option is ignored in verify mode')
            options.gzip = False
        if options.file is not None:
            log('--file option is ignored in verify mode')
            options.file = None
        if options.killold:
            log('--kill-old-on-full option is ignored in verify mode')
            options.killold = False
        if options.withverify is not None:
            log('--with-verify option is ignored in verify mode')
            options.withverify = None
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
    bytesread = 0
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


def file_size(fp):
    # Compute number of bytes that can be read from fp
    def func(data):
        pass
    return dofile(func, fp, None)


def checksum_and_size(fp):
    # Checksum and return it with the size of the file
    sum = md5()

    def func(data):
        sum.update(data)
    size = dofile(func, fp, None)
    return sum.hexdigest(), size


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
    # Concatenate a bunch of files from the repository, output to 'ofp' if
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
    return bytesread, sum.hexdigest()


def gen_filedate(options):
    return getattr(options, 'test_now', time.gmtime()[:6])


def gen_filename(options, ext=None, now=None):
    if ext is None:
        if options.full:
            ext = '.fs'
        else:
            ext = '.deltafs'
        if options.gzip:
            ext += 'z'
    # Hook for testing
    if now is None:
        now = gen_filedate(options)
    t = now + (ext,)
    return '%04d-%02d-%02d-%02d-%02d-%02d%s' % t

# Return a list of files needed to reproduce state at time options.date.
# This is a list, in chronological order, of the .fs[z] and .deltafs[z]
# files, from the time of the most recent full backup preceding
# options.date, up to options.date.


is_data_file = re.compile(r'\d{4}(?:-\d\d){5}\.(?:delta)?fsz?$').match
del re


def find_files(options):
    when = options.date
    if not when:
        when = gen_filename(options, ext='')
    log('looking for files between last full backup and %s...', when)
    # newest file first
    all = sorted(
        filter(is_data_file, os.listdir(options.repository)), reverse=True)
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
    fn = startpos = endpos = sum = None  # assume .dat file missing or empty
    try:
        fp = open(datfile)
    except IOError as e:
        if e.errno != errno.ENOENT:
            raise
    else:
        # We only care about the last one.
        lines = fp.readlines()
        fp.close()
        if lines:
            fn, startpos, endpos, sum = lines[-1].split()
            startpos = int(startpos)
            endpos = int(endpos)

    return fn, startpos, endpos, sum


def delete_old_backups(options):
    # Delete all full backup files except for the most recent full backup file
    all = sorted(filter(is_data_file, os.listdir(options.repository)))

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
    index = root + '.index'
    if index in deletable:
        deletable.remove(index)

    for fname in deletable:
        log('removing old backup file %s (and .dat / .index)', fname)
        root, ext = os.path.splitext(fname)
        try:
            os.unlink(os.path.join(options.repository, root + '.dat'))
        except OSError:
            pass
        try:
            os.unlink(os.path.join(options.repository, root + '.index'))
        except OSError:
            pass
        os.unlink(os.path.join(options.repository, fname))


def do_full_backup(options):
    options.full = True
    tnow = gen_filedate(options)
    dest = os.path.join(options.repository, gen_filename(options, now=tnow))
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
                              gen_filename(options, '.index', tnow))
    log('writing index')
    fs._index.save(pos, index_file)
    fs.close()
    log('writing full backup: %s bytes to %s', pos, dest)
    sum = copyfile(options, dest, 0, pos)
    # Write the data file for this full backup
    datfile = os.path.splitext(dest)[0] + '.dat'
    fp = open(datfile, 'w')
    print(dest, 0, pos, sum, file=fp)
    fp.flush()
    os.fsync(fp.fileno())
    fp.close()
    if options.killold:
        delete_old_backups(options)


def do_incremental_backup(options, reposz, repofiles):
    options.full = False
    tnow = gen_filedate(options)
    dest = os.path.join(options.repository, gen_filename(options, now=tnow))
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
                              gen_filename(options, '.index', tnow))
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
    print(dest, reposz, pos, sum, file=fp)
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
            raise NoFiles('No files in repository before %s' % (options.date,))
        else:
            raise NoFiles('No files in repository')

    files_to_close = ()
    if options.output is None:
        log('Recovering file to stdout')
        outfp = sys.stdout
    else:
        # Delete old ZODB before recovering backup as size of
        # old ZODB + full partial file may be superior to free disk space
        if os.path.exists(options.output):
            log('Deleting old %s', options.output)
            os.unlink(options.output)
        log('Recovering file to %s', options.output)
        temporary_output_file = options.output + '.part'
        outfp = open(temporary_output_file, 'wb')
        files_to_close += (outfp,)

    try:
        if options.withverify:
            datfile = os.path.splitext(repofiles[0])[0] + '.dat'
            with open(datfile) as fp:
                truth_dict = {}
                for line in fp:
                    fn, startpos, endpos, sum = line.split()
                    startpos = int(startpos)
                    endpos = int(endpos)
                    filename = os.path.join(options.repository,
                                            os.path.basename(fn))
                    truth_dict[filename] = {
                        'size': endpos - startpos,
                        'sum': sum,
                    }
            totalsz = 0
            for repofile in repofiles:
                reposz, reposum = concat([repofile], outfp)
                expected_truth = truth_dict[repofile]
                if reposz != expected_truth['size']:
                    raise VerificationFail(
                        "%s is %d bytes, should be %d bytes" % (
                            repofile, reposz, expected_truth['size']))
                if reposum != expected_truth['sum']:
                    raise VerificationFail(
                        "%s has checksum %s instead of %s" % (
                            repofile, reposum, expected_truth['sum']))
                totalsz += reposz
                log("Recovered chunk %s : %s bytes, md5: %s",
                    repofile, reposz, reposum)
            log("Recovered a total of %s bytes", totalsz)
        else:
            reposz, reposum = concat(repofiles, outfp)
            log('Recovered %s bytes, md5: %s', reposz, reposum)

        if options.output is not None:
            last_base = os.path.splitext(repofiles[-1])[0]
            source_index = '%s.index' % last_base
            target_index = '%s.index' % options.output
            if os.path.exists(source_index):
                log('Restoring index file %s to %s',
                    source_index, target_index)
                shutil.copyfile(source_index, target_index)
            else:
                log('No index file to restore: %s', source_index)
    finally:
        for f in files_to_close:
            f.close()

    if options.output is not None:
        try:
            os.rename(temporary_output_file, options.output)
        except OSError:
            log("ZODB has been fully recovered as %s, but it cannot be renamed"
                " into : %s", temporary_output_file, options.output)
            raise


def do_verify(options):
    # Verify the sizes and checksums of all files mentioned in the .dat file
    repofiles = find_files(options)
    if not repofiles:
        raise NoFiles('No files in repository')
    datfile = os.path.splitext(repofiles[0])[0] + '.dat'
    with open(datfile) as fp:
        for line in fp:
            fn, startpos, endpos, sum = line.split()
            startpos = int(startpos)
            endpos = int(endpos)
            filename = os.path.join(options.repository,
                                    os.path.basename(fn))
            expected_size = endpos - startpos
            log("Verifying %s", filename)
            try:
                if filename.endswith('fsz'):
                    actual_sum, size = get_checksum_and_size_of_gzipped_file(
                        filename, options.quick)
                    when_uncompressed = ' (when uncompressed)'
                else:
                    actual_sum, size = get_checksum_and_size_of_file(
                        filename, options.quick)
                    when_uncompressed = ''
            except IOError:
                error("%s is missing", filename)
                continue
            if size != expected_size:
                error("%s is %d bytes%s, should be %d bytes", filename,
                      size, when_uncompressed, expected_size)
            elif not options.quick:
                if actual_sum != sum:
                    error("%s has checksum %s%s instead of %s", filename,
                          actual_sum, when_uncompressed, sum)


def get_checksum_and_size_of_gzipped_file(filename, quick):
    with _GzipCloser(filename, 'rb') as fp:
        if quick:
            return None, file_size(fp)
        else:
            return checksum_and_size(fp)


def get_checksum_and_size_of_file(filename, quick):
    with open(filename, 'rb') as fp:
        fp.seek(0, 2)
        actual_size = fp.tell()
        if quick:
            actual_sum = None
        else:
            fp.seek(0)
            actual_sum = checksum(fp, actual_size)
    return actual_sum, actual_size


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    options = parseargs(argv)
    try:
        if options.mode == BACKUP:
            do_backup(options)
        elif options.mode == RECOVER:
            do_recover(options)
        else:
            assert options.mode == VERIFY
            do_verify(options)
    except (RepozoError, OSError) as e:
        sys.exit(str(e))


if __name__ == '__main__':
    main()
