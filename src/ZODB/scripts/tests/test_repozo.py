##############################################################################
#
# Copyright (c) 2004-2009 Zope Foundation and Contributors.
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
from __future__ import print_function

import os
import sys
import unittest
from hashlib import md5
from io import BytesIO
from io import StringIO

import ZODB.tests.util  # layer used at class scope


if str is bytes:
    NativeStringIO = BytesIO
else:
    NativeStringIO = StringIO


_NOISY = os.environ.get('NOISY_REPOZO_TEST_OUTPUT')


def _write_file(name, bits, mode='wb'):
    with open(name, mode) as f:
        f.write(bits)
        f.flush()


def _read_file(name, mode='rb'):
    with open(name, mode) as f:
        return f.read()


class OurDB(object):

    _file_name = None

    def __init__(self, dir):
        import transaction
        from BTrees.OOBTree import OOBTree
        self.dir = dir
        self.getdb()
        conn = self.db.open()
        conn.root()['tree'] = OOBTree()
        transaction.commit()
        self.pos = self.db.storage._pos
        self.close()

    def getdb(self):
        from ZODB import DB
        from ZODB.FileStorage import FileStorage
        self._file_name = storage_filename = os.path.join(self.dir, 'Data.fs')
        storage = FileStorage(storage_filename)
        self.db = DB(storage)

    def gettree(self):
        self.getdb()
        conn = self.db.open()
        return conn.root()['tree']

    def pack(self):
        self.getdb()
        self.db.pack()

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None

    def mutate(self):
        # Make random mutations to the btree in the database.
        import random

        import transaction
        tree = self.gettree()
        for dummy in range(100):
            if random.random() < 0.6:
                tree[random.randrange(100000)] = random.randrange(100000)
            else:
                keys = tree.keys()
                if keys:
                    del tree[keys[0]]
        transaction.commit()
        self.pos = self.db.storage._pos
        self.maxkey = self.db.storage._oid
        self.close()


class Test_parseargs(unittest.TestCase):

    def setUp(self):
        from ZODB.scripts import repozo
        self._old_verbosity = repozo.VERBOSE
        self._old_stderr = sys.stderr
        repozo.VERBOSE = False
        sys.stderr = NativeStringIO()

    def tearDown(self):
        from ZODB.scripts import repozo
        sys.stderr = self._old_stderr
        repozo.VERBOSE = self._old_verbosity

    def test_short(self):
        from ZODB.scripts import repozo
        options = repozo.parseargs(['-v', '-V', '-r', '/tmp/nosuchdir'])
        self.assertTrue(repozo.VERBOSE)
        self.assertEqual(options.mode, repozo.VERIFY)
        self.assertEqual(options.repository, '/tmp/nosuchdir')

    def test_long(self):
        from ZODB.scripts import repozo
        options = repozo.parseargs(['--verbose', '--verify',
                                    '--repository=/tmp/nosuchdir'])
        self.assertTrue(repozo.VERBOSE)
        self.assertEqual(options.mode, repozo.VERIFY)
        self.assertEqual(options.repository, '/tmp/nosuchdir')

    def test_help(self):
        from ZODB.scripts import repozo

        # Note: can't mock sys.stdout in our setUp: if a test fails,
        # zope.testrunner will happily print the traceback and failure message
        # into our StringIO before running our tearDown.
        old_stdout = sys.stdout
        sys.stdout = NativeStringIO()
        try:
            self.assertRaises(SystemExit, repozo.parseargs, ['--help'])
            self.assertIn('Usage:', sys.stdout.getvalue())
        finally:
            sys.stdout = old_stdout

    def test_bad_option(self):
        from ZODB.scripts import repozo
        self.assertRaises(SystemExit, repozo.parseargs,
                          ['--crash-please'])
        self.assertIn('option --crash-please not recognized',
                      sys.stderr.getvalue())

    def test_bad_argument(self):
        from ZODB.scripts import repozo
        self.assertRaises(SystemExit, repozo.parseargs,
                          ['crash', 'please'])
        self.assertIn('Invalid arguments: crash, please',
                      sys.stderr.getvalue())

    def test_mode_selection(self):
        from ZODB.scripts import repozo
        options = repozo.parseargs([
            '-B', '-f', '/tmp/Data.fs', '-r', '/tmp/nosuchdir'])
        self.assertEqual(options.mode, repozo.BACKUP)
        options = repozo.parseargs(['-R', '-r', '/tmp/nosuchdir'])
        self.assertEqual(options.mode, repozo.RECOVER)
        options = repozo.parseargs(['-V', '-r', '/tmp/nosuchdir'])
        self.assertEqual(options.mode, repozo.VERIFY)

    def test_mode_selection_is_mutually_exclusive(self):
        from ZODB.scripts import repozo
        self.assertRaises(SystemExit, repozo.parseargs, ['-B', '-R'])
        self.assertIn('-B, -R, and -V are mutually exclusive',
                      sys.stderr.getvalue())
        self.assertRaises(SystemExit, repozo.parseargs, ['-R', '-V'])
        self.assertRaises(SystemExit, repozo.parseargs, ['-V', '-B'])

    def test_mode_selection_required(self):
        from ZODB.scripts import repozo
        self.assertRaises(SystemExit, repozo.parseargs, [])
        self.assertIn('Either --backup, --recover or --verify is required',
                      sys.stderr.getvalue())

    def test_misc_flags(self):
        from ZODB.scripts import repozo
        options = repozo.parseargs([
            '-B', '-f', '/tmp/Data.fs', '-r', '/tmp/nosuchdir', '-F'])
        self.assertTrue(options.full)
        options = repozo.parseargs([
            '-B', '-f', '/tmp/Data.fs', '-r', '/tmp/nosuchdir', '-k'])
        self.assertTrue(options.killold)

    def test_repo_is_required(self):
        from ZODB.scripts import repozo
        self.assertRaises(SystemExit, repozo.parseargs, ['-B'])
        self.assertIn('--repository is required', sys.stderr.getvalue())

    def test_backup_ignored_args(self):
        from ZODB.scripts import repozo
        options = repozo.parseargs(['-B', '-r', '/tmp/nosuchdir', '-v',
                                    '-f', '/tmp/Data.fs',
                                    '-o', '/tmp/ignored.fs',
                                    '-D', '2011-12-13'])
        self.assertEqual(options.date, None)
        self.assertIn('--date option is ignored in backup mode',
                      sys.stderr.getvalue())
        self.assertEqual(options.output, None)
        self.assertIn('--output option is ignored in backup mode',
                      sys.stderr.getvalue())

    def test_backup_required_args(self):
        from ZODB.scripts import repozo
        self.assertRaises(SystemExit, repozo.parseargs,
                          ['-B', '-r', '/tmp/nosuchdir'])
        self.assertIn('--file is required', sys.stderr.getvalue())

    def test_recover_ignored_args(self):
        from ZODB.scripts import repozo
        options = repozo.parseargs(['-R', '-r', '/tmp/nosuchdir', '-v',
                                    '-f', '/tmp/ignored.fs',
                                    '-k'])
        self.assertEqual(options.file, None)
        self.assertIn('--file option is ignored in recover mode',
                      sys.stderr.getvalue())
        self.assertEqual(options.killold, False)
        self.assertIn('--kill-old-on-full option is ignored in recover mode',
                      sys.stderr.getvalue())

    def test_verify_ignored_args(self):
        from ZODB.scripts import repozo
        options = repozo.parseargs(['-V', '-r', '/tmp/nosuchdir', '-v',
                                    '-o', '/tmp/ignored.fs',
                                    '-D', '2011-12-13',
                                    '-f', '/tmp/ignored.fs',
                                    '-z', '-k', '-F'])
        self.assertEqual(options.date, None)
        self.assertIn('--date option is ignored in verify mode',
                      sys.stderr.getvalue())
        self.assertEqual(options.output, None)
        self.assertIn('--output option is ignored in verify mode',
                      sys.stderr.getvalue())
        self.assertEqual(options.full, False)
        self.assertIn('--full option is ignored in verify mode',
                      sys.stderr.getvalue())
        self.assertEqual(options.gzip, False)
        self.assertIn('--gzip option is ignored in verify mode',
                      sys.stderr.getvalue())
        self.assertEqual(options.file, None)
        self.assertIn('--file option is ignored in verify mode',
                      sys.stderr.getvalue())
        self.assertEqual(options.killold, False)
        self.assertIn('--kill-old-on-full option is ignored in verify mode',
                      sys.stderr.getvalue())


class FileopsBase(object):

    def _makeChunks(self):
        from ZODB.scripts.repozo import READCHUNK
        return [b'x' * READCHUNK, b'y' * READCHUNK, b'z']

    def _makeFile(self, text=None):
        if text is None:
            text = b''.join(self._makeChunks())
        return BytesIO(text)


class Test_dofile(unittest.TestCase, FileopsBase):

    def _callFUT(self, func, fp, n):
        from ZODB.scripts.repozo import dofile
        return dofile(func, fp, n)

    def test_empty_read_all(self):
        chunks = []
        file = self._makeFile(b'')
        bytes = self._callFUT(chunks.append, file, None)
        self.assertEqual(bytes, 0)
        self.assertEqual(chunks, [])

    def test_empty_read_count(self):
        chunks = []
        file = self._makeFile(b'')
        bytes = self._callFUT(chunks.append, file, 42)
        self.assertEqual(bytes, 0)
        self.assertEqual(chunks, [])

    def test_nonempty_read_all(self):
        chunks = []
        file = self._makeFile()
        bytes = self._callFUT(chunks.append, file, None)
        self.assertEqual(bytes, file.tell())
        self.assertEqual(chunks, self._makeChunks())

    def test_nonempty_read_count(self):
        chunks = []
        file = self._makeFile()
        bytes = self._callFUT(chunks.append, file, 42)
        self.assertEqual(bytes, 42)
        self.assertEqual(chunks, [b'x' * 42])


class Test_checksum(unittest.TestCase, FileopsBase):

    def _callFUT(self, fp, n):
        from ZODB.scripts.repozo import checksum
        return checksum(fp, n)

    def test_empty_read_all(self):
        file = self._makeFile(b'')
        sum = self._callFUT(file, None)
        self.assertEqual(sum, md5(b'').hexdigest())

    def test_empty_read_count(self):
        file = self._makeFile(b'')
        sum = self._callFUT(file, 42)
        self.assertEqual(sum, md5(b'').hexdigest())

    def test_nonempty_read_all(self):
        file = self._makeFile()
        sum = self._callFUT(file, None)
        self.assertEqual(sum, md5(b''.join(self._makeChunks())).hexdigest())

    def test_nonempty_read_count(self):
        file = self._makeFile()
        sum = self._callFUT(file, 42)
        self.assertEqual(sum, md5(b'x' * 42).hexdigest())


class OptionsTestBase(object):

    _repository_directory = None
    _data_directory = None

    def tearDown(self):
        if self._repository_directory is not None:
            from shutil import rmtree
            rmtree(self._repository_directory)
        if self._data_directory is not None:
            from shutil import rmtree
            rmtree(self._data_directory)

    def _makeOptions(self, **kw):
        import tempfile
        self._repository_directory = tempfile.mkdtemp(prefix='test-repozo-')

        class Options(object):
            repository = self._repository_directory
            date = None

            def __init__(self, **kw):
                self.__dict__.update(kw)
        return Options(**kw)


class Test_copyfile(OptionsTestBase, unittest.TestCase):

    def _callFUT(self, options, dest, start, n):
        from ZODB.scripts.repozo import copyfile
        return copyfile(options, dest, start, n)

    def test_no_gzip(self):
        options = self._makeOptions(gzip=False)
        source = options.file = os.path.join(self._repository_directory,
                                             'source.txt')
        _write_file(source, b'x' * 1000)
        target = os.path.join(self._repository_directory, 'target.txt')
        sum = self._callFUT(options, target, 0, 100)
        self.assertEqual(sum, md5(b'x' * 100).hexdigest())
        self.assertEqual(_read_file(target), b'x' * 100)

    def test_w_gzip(self):
        from ZODB.scripts.repozo import _GzipCloser
        options = self._makeOptions(gzip=True)
        source = options.file = os.path.join(self._repository_directory,
                                             'source.txt')
        _write_file(source, b'x' * 1000)
        target = os.path.join(self._repository_directory, 'target.txt')
        sum = self._callFUT(options, target, 0, 100)
        self.assertEqual(sum, md5(b'x' * 100).hexdigest())
        with _GzipCloser(target, 'rb') as f:
            self.assertEqual(f.read(), b'x' * 100)


class Test_concat(OptionsTestBase, unittest.TestCase):

    def _callFUT(self, files, ofp):
        from ZODB.scripts.repozo import concat
        return concat(files, ofp)

    def _makeFile(self, name, text, gzip_file=False):
        import tempfile

        from ZODB.scripts.repozo import _GzipCloser
        if self._repository_directory is None:
            self._repository_directory = tempfile.mkdtemp(prefix='zodb-test-')
        fqn = os.path.join(self._repository_directory, name)
        if gzip_file:
            _opener = _GzipCloser
        else:
            _opener = open
        with _opener(fqn, 'wb') as f:
            f.write(text)
            f.flush()
        return fqn

    def test_empty_list_no_ofp(self):
        bytes, sum = self._callFUT([], None)
        self.assertEqual(bytes, 0)
        self.assertEqual(sum, md5(b'').hexdigest())

    def test_w_plain_files_no_ofp(self):
        files = [self._makeFile(x, x.encode(), False) for x in 'ABC']
        bytes, sum = self._callFUT(files, None)
        self.assertEqual(bytes, 3)
        self.assertEqual(sum, md5(b'ABC').hexdigest())

    def test_w_gzipped_files_no_ofp(self):
        files = [self._makeFile('%s.fsz' % x, x.encode(), True) for x in 'ABC']
        bytes, sum = self._callFUT(files, None)
        self.assertEqual(bytes, 3)
        self.assertEqual(sum, md5(b'ABC').hexdigest())

    def test_w_ofp(self):

        class Faux(object):
            _closed = False

            def __init__(self):
                self._written = []

            def write(self, data):
                self._written.append(data)

            def close(self):
                self._closed = True

        files = [self._makeFile(x, x.encode(), False) for x in 'ABC']
        ofp = Faux()
        bytes, sum = self._callFUT(files, ofp)
        self.assertEqual(ofp._written, [x.encode() for x in 'ABC'])
        self.assertFalse(ofp._closed)


_marker = object()


class Test_gen_filename(OptionsTestBase, unittest.TestCase):

    def _callFUT(self, options, ext=_marker):
        from ZODB.scripts.repozo import gen_filename
        if ext is _marker:
            return gen_filename(options)
        return gen_filename(options, ext)

    def test_explicit_ext(self):
        options = self._makeOptions(test_now=(2010, 5, 14, 12, 52, 31))
        fn = self._callFUT(options, '.txt')
        self.assertEqual(fn, '2010-05-14-12-52-31.txt')

    def test_full_no_gzip(self):
        options = self._makeOptions(test_now=(2010, 5, 14, 12, 52, 31),
                                    full=True,
                                    gzip=False,
                                    )
        fn = self._callFUT(options)
        self.assertEqual(fn, '2010-05-14-12-52-31.fs')

    def test_full_w_gzip(self):
        options = self._makeOptions(test_now=(2010, 5, 14, 12, 52, 31),
                                    full=True,
                                    gzip=True,
                                    )
        fn = self._callFUT(options)
        self.assertEqual(fn, '2010-05-14-12-52-31.fsz')

    def test_incr_no_gzip(self):
        options = self._makeOptions(test_now=(2010, 5, 14, 12, 52, 31),
                                    full=False,
                                    gzip=False,
                                    )
        fn = self._callFUT(options)
        self.assertEqual(fn, '2010-05-14-12-52-31.deltafs')

    def test_incr_w_gzip(self):
        options = self._makeOptions(test_now=(2010, 5, 14, 12, 52, 31),
                                    full=False,
                                    gzip=True,
                                    )
        fn = self._callFUT(options)
        self.assertEqual(fn, '2010-05-14-12-52-31.deltafsz')


class Test_find_files(OptionsTestBase, unittest.TestCase):

    def _callFUT(self, options):
        from ZODB.scripts.repozo import find_files
        return find_files(options)

    def _makeFile(self, hour, min, sec, ext):
        # call _makeOptions first!
        name = '2010-05-14-%02d-%02d-%02d%s' % (hour, min, sec, ext)
        fqn = os.path.join(self._repository_directory, name)
        _write_file(fqn, name.encode())
        return fqn

    def test_no_files(self):
        options = self._makeOptions(date='2010-05-14-13-30-57')
        found = self._callFUT(options)
        self.assertEqual(found, [])

    def test_explicit_date(self):
        options = self._makeOptions(date='2010-05-14-13-30-57')
        files = []
        for h, m, s, e in [(2, 13, 14, '.fs'),
                           (2, 13, 14, '.dat'),
                           (3, 14, 15, '.deltafs'),
                           (4, 14, 15, '.deltafs'),
                           (5, 14, 15, '.deltafs'),
                           (12, 13, 14, '.fs'),
                           (12, 13, 14, '.dat'),
                           (13, 14, 15, '.deltafs'),
                           (14, 15, 16, '.deltafs'),
                           ]:
            files.append(self._makeFile(h, m, s, e))
        found = self._callFUT(options)
        # Older files, .dat file not included
        self.assertEqual(found, [files[5], files[7]])

    def test_using_gen_filename(self):
        options = self._makeOptions(date=None,
                                    test_now=(2010, 5, 14, 13, 30, 57))
        files = []
        for h, m, s, e in [(2, 13, 14, '.fs'),
                           (2, 13, 14, '.dat'),
                           (3, 14, 15, '.deltafs'),
                           (4, 14, 15, '.deltafs'),
                           (5, 14, 15, '.deltafs'),
                           (12, 13, 14, '.fs'),
                           (12, 13, 14, '.dat'),
                           (13, 14, 15, '.deltafs'),
                           (14, 15, 16, '.deltafs'),
                           ]:
            files.append(self._makeFile(h, m, s, e))
        found = self._callFUT(options)
        # Older files, .dat file not included
        self.assertEqual(found, [files[5], files[7]])


class Test_scandat(OptionsTestBase, unittest.TestCase):

    def _callFUT(self, repofiles):
        from ZODB.scripts.repozo import scandat
        return scandat(repofiles)

    def test_no_dat_file(self):
        self._makeOptions()
        fsfile = os.path.join(self._repository_directory, 'foo.fs')
        fn, startpos, endpos, sum = self._callFUT([fsfile])
        self.assertEqual(fn, None)
        self.assertEqual(startpos, None)
        self.assertEqual(endpos, None)
        self.assertEqual(sum, None)

    def test_empty_dat_file(self):
        self._makeOptions()
        fsfile = os.path.join(self._repository_directory, 'foo.fs')
        datfile = os.path.join(self._repository_directory, 'foo.dat')
        _write_file(datfile, b'')
        fn, startpos, endpos, sum = self._callFUT([fsfile])
        self.assertEqual(fn, None)
        self.assertEqual(startpos, None)
        self.assertEqual(endpos, None)
        self.assertEqual(sum, None)

    def test_single_line(self):
        self._makeOptions()
        fsfile = os.path.join(self._repository_directory, 'foo.fs')
        datfile = os.path.join(self._repository_directory, 'foo.dat')
        _write_file(datfile, b'foo.fs 0 123 ABC\n')
        fn, startpos, endpos, sum = self._callFUT([fsfile])
        self.assertEqual(fn, 'foo.fs')
        self.assertEqual(startpos, 0)
        self.assertEqual(endpos, 123)
        self.assertEqual(sum, 'ABC')

    def test_multiple_lines(self):
        self._makeOptions()
        fsfile = os.path.join(self._repository_directory, 'foo.fs')
        datfile = os.path.join(self._repository_directory, 'foo.dat')
        _write_file(datfile, b'foo.fs 0 123 ABC\n'
                             b'bar.deltafs 123 456 DEF\n')
        fn, startpos, endpos, sum = self._callFUT([fsfile])
        self.assertEqual(fn, 'bar.deltafs')
        self.assertEqual(startpos, 123)
        self.assertEqual(endpos, 456)
        self.assertEqual(sum, 'DEF')


class Test_delete_old_backups(OptionsTestBase, unittest.TestCase):

    def _makeOptions(self, filenames=()):
        options = super(Test_delete_old_backups, self)._makeOptions()
        for filename in filenames:
            fqn = os.path.join(options.repository, filename)
            _write_file(fqn, b'testing delete_old_backups')
        return options

    def _callFUT(self, options=None, filenames=()):
        from ZODB.scripts.repozo import delete_old_backups
        if options is None:
            options = self._makeOptions(filenames)
        return delete_old_backups(options)

    def test_empty_dir_doesnt_raise(self):
        self._callFUT()
        self.assertEqual(len(os.listdir(self._repository_directory)), 0)

    def test_no_repozo_files_doesnt_raise(self):
        FILENAMES = ['bogus.txt', 'not_a_repozo_file']
        self._callFUT(filenames=FILENAMES)
        remaining = os.listdir(self._repository_directory)
        self.assertEqual(len(remaining), len(FILENAMES))
        for name in FILENAMES:
            fqn = os.path.join(self._repository_directory, name)
            self.assertTrue(os.path.isfile(fqn))

    def test_doesnt_remove_current_repozo_files(self):
        FILENAMES = ['2009-12-20-10-08-03.fs',
                     '2009-12-20-10-08-03.dat',
                     '2009-12-20-10-08-03.index',
                     ]
        self._callFUT(filenames=FILENAMES)
        remaining = os.listdir(self._repository_directory)
        self.assertEqual(len(remaining), len(FILENAMES))
        for name in FILENAMES:
            fqn = os.path.join(self._repository_directory, name)
            self.assertTrue(os.path.isfile(fqn))

    def test_removes_older_repozo_files(self):
        OLDER_FULL = ['2009-12-20-00-01-03.fs',
                      '2009-12-20-00-01-03.dat',
                      '2009-12-20-00-01-03.index',
                      ]
        DELTAS = ['2009-12-21-00-00-01.deltafs',
                  '2009-12-21-00-00-01.index',
                  '2009-12-22-00-00-01.deltafs',
                  '2009-12-22-00-00-01.index',
                  ]
        CURRENT_FULL = ['2009-12-23-00-00-01.fs',
                        '2009-12-23-00-00-01.dat',
                        '2009-12-23-00-00-01.index',
                        ]
        FILENAMES = OLDER_FULL + DELTAS + CURRENT_FULL
        self._callFUT(filenames=FILENAMES)
        remaining = os.listdir(self._repository_directory)
        self.assertEqual(len(remaining), len(CURRENT_FULL))
        for name in OLDER_FULL:
            fqn = os.path.join(self._repository_directory, name)
            self.assertFalse(os.path.isfile(fqn))
        for name in DELTAS:
            fqn = os.path.join(self._repository_directory, name)
            self.assertFalse(os.path.isfile(fqn))
        for name in CURRENT_FULL:
            fqn = os.path.join(self._repository_directory, name)
            self.assertTrue(os.path.isfile(fqn))

    def test_removes_older_repozo_files_zipped(self):
        OLDER_FULL = ['2009-12-20-00-01-03.fsz',
                      '2009-12-20-00-01-03.dat',
                      '2009-12-20-00-01-03.index',
                      ]
        DELTAS = ['2009-12-21-00-00-01.deltafsz',
                  '2009-12-21-00-00-01.index',
                  '2009-12-22-00-00-01.deltafsz',
                  '2009-12-22-00-00-01.index',
                  ]
        CURRENT_FULL = ['2009-12-23-00-00-01.fsz',
                        '2009-12-23-00-00-01.dat',
                        '2009-12-23-00-00-01.index',
                        ]
        FILENAMES = OLDER_FULL + DELTAS + CURRENT_FULL
        self._callFUT(filenames=FILENAMES)
        remaining = os.listdir(self._repository_directory)
        self.assertEqual(len(remaining), len(CURRENT_FULL))
        for name in OLDER_FULL:
            fqn = os.path.join(self._repository_directory, name)
            self.assertFalse(os.path.isfile(fqn))
        for name in DELTAS:
            fqn = os.path.join(self._repository_directory, name)
            self.assertFalse(os.path.isfile(fqn))
        for name in CURRENT_FULL:
            fqn = os.path.join(self._repository_directory, name)
            self.assertTrue(os.path.isfile(fqn))


class Test_do_full_backup(OptionsTestBase, unittest.TestCase):

    def _callFUT(self, options):
        from ZODB.scripts.repozo import do_full_backup
        return do_full_backup(options)

    def _makeDB(self):
        import tempfile
        self._data_directory = tempfile.mkdtemp(prefix='zodb-test-')
        return OurDB(self._data_directory)

    def test_dont_overwrite_existing_file(self):
        from ZODB.scripts.repozo import WouldOverwriteFiles
        from ZODB.scripts.repozo import gen_filename
        db = self._makeDB()
        options = self._makeOptions(full=True,
                                    file=db._file_name,
                                    gzip=False,
                                    test_now=(2010, 5, 14, 10, 51, 22),
                                    )
        fqn = os.path.join(self._repository_directory, gen_filename(options))
        _write_file(fqn, b'TESTING')
        self.assertRaises(WouldOverwriteFiles, self._callFUT, options)

    def test_empty(self):
        import struct

        from ZODB.fsIndex import fsIndex
        from ZODB.scripts.repozo import gen_filename
        db = self._makeDB()
        options = self._makeOptions(file=db._file_name,
                                    gzip=False,
                                    killold=False,
                                    test_now=(2010, 5, 14, 10, 51, 22),
                                    )
        self._callFUT(options)
        target = os.path.join(self._repository_directory,
                              gen_filename(options))
        original = _read_file(db._file_name)
        self.assertEqual(_read_file(target), original)
        datfile = os.path.join(self._repository_directory,
                               gen_filename(options, '.dat'))
        self.assertEqual(_read_file(datfile, mode='r'),  # XXX 'rb'?
                         '%s 0 %d %s\n' %
                         (target, len(original), md5(original).hexdigest()))
        ndxfile = os.path.join(self._repository_directory,
                               gen_filename(options, '.index'))
        ndx_info = fsIndex.load(ndxfile)
        self.assertEqual(ndx_info['pos'], len(original))
        index = ndx_info['index']
        pZero = struct.pack(">Q", 0)
        pOne = struct.pack(">Q", 1)
        self.assertEqual(index.minKey(), pZero)
        self.assertEqual(index.maxKey(), pOne)


class Test_do_incremental_backup(OptionsTestBase, unittest.TestCase):

    def _callFUT(self, options, reposz, repofiles):
        from ZODB.scripts.repozo import do_incremental_backup
        return do_incremental_backup(options, reposz, repofiles)

    def _makeDB(self):
        import tempfile
        self._data_directory = tempfile.mkdtemp(prefix='zodb-test-')
        return OurDB(self._data_directory)

    def test_dont_overwrite_existing_file(self):
        from ZODB.scripts.repozo import WouldOverwriteFiles
        from ZODB.scripts.repozo import find_files
        from ZODB.scripts.repozo import gen_filename
        db = self._makeDB()
        options = self._makeOptions(full=False,
                                    file=db._file_name,
                                    gzip=False,
                                    test_now=(2010, 5, 14, 10, 51, 22),
                                    date=None,
                                    )
        fqn = os.path.join(self._repository_directory, gen_filename(options))
        _write_file(fqn, b'TESTING')
        repofiles = find_files(options)
        self.assertRaises(WouldOverwriteFiles,
                          self._callFUT, options, 0, repofiles)

    def test_no_changes(self):
        import struct

        from ZODB.fsIndex import fsIndex
        from ZODB.scripts.repozo import gen_filename
        db = self._makeDB()
        oldpos = db.pos
        options = self._makeOptions(file=db._file_name,
                                    gzip=False,
                                    killold=False,
                                    test_now=(2010, 5, 14, 10, 51, 22),
                                    date=None,
                                    )
        fullfile = os.path.join(self._repository_directory,
                                '2010-05-14-00-00-00.fs')
        original = _read_file(db._file_name)
        _write_file(fullfile, original)
        datfile = os.path.join(self._repository_directory,
                               '2010-05-14-00-00-00.dat')
        repofiles = [fullfile, datfile]
        self._callFUT(options, oldpos, repofiles)
        target = os.path.join(self._repository_directory,
                              gen_filename(options))
        self.assertEqual(_read_file(target), b'')
        self.assertEqual(_read_file(datfile, mode='r'),  # XXX mode='rb'?
                         '%s %d %d %s\n' %
                         (target, oldpos, oldpos, md5(b'').hexdigest()))
        ndxfile = os.path.join(self._repository_directory,
                               gen_filename(options, '.index'))
        ndx_info = fsIndex.load(ndxfile)
        self.assertEqual(ndx_info['pos'], oldpos)
        index = ndx_info['index']
        pZero = struct.pack(">Q", 0)
        pOne = struct.pack(">Q", 1)
        self.assertEqual(index.minKey(), pZero)
        self.assertEqual(index.maxKey(), pOne)

    def test_w_changes(self):
        import struct

        from ZODB.fsIndex import fsIndex
        from ZODB.scripts.repozo import gen_filename
        db = self._makeDB()
        oldpos = db.pos
        options = self._makeOptions(file=db._file_name,
                                    gzip=False,
                                    killold=False,
                                    test_now=(2010, 5, 14, 10, 51, 22),
                                    date=None,
                                    )
        fullfile = os.path.join(self._repository_directory,
                                '2010-05-14-00-00-00.fs')
        original = _read_file(db._file_name)
        f = _write_file(fullfile, original)
        datfile = os.path.join(self._repository_directory,
                               '2010-05-14-00-00-00.dat')
        repofiles = [fullfile, datfile]
        db.mutate()
        newpos = db.pos
        self._callFUT(options, oldpos, repofiles)
        target = os.path.join(self._repository_directory,
                              gen_filename(options))
        with open(db._file_name, 'rb') as f:
            f.seek(oldpos)
            increment = f.read()
        self.assertEqual(_read_file(target), increment)
        self.assertEqual(_read_file(datfile, mode='r'),  # XXX mode='rb'?
                         '%s %d %d %s\n' %
                         (target, oldpos, newpos,
                             md5(increment).hexdigest()))
        ndxfile = os.path.join(self._repository_directory,
                               gen_filename(options, '.index'))
        ndx_info = fsIndex.load(ndxfile)
        self.assertEqual(ndx_info['pos'], newpos)
        index = ndx_info['index']
        pZero = struct.pack(">Q", 0)
        self.assertEqual(index.minKey(), pZero)
        self.assertEqual(index.maxKey(), db.maxkey)


class Test_do_recover(OptionsTestBase, unittest.TestCase):

    def _callFUT(self, options):
        from ZODB.scripts.repozo import do_recover
        return do_recover(options)

    def _makeFile(self, hour, min, sec, ext, text=None):
        # call _makeOptions first!
        name = '2010-05-14-%02d-%02d-%02d%s' % (hour, min, sec, ext)
        if text is None:
            text = name
        fqn = os.path.join(self._repository_directory, name)
        _write_file(fqn, text.encode())
        return fqn

    def test_no_files(self):
        from ZODB.scripts.repozo import NoFiles
        options = self._makeOptions(date=None,
                                    test_now=(2010, 5, 15, 13, 30, 57))
        self.assertRaises(NoFiles, self._callFUT, options)

    def test_no_files_before_explicit_date(self):
        from ZODB.scripts.repozo import NoFiles
        options = self._makeOptions(date='2010-05-13-13-30-57')
        files = []
        for h, m, s, e in [(2, 13, 14, '.fs'),
                           (2, 13, 14, '.dat'),
                           (3, 14, 15, '.deltafs'),
                           (4, 14, 15, '.deltafs'),
                           (5, 14, 15, '.deltafs'),
                           (12, 13, 14, '.fs'),
                           (12, 13, 14, '.dat'),
                           (13, 14, 15, '.deltafs'),
                           (14, 15, 16, '.deltafs'),
                           ]:
            files.append(self._makeFile(h, m, s, e))
        self.assertRaises(NoFiles, self._callFUT, options)

    def test_w_full_backup_latest_no_index(self):
        import tempfile
        dd = self._data_directory = tempfile.mkdtemp(prefix='zodb-test-')
        output = os.path.join(dd, 'Data.fs')
        options = self._makeOptions(date='2010-05-15-13-30-57',
                                    output=output,
                                    withverify=False)
        self._makeFile(2, 3, 4, '.fs', 'AAA')
        self._makeFile(4, 5, 6, '.fs', 'BBB')
        self._callFUT(options)
        self.assertEqual(_read_file(output), b'BBB')

    def test_w_full_backup_latest_index(self):
        import tempfile
        dd = self._data_directory = tempfile.mkdtemp(prefix='zodb-test-')
        output = os.path.join(dd, 'Data.fs')
        index = os.path.join(dd, 'Data.fs.index')
        options = self._makeOptions(date='2010-05-15-13-30-57',
                                    output=output,
                                    withverify=False)
        self._makeFile(2, 3, 4, '.fs', 'AAA')
        self._makeFile(4, 5, 6, '.fs', 'BBB')
        self._makeFile(4, 5, 6, '.index', 'CCC')
        self._callFUT(options)
        self.assertEqual(_read_file(output), b'BBB')
        self.assertEqual(_read_file(index), b'CCC')

    def test_w_incr_backup_latest_no_index(self):
        import tempfile
        dd = self._data_directory = tempfile.mkdtemp(prefix='zodb-test-')
        output = os.path.join(dd, 'Data.fs')
        options = self._makeOptions(date='2010-05-15-13-30-57',
                                    output=output,
                                    withverify=False)
        self._makeFile(2, 3, 4, '.fs', 'AAA')
        self._makeFile(4, 5, 6, '.deltafs', 'BBB')
        self._callFUT(options)
        self.assertEqual(_read_file(output), b'AAABBB')

    def test_w_incr_backup_latest_index(self):
        import tempfile
        dd = self._data_directory = tempfile.mkdtemp(prefix='zodb-test-')
        output = os.path.join(dd, 'Data.fs')
        index = os.path.join(dd, 'Data.fs.index')
        options = self._makeOptions(date='2010-05-15-13-30-57',
                                    output=output,
                                    withverify=False)
        self._makeFile(2, 3, 4, '.fs', 'AAA')
        self._makeFile(4, 5, 6, '.deltafs', 'BBB')
        self._makeFile(4, 5, 6, '.index', 'CCC')
        self._callFUT(options)
        self.assertEqual(_read_file(output), b'AAABBB')
        self.assertEqual(_read_file(index), b'CCC')

    def test_w_incr_backup_with_verify_all_is_fine(self):
        import tempfile
        dd = self._data_directory = tempfile.mkdtemp(prefix='zodb-test-')
        output = os.path.join(dd, 'Data.fs')
        options = self._makeOptions(date='2010-05-15-13-30-57',
                                    output=output,
                                    withverify=True)
        self._makeFile(2, 3, 4, '.fs', 'AAA')
        self._makeFile(4, 5, 6, '.deltafs', 'BBBB')
        self._makeFile(
            2, 3, 4, '.dat',
            '/backup/2010-05-14-02-03-04.fs 0 3 e1faffb3e614e6c2fba74296962386b7\n'  # noqa: E501 line too long
            '/backup/2010-05-14-04-05-06.deltafs 3 7 f50881ced34c7d9e6bce100bf33dec60\n')  # noqa: E501 line too long
        self._callFUT(options)
        self.assertFalse(os.path.exists(output + '.part'))
        self.assertEqual(_read_file(output), b'AAABBBB')

    def test_w_incr_backup_with_verify_sum_inconsistent(self):
        import tempfile

        from ZODB.scripts.repozo import VerificationFail
        dd = self._data_directory = tempfile.mkdtemp(prefix='zodb-test-')
        output = os.path.join(dd, 'Data.fs')
        options = self._makeOptions(date='2010-05-15-13-30-57',
                                    output=output,
                                    withverify=True)
        self._makeFile(2, 3, 4, '.fs', 'AAA')
        self._makeFile(4, 5, 6, '.deltafs', 'BBBB')
        self._makeFile(
            2, 3, 4, '.dat',
            '/backup/2010-05-14-02-03-04.fs 0 3 e1faffb3e614e6c2fba74296962386b7\n'  # noqa: E501 line too long
            '/backup/2010-05-14-04-05-06.deltafs 3 7 f50881ced34c7d9e6bce100bf33dec61\n')  # noqa: E501 line too long
        self.assertRaises(VerificationFail, self._callFUT, options)
        self.assertTrue(os.path.exists(output + '.part'))

    def test_w_incr_backup_with_verify_size_inconsistent(self):
        import tempfile

        from ZODB.scripts.repozo import VerificationFail
        dd = self._data_directory = tempfile.mkdtemp(prefix='zodb-test-')
        output = os.path.join(dd, 'Data.fs')
        options = self._makeOptions(date='2010-05-15-13-30-57',
                                    output=output,
                                    withverify=True)
        self._makeFile(2, 3, 4, '.fs', 'AAA')
        self._makeFile(4, 5, 6, '.deltafs', 'BBBB')
        self._makeFile(
            2, 3, 4, '.dat',
            '/backup/2010-05-14-02-03-04.fs 0 3 e1faffb3e614e6c2fba74296962386b7\n'  # noqa: E501 line too long
            '/backup/2010-05-14-04-05-06.deltafs 3 8 f50881ced34c7d9e6bce100bf33dec60\n')  # noqa: E501 line too long
        self.assertRaises(VerificationFail, self._callFUT, options)
        self.assertTrue(os.path.exists(output + '.part'))


class Test_do_verify(OptionsTestBase, unittest.TestCase):

    def _callFUT(self, options):
        from ZODB.scripts import repozo
        errors = []
        orig_error = repozo.error

        def _error(msg, *args):
            errors.append(msg % args)
        repozo.error = _error
        try:
            repozo.do_verify(options)
            return errors
        finally:
            repozo.error = orig_error

    def _makeFile(self, hour, min, sec, ext, text=None):
        from ZODB.scripts.repozo import _GzipCloser
        assert self._repository_directory, 'call _makeOptions first!'
        name = '2010-05-14-%02d-%02d-%02d%s' % (hour, min, sec, ext)
        if text is None:
            text = name
        fqn = os.path.join(self._repository_directory, name)
        if ext.endswith('fsz'):
            _opener = _GzipCloser
        else:
            _opener = open
        with _opener(fqn, 'wb') as f:
            f.write(text.encode())
            f.flush()
        return fqn

    def test_no_files(self):
        from ZODB.scripts.repozo import NoFiles
        options = self._makeOptions()
        self.assertRaises(NoFiles, self._callFUT, options)

    def test_all_is_fine(self):
        options = self._makeOptions(quick=False)
        self._makeFile(2, 3, 4, '.fs', 'AAA')
        self._makeFile(4, 5, 6, '.deltafs', 'BBBB')
        self._makeFile(
            2, 3, 4, '.dat',
            '/backup/2010-05-14-02-03-04.fs 0 3 e1faffb3e614e6c2fba74296962386b7\n'  # noqa: E501 line too long
            '/backup/2010-05-14-04-05-06.deltafs 3 7 f50881ced34c7d9e6bce100bf33dec60\n')  # noqa: E501 line too long
        self.assertEqual(self._callFUT(options), [])

    def test_all_is_fine_gzip(self):
        options = self._makeOptions(quick=False)
        self._makeFile(2, 3, 4, '.fsz', 'AAA')
        self._makeFile(4, 5, 6, '.deltafsz', 'BBBB')
        self._makeFile(
            2, 3, 4, '.dat',
            '/backup/2010-05-14-02-03-04.fsz 0 3 e1faffb3e614e6c2fba74296962386b7\n'  # noqa: E501 line too long
            '/backup/2010-05-14-04-05-06.deltafsz 3 7 f50881ced34c7d9e6bce100bf33dec60\n')  # noqa: E501 line too long
        self.assertEqual(self._callFUT(options), [])

    def test_missing_file(self):
        options = self._makeOptions(quick=True)
        self._makeFile(2, 3, 4, '.fs', 'AAA')
        self._makeFile(
            2, 3, 4, '.dat',
            '/backup/2010-05-14-02-03-04.fs 0 3 e1faffb3e614e6c2fba74296962386b7\n'  # noqa: E501 line too long
            '/backup/2010-05-14-04-05-06.deltafs 3 7 f50881ced34c7d9e6bce100bf33dec60\n')  # noqa: E501 line too long
        self.assertEqual(self._callFUT(options),
                         [options.repository + os.path.sep +
                          '2010-05-14-04-05-06.deltafs is missing'])

    def test_missing_file_gzip(self):
        options = self._makeOptions(quick=True)
        self._makeFile(2, 3, 4, '.fsz', 'AAA')
        self._makeFile(
            2, 3, 4, '.dat',
            '/backup/2010-05-14-02-03-04.fsz 0 3 e1faffb3e614e6c2fba74296962386b7\n'  # noqa: E501 line too long
            '/backup/2010-05-14-04-05-06.deltafsz 3 7 f50881ced34c7d9e6bce100bf33dec60\n')  # noqa: E501 line too long
        self.assertEqual(self._callFUT(options),
                         [options.repository + os.path.sep +
                          '2010-05-14-04-05-06.deltafsz is missing'])

    def test_bad_size(self):
        options = self._makeOptions(quick=False)
        self._makeFile(2, 3, 4, '.fs', 'AAA')
        self._makeFile(4, 5, 6, '.deltafs', 'BBB')
        self._makeFile(
            2, 3, 4, '.dat',
            '/backup/2010-05-14-02-03-04.fs 0 3 e1faffb3e614e6c2fba74296962386b7\n'  # noqa: E501 line too long
            '/backup/2010-05-14-04-05-06.deltafs 3 7 f50881ced34c7d9e6bce100bf33dec60\n')  # noqa: E501 line too long
        self.assertEqual(self._callFUT(options),
                         [options.repository + os.path.sep +
                          '2010-05-14-04-05-06.deltafs is 3 bytes,'
                          ' should be 4 bytes'])

    def test_bad_size_gzip(self):
        options = self._makeOptions(quick=False)
        self._makeFile(2, 3, 4, '.fsz', 'AAA')
        self._makeFile(4, 5, 6, '.deltafsz', 'BBB')
        self._makeFile(
            2, 3, 4, '.dat',
            '/backup/2010-05-14-02-03-04.fsz 0 3 e1faffb3e614e6c2fba74296962386b7\n'   # noqa: E501 line too long
            '/backup/2010-05-14-04-05-06.deltafsz 3 7 f50881ced34c7d9e6bce100bf33dec60\n')  # noqa: E501 line too long
        self.assertEqual(
            self._callFUT(options),
            [options.repository + os.path.sep +
             '2010-05-14-04-05-06.deltafsz is 3 bytes (when uncompressed),'
             ' should be 4 bytes'])

    def test_bad_checksum(self):
        options = self._makeOptions(quick=False)
        self._makeFile(2, 3, 4, '.fs', 'AAA')
        self._makeFile(4, 5, 6, '.deltafs', 'BbBB')
        self._makeFile(
            2, 3, 4, '.dat',
            '/backup/2010-05-14-02-03-04.fs 0 3 e1faffb3e614e6c2fba74296962386b7\n'  # noqa: E501 line too long
            '/backup/2010-05-14-04-05-06.deltafs 3 7 f50881ced34c7d9e6bce100bf33dec60\n')  # noqa: E501 line too long
        self.assertEqual(self._callFUT(options),
                         [options.repository + os.path.sep +
                          '2010-05-14-04-05-06.deltafs has checksum'
                          ' 36486440db255f0ee6ab109d5d231406 instead of'
                          ' f50881ced34c7d9e6bce100bf33dec60'])

    def test_bad_checksum_gzip(self):
        options = self._makeOptions(quick=False)
        self._makeFile(2, 3, 4, '.fsz', 'AAA')
        self._makeFile(4, 5, 6, '.deltafsz', 'BbBB')
        self._makeFile(
            2, 3, 4, '.dat',
            '/backup/2010-05-14-02-03-04.fsz 0 3 e1faffb3e614e6c2fba74296962386b7\n'  # noqa: E501 line too long
            '/backup/2010-05-14-04-05-06.deltafsz 3 7 f50881ced34c7d9e6bce100bf33dec60\n')  # noqa: E501 line too long
        self.assertEqual(
            self._callFUT(options),
            [options.repository + os.path.sep +
             '2010-05-14-04-05-06.deltafsz has checksum'
             ' 36486440db255f0ee6ab109d5d231406 (when uncompressed) instead of'
             ' f50881ced34c7d9e6bce100bf33dec60'])

    def test_quick_ignores_checksums(self):
        options = self._makeOptions(quick=True)
        self._makeFile(2, 3, 4, '.fs', 'AAA')
        self._makeFile(4, 5, 6, '.deltafs', 'BBBB')
        self._makeFile(
                2, 3, 4, '.dat',
                '/backup/2010-05-14-02-03-04.fs 0 3 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n'  # noqa: E501 line too long
                '/backup/2010-05-14-04-05-06.deltafs 3 7 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n')  # noqa: E501 line too long
        self.assertEqual(self._callFUT(options), [])

    def test_quick_ignores_checksums_gzip(self):
        options = self._makeOptions(quick=True)
        self._makeFile(2, 3, 4, '.fsz', 'AAA')
        self._makeFile(4, 5, 6, '.deltafsz', 'BBBB')
        self._makeFile(
            2, 3, 4, '.dat',
            '/backup/2010-05-14-02-03-04.fsz 0 3 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n'  # noqa: E501 line too long
            '/backup/2010-05-14-04-05-06.deltafsz 3 7 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\n')  # noqa: E501 line too long
        self.assertEqual(self._callFUT(options), [])


class MonteCarloTests(unittest.TestCase):

    layer = ZODB.tests.util.MininalTestLayer('repozo')

    def setUp(self):
        # compute directory names
        import tempfile
        self.basedir = tempfile.mkdtemp(prefix='zodb-test-')
        self.backupdir = os.path.join(self.basedir, 'backup')
        self.datadir = os.path.join(self.basedir, 'data')
        self.restoredir = os.path.join(self.basedir, 'restore')
        self.copydir = os.path.join(self.basedir, 'copy')
        self.currdir = os.getcwd()
        # create empty directories
        os.mkdir(self.backupdir)
        os.mkdir(self.datadir)
        os.mkdir(self.restoredir)
        os.mkdir(self.copydir)
        os.chdir(self.datadir)
        self.db = OurDB(self.datadir)

    def tearDown(self):
        os.chdir(self.currdir)
        import shutil
        shutil.rmtree(self.basedir)

    def _callRepozoMain(self, argv):
        from ZODB.scripts.repozo import main
        main(argv)

    @ZODB.tests.util.time_monotonically_increases
    def test_via_monte_carlo(self):
        self.saved_snapshots = []  # list of (name, time) pairs for copies.

        for i in range(100):
            self.mutate_pack_backup(i)

        # Verify snapshots can be reproduced exactly.
        for copyname, copytime in self.saved_snapshots:
            if _NOISY:
                print("Checking that", copyname, end=' ')
                print("at", copytime, "is reproducible.")
            self.assertRestored(copyname, copytime)

    def mutate_pack_backup(self, i):
        import random
        from shutil import copyfile
        from time import gmtime
        self.db.mutate()

        # Pack about each tenth time.
        if random.random() < 0.1:
            if _NOISY:
                print("packing")
            self.db.pack()
            self.db.close()

        # Make an incremental backup, half the time with gzip (-z).
        argv = ['-BQr', self.backupdir, '-f', 'Data.fs']
        if _NOISY:
            argv.insert(0, '-v')
        if random.random() < 0.5:
            argv.insert(0, '-z')
        self._callRepozoMain(argv)

        # Save snapshots to assert that dated restores are possible
        if i % 9 == 0:
            srcname = os.path.join(self.datadir, 'Data.fs')
            copytime = '%04d-%02d-%02d-%02d-%02d-%02d' % (gmtime()[:6])
            copyname = os.path.join(self.copydir, "Data%d.fs" % i)
            copyfile(srcname, copyname)
            self.saved_snapshots.append((copyname, copytime))

        # The clock moves forward automatically on calls to time.time()

        # Verify current Data.fs can be reproduced exactly.
        self.assertRestored()

    def assertRestored(self, correctpath='Data.fs', when=None):
        # Do recovery to time 'when', and check that it's identical to
        # correctpath.
        # restore to Restored.fs
        restoredfile = os.path.join(self.restoredir, 'Restored.fs')
        argv = ['-Rr', self.backupdir, '-o', restoredfile]
        if _NOISY:
            argv.insert(0, '-v')
        if when is not None:
            argv.append('-D')
            argv.append(when)
        self._callRepozoMain(argv)

        # check restored file content is equal to file that was backed up
        fguts = _read_file(correctpath)
        gguts = _read_file(restoredfile)
        msg = ("guts don't match\ncorrectpath=%r when=%r\n cmd=%r" %
               (correctpath, when, ' '.join(argv)))
        self.assertEqual(fguts, gguts, msg)


def test_suite():
    return unittest.TestSuite([
        unittest.makeSuite(Test_parseargs),
        unittest.makeSuite(Test_dofile),
        unittest.makeSuite(Test_checksum),
        unittest.makeSuite(Test_copyfile),
        unittest.makeSuite(Test_concat),
        unittest.makeSuite(Test_gen_filename),
        unittest.makeSuite(Test_find_files),
        unittest.makeSuite(Test_scandat),
        unittest.makeSuite(Test_delete_old_backups),
        unittest.makeSuite(Test_do_full_backup),
        unittest.makeSuite(Test_do_incremental_backup),
        # unittest.makeSuite(Test_do_backup),  #TODO
        unittest.makeSuite(Test_do_recover),
        unittest.makeSuite(Test_do_verify),
        # N.B.:  this test take forever to run (~40sec on a fast laptop),
        # *and* it is non-deterministic.
        unittest.makeSuite(MonteCarloTests),
    ])
