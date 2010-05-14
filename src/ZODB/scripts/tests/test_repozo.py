##############################################################################
#
# Copyright (c) 2004-2009 Zope Corporation and Contributors.
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
import unittest
import os
try:
    # the hashlib package is available from Python 2.5
    from hashlib import md5
except ImportError:
    # the md5 package is deprecated in Python 2.6
    from md5 import new as md5

import ZODB.tests.util  # layer used at class scope

_NOISY = os.environ.get('NOISY_REPOZO_TEST_OUTPUT')

class OurDB:

    _file_name = None

    def __init__(self, dir):
        from BTrees.OOBTree import OOBTree
        import transaction
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
        self.close()


class Test_dofile(unittest.TestCase):

    def _callFUT(self, func, fp, n):
        from ZODB.scripts.repozo import dofile
        return dofile(func, fp, n)

    def _makeChunks(self):
        from ZODB.scripts.repozo import READCHUNK
        return ['x' * READCHUNK, 'y' * READCHUNK, 'z']

    def _makeFile(self, text=None):
        from StringIO import StringIO
        if text is None:
            text = ''.join(self._makeChunks())
        return StringIO(text)

    def test_empty_read_all(self):
        chunks = []
        file = self._makeFile('')
        bytes = self._callFUT(chunks.append, file, None)
        self.assertEqual(bytes, 0)
        self.assertEqual(chunks, [])

    def test_empty_read_count(self):
        chunks = []
        file = self._makeFile('')
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
        self.assertEqual(chunks, ['x' * 42])

class TestBase:

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
        self._repository_directory = tempfile.mkdtemp()
        class Options(object):
            repository = self._repository_directory
            def __init__(self, **kw):
                self.__dict__.update(kw)
        return Options(**kw)


class Test_delete_old_backups(TestBase, unittest.TestCase):

    def _makeOptions(self, filenames=()):
        options = super(Test_delete_old_backups, self)._makeOptions()
        for filename in filenames:
            fqn = os.path.join(options.repository, filename)
            f = open(fqn, 'wb')
            f.write('testing delete_old_backups')
            f.close()
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
            self.failUnless(os.path.isfile(fqn))

    def test_doesnt_remove_current_repozo_files(self):
        FILENAMES = ['2009-12-20-10-08-03.fs', '2009-12-20-10-08-03.dat']
        self._callFUT(filenames=FILENAMES)
        remaining = os.listdir(self._repository_directory)
        self.assertEqual(len(remaining), len(FILENAMES))
        for name in FILENAMES:
            fqn = os.path.join(self._repository_directory, name)
            self.failUnless(os.path.isfile(fqn))

    def test_removes_older_repozo_files(self):
        OLDER_FULL = ['2009-12-20-00-01-03.fs', '2009-12-20-00-01-03.dat']
        DELTAS = ['2009-12-21-00-00-01.deltafs', '2009-12-22-00-00-01.deltafs']
        CURRENT_FULL = ['2009-12-23-00-00-01.fs', '2009-12-23-00-00-01.dat']
        FILENAMES = OLDER_FULL + DELTAS + CURRENT_FULL
        self._callFUT(filenames=FILENAMES)
        remaining = os.listdir(self._repository_directory)
        self.assertEqual(len(remaining), len(CURRENT_FULL))
        for name in OLDER_FULL:
            fqn = os.path.join(self._repository_directory, name)
            self.failIf(os.path.isfile(fqn))
        for name in DELTAS:
            fqn = os.path.join(self._repository_directory, name)
            self.failIf(os.path.isfile(fqn))
        for name in CURRENT_FULL:
            fqn = os.path.join(self._repository_directory, name)
            self.failUnless(os.path.isfile(fqn))

    def test_removes_older_repozo_files_zipped(self):
        OLDER_FULL = ['2009-12-20-00-01-03.fsz', '2009-12-20-00-01-03.dat']
        DELTAS = ['2009-12-21-00-00-01.deltafsz',
                  '2009-12-22-00-00-01.deltafsz']
        CURRENT_FULL = ['2009-12-23-00-00-01.fsz', '2009-12-23-00-00-01.dat']
        FILENAMES = OLDER_FULL + DELTAS + CURRENT_FULL
        self._callFUT(filenames=FILENAMES)
        remaining = os.listdir(self._repository_directory)
        self.assertEqual(len(remaining), len(CURRENT_FULL))
        for name in OLDER_FULL:
            fqn = os.path.join(self._repository_directory, name)
            self.failIf(os.path.isfile(fqn))
        for name in DELTAS:
            fqn = os.path.join(self._repository_directory, name)
            self.failIf(os.path.isfile(fqn))
        for name in CURRENT_FULL:
            fqn = os.path.join(self._repository_directory, name)
            self.failUnless(os.path.isfile(fqn))


class Test_do_full_backup(TestBase, unittest.TestCase):

    def _callFUT(self, options):
        from ZODB.scripts.repozo import do_full_backup
        return do_full_backup(options)

    def _makeDB(self):
        import tempfile
        datadir = self._data_directory = tempfile.mkdtemp()
        return OurDB(self._data_directory)

    def test_dont_overwrite_existing_file(self):
        from ZODB.scripts.repozo import WouldOverwriteFiles
        from ZODB.scripts.repozo import gen_filename
        db = self._makeDB()
        options = self._makeOptions(full=True,
                                    file=db._file_name,
                                    gzip=False,
                                    test_now = (2010, 5, 14, 10, 51, 22),
                                   )
        f = open(os.path.join(self._repository_directory,
                              gen_filename(options)), 'w')
        f.write('TESTING')
        f.flush()
        f.close()
        self.assertRaises(WouldOverwriteFiles, self._callFUT, options)

    def test_empty(self):
        from ZODB.scripts.repozo import gen_filename
        db = self._makeDB()
        options = self._makeOptions(file=db._file_name,
                                    gzip=False,
                                    killold=False,
                                    test_now = (2010, 5, 14, 10, 51, 22),
                                   )
        self._callFUT(options)
        target = os.path.join(self._repository_directory,
                              gen_filename(options))
        original = open(db._file_name, 'rb').read()
        self.assertEqual(open(target, 'rb').read(), original)
        datfile = os.path.join(self._repository_directory,
                               gen_filename(options, '.dat'))
        self.assertEqual(open(datfile).read(),
                         '%s 0 %d %s\n' %
                            (target, len(original), md5(original).hexdigest()))


class Test_do_incremental_backup(TestBase, unittest.TestCase):

    def _callFUT(self, options, reposz, repofiles):
        from ZODB.scripts.repozo import do_incremental_backup
        return do_incremental_backup(options, reposz, repofiles)

    def _makeDB(self):
        import tempfile
        datadir = self._data_directory = tempfile.mkdtemp()
        return OurDB(self._data_directory)

    def test_dont_overwrite_existing_file(self):
        from ZODB.scripts.repozo import WouldOverwriteFiles
        from ZODB.scripts.repozo import gen_filename
        from ZODB.scripts.repozo import find_files
        db = self._makeDB()
        options = self._makeOptions(full=False,
                                    file=db._file_name,
                                    gzip=False,
                                    test_now = (2010, 5, 14, 10, 51, 22),
                                    date = None,
                                   )
        f = open(os.path.join(self._repository_directory,
                              gen_filename(options)), 'w')
        f.write('TESTING')
        f.flush()
        f.close()
        repofiles = find_files(options)
        self.assertRaises(WouldOverwriteFiles,
                          self._callFUT, options, 0, repofiles)

    def test_no_changes(self):
        from ZODB.scripts.repozo import gen_filename
        db = self._makeDB()
        oldpos = db.pos
        options = self._makeOptions(file=db._file_name,
                                    gzip=False,
                                    killold=False,
                                    test_now = (2010, 5, 14, 10, 51, 22),
                                    date = None,
                                   )
        fullfile = os.path.join(self._repository_directory,
                                '2010-05-14-00-00-00.fs')
        original = open(db._file_name, 'rb').read()
        last = len(original)
        f = open(fullfile, 'wb')
        f.write(original)
        f.flush()
        f.close()
        datfile = os.path.join(self._repository_directory,
                                '2010-05-14-00-00-00.dat')
        repofiles = [fullfile, datfile]
        self._callFUT(options, oldpos, repofiles)
        target = os.path.join(self._repository_directory,
                              gen_filename(options))
        self.assertEqual(open(target, 'rb').read(), '')
        self.assertEqual(open(datfile).read(),
                         '%s %d %d %s\n' %
                            (target, oldpos, oldpos, md5('').hexdigest()))

    def test_w_changes(self):
        from ZODB.scripts.repozo import gen_filename
        db = self._makeDB()
        oldpos = db.pos
        options = self._makeOptions(file=db._file_name,
                                    gzip=False,
                                    killold=False,
                                    test_now = (2010, 5, 14, 10, 51, 22),
                                    date = None,
                                   )
        fullfile = os.path.join(self._repository_directory,
                                '2010-05-14-00-00-00.fs')
        original = open(db._file_name, 'rb').read()
        f = open(fullfile, 'wb')
        f.write(original)
        f.flush()
        f.close()
        datfile = os.path.join(self._repository_directory,
                                '2010-05-14-00-00-00.dat')
        repofiles = [fullfile, datfile]
        db.mutate()
        newpos = db.pos
        self._callFUT(options, oldpos, repofiles)
        target = os.path.join(self._repository_directory,
                              gen_filename(options))
        f = open(db._file_name, 'rb')
        f.seek(oldpos)
        increment = f.read()
        self.assertEqual(open(target, 'rb').read(), increment)
        self.assertEqual(open(datfile).read(),
                         '%s %d %d %s\n' %
                            (target, oldpos, newpos,
                             md5(increment).hexdigest()))


class MonteCarloTests(unittest.TestCase):

    layer = ZODB.tests.util.MininalTestLayer('repozo')

    def setUp(self):
        # compute directory names
        import tempfile
        self.basedir = tempfile.mkdtemp()
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

    def test_via_monte_carlo(self):
        self.saved_snapshots = []  # list of (name, time) pairs for copies.

        for i in range(100):
            self.mutate_pack_backup(i)

        # Verify snapshots can be reproduced exactly.
        for copyname, copytime in self.saved_snapshots:
            if _NOISY:
                print "Checking that", copyname,
                print "at", copytime, "is reproducible."
            self.assertRestored(copyname, copytime)

    def mutate_pack_backup(self, i):
        import random
        from shutil import copyfile
        from time import gmtime
        from time import sleep
        self.db.mutate()

        # Pack about each tenth time.
        if random.random() < 0.1:
            if _NOISY:
                print "packing"
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

        # Make sure the clock moves at least a second.
        sleep(1.01)

        # Verify current Data.fs can be reproduced exactly.
        self.assertRestored()

    def assertRestored(self, correctpath='Data.fs', when=None):
    # Do recovery to time 'when', and check that it's identical to correctpath.
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
        f = file(correctpath, 'rb')
        g = file(restoredfile, 'rb')
        fguts = f.read()
        gguts = g.read()
        f.close()
        g.close()
        msg = ("guts don't match\ncorrectpath=%r when=%r\n cmd=%r" %
            (correctpath, when, ' '.join(argv)))
        self.assertEquals(fguts, gguts, msg)


def test_suite():
    return unittest.TestSuite([
        unittest.makeSuite(Test_dofile),
        unittest.makeSuite(Test_delete_old_backups),
        unittest.makeSuite(Test_do_full_backup),
        unittest.makeSuite(Test_do_incremental_backup),
        unittest.makeSuite(MonteCarloTests),
    ])
