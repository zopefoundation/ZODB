##############################################################################
#
# Copyright (c) 2005-2006 Zope Corporation and Contributors.
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
"""Blobs
"""

import base64
import logging
import os
import shutil
import sys
import time
import tempfile
import logging

import zope.interface

import ZODB.interfaces
from ZODB.interfaces import BlobError
from ZODB import utils
from ZODB.POSException import POSKeyError
import transaction
import transaction.interfaces
import persistent

from zope.proxy import getProxiedObject, non_overridable
from zope.proxy.decorator import SpecificationDecoratorBase

logger = logging.getLogger('ZODB.blob')

BLOB_SUFFIX = ".blob"

valid_modes = 'r', 'w', 'r+', 'a'

class Blob(persistent.Persistent):
    """A BLOB supports efficient handling of large data within ZODB."""

    zope.interface.implements(ZODB.interfaces.IBlob)

    _os_link = os.rename

    _p_blob_readers = 0
    _p_blob_writers = 0
    _p_blob_uncommitted = None  # Filename of the uncommitted (dirty) data
    _p_blob_data = None         # Filename of the committed data

    # All persistent object store a reference to their data manager, a database
    # connection in the _p_jar attribute. So we are going to do the same with
    # blobs here.
    _p_blob_manager = None

    # Blobs need to participate in transactions even when not connected to
    # a database yet. If you want to use a non-default transaction manager,
    # you can override it via _p_blob_transaction. This is currently
    # required for unit testing.
    _p_blob_transaction = None

    def open(self, mode="r"):
        """Returns a file(-like) object representing blob data."""
        result = None
            
        if mode not in valid_modes:
            raise ValueError("invalid mode", mode)

        if mode == 'r':
            if self._current_filename() is None:
                raise BlobError("Blob does not exist.")

            if self._p_blob_writers != 0:
                raise BlobError("Already opened for writing.")

            self._p_blob_readers += 1
            result = BlobFile(self._current_filename(), mode, self)

        elif mode == 'w':
            if self._p_blob_readers != 0:
                raise BlobError("Already opened for reading.")

            self._p_blob_writers += 1
            if self._p_blob_uncommitted is None:
                self._create_uncommitted_file()
            result = BlobFile(self._p_blob_uncommitted, mode, self)

        elif mode in ('a', 'r+'):
            if self._p_blob_readers != 0:
                raise BlobError("Already opened for reading.")

            if self._p_blob_uncommitted is None:
                # Create a new working copy
                uncommitted = BlobFile(self._create_uncommitted_file(),
                                       mode, self)
                # NOTE: _p_blob data appears by virtue of Connection._setstate
                utils.cp(file(self._p_blob_data), uncommitted)
                uncommitted.seek(0)
            else:
                # Re-use existing working copy
                uncommitted = BlobFile(self._p_blob_uncommitted, mode, self)

            self._p_blob_writers += 1
            result = uncommitted

        else:
            raise IOError('invalid mode: %s ' % mode)

        if result is not None:
            self._setup_transaction_manager(result)
        return result

    def openDetached(self, class_=file):
        """Returns a file(-like) object in read mode that can be used
        outside of transaction boundaries.

        """
        if self._current_filename() is None:
            raise BlobError("Blob does not exist.")
        if self._p_blob_writers != 0:
            raise BlobError("Already opened for writing.")
        # XXX this should increase the reader number and have a test !?!
        return class_(self._current_filename(), "rb")

    def consumeFile(self, filename):
        """Will replace the current data of the blob with the file given under
        filename.
        """
        if self._p_blob_writers != 0:
            raise BlobError("Already opened for writing.")
        if self._p_blob_readers != 0:
            raise BlobError("Already opened for reading.")

        previous_uncommitted = bool(self._p_blob_uncommitted)
        if previous_uncommitted:
            # If we have uncommitted data, we move it aside for now
            # in case the consumption doesn't work.
            target = self._p_blob_uncommitted
            target_aside = target+".aside"
            os.rename(target, target_aside)
        else:
            target = self._create_uncommitted_file()
            # We need to unlink the freshly created target again
            # to allow link() to do its job
            os.unlink(target)

        try:
            self._os_link(filename, target)
        except:
            # Recover from the failed consumption: First remove the file, it
            # might exist and mark the pointer to the uncommitted file.
            self._p_blob_uncommitted = None
            if os.path.exists(target):
                os.unlink(target)

            # If there was a file moved aside, bring it back including the
            # pointer to the uncommitted file.
            if previous_uncommitted:
                os.rename(target_aside, target)
                self._p_blob_uncommitted = target

            # Re-raise the exception to make the application aware of it.
            raise
        else:
            if previous_uncommitted:
                # The relinking worked so we can remove the data that we had 
                # set aside.
                os.unlink(target_aside)

            # We changed the blob state and have to make sure we join the
            # transaction.
            self._change()

    # utility methods

    def _current_filename(self):
        # NOTE: _p_blob_data and _p_blob_uncommitted appear by virtue of
        # Connection._setstate
        return self._p_blob_uncommitted or self._p_blob_data

    def _create_uncommitted_file(self):
        assert self._p_blob_uncommitted is None, (
            "Uncommitted file already exists.")
        tempdir = os.environ.get('ZODB_BLOB_TEMPDIR', tempfile.gettempdir())
        self._p_blob_uncommitted = utils.mktemp(dir=tempdir)
        return self._p_blob_uncommitted

    def _change(self):
        self._p_changed = 1

    def _setup_transaction_manager(self, result):
        # We join the transaction with our own data manager in order to be
        # notified of commit/vote/abort events.  We do this because at
        # transaction boundaries, we need to fix up _p_ reference counts
        # that keep track of open readers and writers and close any
        # writable filehandles we've opened.
        if self._p_blob_manager is None:
            # Blobs need to always participate in transactions.
            if self._p_jar is not None:
                # If we are connected to a database, then we use the
                # transaction manager that belongs to this connection
                tm = self._p_jar.transaction_manager
            else:
                # If we are not connected to a database, we check whether
                # we have been given an explicit transaction manager
                if self._p_blob_transaction:
                    tm = self._p_blob_transaction
                else:
                    # Otherwise we use the default
                    # transaction manager as an educated guess.
                    tm = transaction.manager
            # Create our datamanager and join he current transaction.
            dm = BlobDataManager(self, result, tm)
            tm.get().join(dm)
        elif result:
            # Each blob data manager should manage only the one blob
            # assigned to it.  Assert that this is the case and it is the
            # correct blob
            assert self._p_blob_manager.blob is self
            self._p_blob_manager.register_fh(result)

    # utility methods which should not cause the object's state to be
    # loaded if they are called while the object is a ghost.  Thus,
    # they are named with the _p_ convention and only operate against
    # other _p_ instance attributes. We conventionally name these methods
    # and attributes with a _p_blob prefix.

    def _p_blob_clear(self):
        self._p_blob_readers = 0
        self._p_blob_writers = 0

    def _p_blob_decref(self, mode):
        if mode == 'r':
            self._p_blob_readers = max(0, self._p_blob_readers - 1)
        else:
            assert mode in valid_modes, "Invalid mode %r" % mode
            self._p_blob_writers = max(0, self._p_blob_writers - 1)

    def _p_blob_refcounts(self):
        # used by unit tests
        return self._p_blob_readers, self._p_blob_writers


class BlobDataManager:
    """Special data manager to handle transaction boundaries for blobs.

    Blobs need some special care-taking on transaction boundaries. As

    a) the ghost objects might get reused, the _p_reader and _p_writer
       refcount attributes must be set to a consistent state
    b) the file objects might get passed out of the thread/transaction
       and must deny any relationship to the original blob.
    c) writable blob filehandles must be closed at the end of a txn so
       as to not allow reuse between two transactions.

    """

    zope.interface.implements(transaction.interfaces.IDataManager)

    def __init__(self, blob, filehandle, tm):
        self.blob = blob
        self.transaction = tm.get()
        # we keep a weakref to the file handle because we don't want to
        # keep it alive if all other references to it die (e.g. in the
        # case it's opened without assigning it to a name).
        self.fhrefs = utils.WeakSet()
        self.register_fh(filehandle)
        self.sortkey = time.time()
        self.prepared = False

    # Blob specific methods

    def register_fh(self, filehandle):
        self.fhrefs.add(filehandle)

    def _remove_uncommitted_data(self):
        self.blob._p_blob_clear()
        self.fhrefs.map(lambda fhref: fhref.close())
        if (self.blob._p_blob_uncommitted is not None and
            os.path.exists(self.blob._p_blob_uncommitted)):
            os.unlink(self.blob._p_blob_uncommitted)
            self.blob._p_blob_uncommitted = None

    # IDataManager

    def tpc_begin(self, transaction):
        if self.prepared:
            raise TypeError('Already prepared')
        self._checkTransaction(transaction)
        self.prepared = True
        self.transaction = transaction
        self.fhrefs.map(lambda fhref: fhref.close())

    def commit(self, transaction):
        if not self.prepared:
            raise TypeError('Not prepared to commit')
        self._checkTransaction(transaction)
        self.transaction = None
        self.prepared = False

        self.blob._p_blob_clear() 

    def abort(self, transaction):
        self.tpc_abort(transaction)

    def tpc_abort(self, transaction):
        self._checkTransaction(transaction)
        if self.transaction is not None:
            self.transaction = None
        self.prepared = False

        self._remove_uncommitted_data()

    def tpc_finish(self, transaction):
        pass

    def tpc_vote(self, transaction):
        pass

    def sortKey(self):
        return self.sortkey

    def _checkTransaction(self, transaction):
        if (self.transaction is not None and
            self.transaction is not transaction):
            raise TypeError("Transaction missmatch",
                            transaction, self.transaction)


class BlobFile(file):
    """A BlobFile that holds a file handle to actual blob data.

    It is a file that can be used within a transaction boundary; a BlobFile is
    just a Python file object, we only override methods which cause a change to
    blob data in order to call methods on our 'parent' persistent blob object
    signifying that the change happened.

    """

    # XXX these files should be created in the same partition as
    # the storage later puts them to avoid copying them ...

    def __init__(self, name, mode, blob):
        super(BlobFile, self).__init__(name, mode+'b')
        self.blob = blob
        self.close_called = False

    def write(self, data):
        super(BlobFile, self).write(data)
        self.blob._change()

    def writelines(self, lines):
        super(BlobFile, self).writelines(lines)
        self.blob._change()

    def truncate(self, size=0):
        super(BlobFile, self).truncate(size)
        self.blob._change()

    def close(self):
        # we don't want to decref twice
        if not self.close_called:
            self.blob._p_blob_decref(self.mode[:-1])
            self.close_called = True
            super(BlobFile, self).close()

    def __del__(self):
        # XXX we need to ensure that the file is closed at object
        # expiration or our blob's refcount won't be decremented.
        # This probably needs some work; I don't know if the names
        # 'BlobFile' or 'super' will be available at program exit, but
        # we'll assume they will be for now in the name of not
        # muddying the code needlessly.
        self.close()

_pid = str(os.getpid())

def log(msg, level=logging.INFO, subsys=_pid, exc_info=False):
    message = "(%s) %s" % (subsys, msg)
    logger.log(level, message, exc_info=exc_info)


class FilesystemHelper:
    # Storages that implement IBlobStorage can choose to use this
    # helper class to generate and parse blob filenames.  This is not
    # a set-in-stone interface for all filesystem operations dealing
    # with blobs and storages needn't indirect through this if they
    # want to perform blob storage differently.

    def __init__(self, base_dir):
        self.base_dir = base_dir

    def create(self):
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir, 0700)
            log("Blob cache directory '%s' does not exist. "
                "Created new directory." % self.base_dir,
                level=logging.INFO)

    def isSecure(self, path):
        """Ensure that (POSIX) path mode bits are 0700."""
        return (os.stat(path).st_mode & 077) != 0

    def checkSecure(self):
        if not self.isSecure(self.base_dir):
            log('Blob dir %s has insecure mode setting' % self.base_dir,
                level=logging.WARNING)

    def getPathForOID(self, oid):
        """Given an OID, return the path on the filesystem where
        the blob data relating to that OID is stored.

        """
        return os.path.join(self.base_dir, utils.oid_repr(oid))

    def getBlobFilename(self, oid, tid):
        """Given an oid and a tid, return the full filename of the
        'committed' blob file related to that oid and tid.

        """
        oid_path = self.getPathForOID(oid)
        filename = "%s%s" % (utils.tid_repr(tid), BLOB_SUFFIX)
        return os.path.join(oid_path, filename)

    def blob_mkstemp(self, oid, tid):
        """Given an oid and a tid, return a temporary file descriptor
        and a related filename.

        The file is guaranteed to exist on the same partition as committed
        data, which is important for being able to rename the file without a
        copy operation.  The directory in which the file will be placed, which
        is the return value of self.getPathForOID(oid), must exist before this
        method may be called successfully.

        """
        oidpath = self.getPathForOID(oid)
        fd, name = tempfile.mkstemp(suffix='.tmp', prefix=utils.tid_repr(tid),
                                    dir=oidpath)
        return fd, name

    def splitBlobFilename(self, filename):
        """Returns the oid and tid for a given blob filename.

        If the filename cannot be recognized as a blob filename, (None, None)
        is returned.

        """
        if not filename.endswith(BLOB_SUFFIX):
            return None, None
        path, filename = os.path.split(filename)
        oid = os.path.split(path)[1]

        serial = filename[:-len(BLOB_SUFFIX)]
        oid = utils.repr_to_oid(oid)
        serial = utils.repr_to_oid(serial)
        return oid, serial 

    def getOIDsForSerial(self, search_serial):
        """Return all oids related to a particular tid that exist in
        blob data.

        """
        oids = []
        base_dir = self.base_dir
        for oidpath in os.listdir(base_dir):
            for filename in os.listdir(os.path.join(base_dir, oidpath)):
                blob_path = os.path.join(base_dir, oidpath, filename)
                oid, serial = self.splitBlobFilename(blob_path)
                if search_serial == serial:
                    oids.append(oid)
        return oids

class BlobStorage(SpecificationDecoratorBase):
    """A storage to support blobs."""

    zope.interface.implements(ZODB.interfaces.IBlobStorage)

    # Proxies can't have a __dict__ so specifying __slots__ here allows
    # us to have instance attributes explicitly on the proxy.
    __slots__ = ('fshelper', 'dirty_oids', '_BlobStorage__supportsUndo')

    def __new__(self, base_directory, storage):
        return SpecificationDecoratorBase.__new__(self, storage)

    def __init__(self, base_directory, storage):
        # XXX Log warning if storage is ClientStorage
        SpecificationDecoratorBase.__init__(self, storage)
        self.fshelper = FilesystemHelper(base_directory)
        self.fshelper.create()
        self.fshelper.checkSecure()
        self.dirty_oids = []
        try:
            supportsUndo = storage.supportsUndo
        except AttributeError:
            supportsUndo = False
        else:
            supportsUndo = supportsUndo()
        self.__supportsUndo = supportsUndo

    @non_overridable
    def temporaryDirectory(self):
        return self.fshelper.base_dir


    @non_overridable
    def __repr__(self):
        normal_storage = getProxiedObject(self)
        return '<BlobStorage proxy for %r at %s>' % (normal_storage,
                                                     hex(id(self)))
    @non_overridable
    def storeBlob(self, oid, oldserial, data, blobfilename, version,
                  transaction):
        """Stores data that has a BLOB attached."""
        serial = self.store(oid, oldserial, data, version, transaction)
        assert isinstance(serial, str) # XXX in theory serials could be 
                                       # something else

        # the user may not have called "open" on the blob object,
        # in which case, the blob will not have a filename.
        if blobfilename is not None:
            self._lock_acquire()
            try:
                targetpath = self.fshelper.getPathForOID(oid)
                if not os.path.exists(targetpath):
                    os.makedirs(targetpath, 0700)

                targetname = self.fshelper.getBlobFilename(oid, serial)
                os.rename(blobfilename, targetname)

                # XXX if oid already in there, something is really hosed.
                # The underlying storage should have complained anyway
                self.dirty_oids.append((oid, serial))
            finally:
                self._lock_release()
            return self._tid

    @non_overridable
    def tpc_finish(self, *arg, **kw):
        # We need to override the base storage's tpc_finish instead of
        # providing a _finish method because methods found on the proxied 
        # object aren't rebound to the proxy
        getProxiedObject(self).tpc_finish(*arg, **kw)
        self.dirty_oids = []

    @non_overridable
    def tpc_abort(self, *arg, **kw):
        # We need to override the base storage's abort instead of
        # providing an _abort method because methods found on the proxied object
        # aren't rebound to the proxy
        getProxiedObject(self).tpc_abort(*arg, **kw)
        while self.dirty_oids:
            oid, serial = self.dirty_oids.pop()
            clean = self.fshelper.getBlobFilename(oid, serial)
            if os.exists(clean):
                os.unlink(clean) 

    @non_overridable
    def loadBlob(self, oid, serial):
        """Return the filename where the blob file can be found.
        """
        filename = self.fshelper.getBlobFilename(oid, serial)
        if not os.path.exists(filename):
            return None
        return filename

    @non_overridable
    def _packUndoing(self, packtime, referencesf):
        # Walk over all existing revisions of all blob files and check
        # if they are still needed by attempting to load the revision
        # of that object from the database.  This is maybe the slowest
        # possible way to do this, but it's safe.

        # XXX we should be tolerant of "garbage" directories/files in
        # the base_directory here.

        base_dir = self.fshelper.base_dir
        for oid_repr in os.listdir(base_dir):
            oid = utils.repr_to_oid(oid_repr)
            oid_path = os.path.join(base_dir, oid_repr)
            files = os.listdir(oid_path)
            files.sort()

            for filename in files:
                filepath = os.path.join(oid_path, filename)
                whatever, serial = self.fshelper.splitBlobFilename(filepath)
                try:
                    fn = self.fshelper.getBlobFilename(oid, serial)
                    self.loadSerial(oid, serial)
                except POSKeyError:
                    os.unlink(filepath)

            if not os.listdir(oid_path):
                shutil.rmtree(oid_path)

    @non_overridable
    def _packNonUndoing(self, packtime, referencesf):
        base_dir = self.fshelper.base_dir
        for oid_repr in os.listdir(base_dir):
            oid = utils.repr_to_oid(oid_repr)
            oid_path = os.path.join(base_dir, oid_repr)
            exists = True

            try:
                self.load(oid, None) # no version support
            except (POSKeyError, KeyError):
                exists = False

            if exists:
                files = os.listdir(oid_path)
                files.sort()
                latest = files[-1] # depends on ever-increasing tids
                files.remove(latest)
                for file in files:
                    os.unlink(os.path.join(oid_path, file))
            else:
                shutil.rmtree(oid_path)
                continue

            if not os.listdir(oid_path):
                shutil.rmtree(oid_path)

    @non_overridable
    def pack(self, packtime, referencesf):
        """Remove all unused oid/tid combinations."""
        unproxied = getProxiedObject(self)

        # pack the underlying storage, which will allow us to determine
        # which serials are current.
        result = unproxied.pack(packtime, referencesf)

        # perform a pack on blob data
        self._lock_acquire()
        try:
            if self.__supportsUndo:
                self._packUndoing(packtime, referencesf)
            else:
                self._packNonUndoing(packtime, referencesf)
        finally:
            self._lock_release()

        return result

    @non_overridable
    def getSize(self):
        """Return the size of the database in bytes."""
        orig_size = getProxiedObject(self).getSize()

        blob_size = 0
        base_dir = self.fshelper.base_dir
        for oid in os.listdir(base_dir):
            for serial in os.listdir(os.path.join(base_dir, oid)):
                if not serial.endswith(BLOB_SUFFIX):
                    continue
                file_path = os.path.join(base_dir, oid, serial)
                blob_size += os.stat(file_path).st_size

        return orig_size + blob_size

    @non_overridable
    def undo(self, serial_id, transaction):
        undo_serial, keys = getProxiedObject(self).undo(serial_id, transaction)
        # serial_id is the transaction id of the txn that we wish to undo.
        # "undo_serial" is the transaction id of txn in which the undo is
        # performed.  "keys" is the list of oids that are involved in the
        # undo transaction.

        # The serial_id is assumed to be given to us base-64 encoded
        # (belying the web UI legacy of the ZODB code :-()
        serial_id = base64.decodestring(serial_id+'\n')

        self._lock_acquire()

        try:
            # we get all the blob oids on the filesystem related to the
            # transaction we want to undo.
            for oid in self.fshelper.getOIDsForSerial(serial_id):

                # we want to find the serial id of the previous revision
                # of this blob object.
                load_result = self.loadBefore(oid, serial_id)

                if load_result is None:
                    # There was no previous revision of this blob
                    # object.  The blob was created in the transaction
                    # represented by serial_id.  We copy the blob data
                    # to a new file that references the undo
                    # transaction in case a user wishes to undo this
                    # undo.
                    orig_fn = self.fshelper.getBlobFilename(oid, serial_id)
                    new_fn = self.fshelper.getBlobFilename(oid, undo_serial)
                else:
                    # A previous revision of this blob existed before the
                    # transaction implied by "serial_id".  We copy the blob
                    # data to a new file that references the undo transaction
                    # in case a user wishes to undo this undo.
                    data, serial_before, serial_after = load_result
                    orig_fn = self.fshelper.getBlobFilename(oid, serial_before)
                    new_fn = self.fshelper.getBlobFilename(oid, undo_serial)
                orig = open(orig_fn, "r")
                new = open(new_fn, "wb")
                utils.cp(orig, new)
                orig.close()
                new.close()
                self.dirty_oids.append((oid, undo_serial))

        finally:
            self._lock_release()
        return undo_serial, keys

# To do:
# 
# Production
# 
#     - Ensure we detect and replay a failed txn involving blobs forward or
#       backward at startup.
#
#     Jim: What does this mean?
# 
# Far future
# 
#       More options for blob directory structures (e.g. dirstorages
#       bushy/chunky/lawn/flat).
# 
#       Make the ClientStorage support minimizing the blob
#       cache. (Idea: LRU principle via mstat access time and a
#       size-based threshold) currently).
# 
#       Make blobs able to efficiently consume existing files from the
#       filesystem
# 
# Savepoint support
# =================
# 
#  - A savepoint represents the whole state of the data at a certain point in
#    time
# 
#  - Need special storage for blob savepointing (in the spirit of tmpstorage) 
# 
#  - What belongs to the state of the data?
# 
#    - Data contained in files at that point in time
# 
#    - File handles are complex because they might be referred to from various
#      places. We would have to introduce an abstraction layer to allow
#      switching them around... 
# 
#      Simpler solution: :
