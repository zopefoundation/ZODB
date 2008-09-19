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
import binascii
import logging
import os
import re
import shutil
import stat
import sys
import tempfile
import threading
import time
import weakref

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
SAVEPOINT_SUFFIX = ".spb"

LAYOUT_MARKER = '.layout'
LAYOUTS = {}

valid_modes = 'r', 'w', 'r+', 'a'

# Threading issues:
# We want to support closing blob files when they are destroyed.
# This introduces a threading issue, since a blob file may be destroyed
# via GC in any thread.


class Blob(persistent.Persistent):
    """A BLOB supports efficient handling of large data within ZODB."""

    zope.interface.implements(ZODB.interfaces.IBlob)

    _p_blob_uncommitted = None  # Filename of the uncommitted (dirty) data
    _p_blob_committed = None    # Filename of the committed data

    readers = writers = None

    def __init__(self):
        # Raise exception if Blobs are getting subclassed
        # refer to ZODB-Bug No.127182 by Jim Fulton on 2007-07-20
        if (self.__class__ is not Blob):
            raise TypeError('Blobs do not support subclassing.')
        self.__setstate__()

    def __setstate__(self, state=None):
        # we use lists here because it will allow us to add and remove
        # atomically
        self.readers = []
        self.writers = []

    def __getstate__(self):
        return None

    def _p_deactivate(self):
        # Only ghostify if we are unopened.
        if self.readers or self.writers:
            return
        super(Blob, self)._p_deactivate()

    def _p_invalidate(self):
        # Force-close any open readers or writers,
        # XXX should we warn of this? Maybe?
        if self._p_changed is None:
            return
        for ref in (self.readers or [])+(self.writers or []):
            f = ref()
            if f is not None:
                f.close()

        if (self._p_blob_uncommitted):
            os.remove(self._p_blob_uncommitted)

        super(Blob, self)._p_invalidate()

    def opened(self):
        return bool(self.readers or self.writers)

    def closed(self, f):
        # We use try/except below because another thread might remove
        # the ref after we check it if the file is GCed.
        for file_refs in (self.readers, self.writers):
            for ref in file_refs:
                if ref() is f:
                    try:
                        file_refs.remove(ref)
                    except ValueError:
                        pass
                    return

    def open(self, mode="r"):
        if mode not in valid_modes:
            raise ValueError("invalid mode", mode)

        if self.writers:
            raise BlobError("Already opened for writing.")

        if mode == 'r':
            if self._current_filename() is None:
                self._create_uncommitted_file()

            result = BlobFile(self._current_filename(), mode, self)

            def destroyed(ref, readers=self.readers):
                try:
                    readers.remove(ref)
                except ValueError:
                    pass

            self.readers.append(weakref.ref(result, destroyed))
        else:
            if self.readers:
                raise BlobError("Already opened for reading.")

            if mode == 'w':
                if self._p_blob_uncommitted is None:
                    self._create_uncommitted_file()
                result = BlobFile(self._p_blob_uncommitted, mode, self)
            else:
                if self._p_blob_uncommitted is None:
                    # Create a new working copy
                    self._create_uncommitted_file()
                    result = BlobFile(self._p_blob_uncommitted, mode, self)
                    utils.cp(file(self._p_blob_committed), result)
                    if mode == 'r+':
                        result.seek(0)
                else:
                    # Re-use existing working copy
                    result = BlobFile(self._p_blob_uncommitted, mode, self)

            def destroyed(ref, writers=self.writers):
                try:
                    writers.remove(ref)
                except ValueError:
                    pass

            self.writers.append(weakref.ref(result, destroyed))

            self._p_changed = True

        return result

    def committed(self):
        if (self._p_blob_uncommitted
            or
            not self._p_blob_committed
            or
            self._p_blob_committed.endswith(SAVEPOINT_SUFFIX)
            ):
            raise BlobError('Uncommitted changes')
        return self._p_blob_committed

    def consumeFile(self, filename):
        """Will replace the current data of the blob with the file given under
        filename.
        """
        if self.writers:
            raise BlobError("Already opened for writing.")
        if self.readers:
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
            os.remove(target)

        try:
            rename_or_copy_blob(filename, target, chmod=False)
        except:
            # Recover from the failed consumption: First remove the file, it
            # might exist and mark the pointer to the uncommitted file.
            self._p_blob_uncommitted = None
            if os.path.exists(target):
                os.remove(target)

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
                os.remove(target_aside)

            # We changed the blob state and have to make sure we join the
            # transaction.
            self._p_changed = True

    # utility methods

    def _current_filename(self):
        # NOTE: _p_blob_committed and _p_blob_uncommitted appear by virtue of
        # Connection._setstate
        return self._p_blob_uncommitted or self._p_blob_committed

    def _create_uncommitted_file(self):
        assert self._p_blob_uncommitted is None, (
            "Uncommitted file already exists.")
        if self._p_jar:
            tempdir = self._p_jar.db()._storage.temporaryDirectory()
        else:
            tempdir = tempfile.gettempdir()
        filename = utils.mktemp(dir=tempdir)
        self._p_blob_uncommitted = filename

        def cleanup(ref):
            if os.path.exists(filename):
                os.remove(filename)

        self._p_blob_ref = weakref.ref(self, cleanup)
        return filename

    def _uncommitted(self):
        # hand uncommitted data to connection, relinquishing responsibility
        # for it.
        filename = self._p_blob_uncommitted
        if filename is None and self._p_blob_committed is None:
            filename = self._create_uncommitted_file()
        self._p_blob_uncommitted = self._p_blob_ref = None
        return filename

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

    def close(self):
        self.blob.closed(self)
        file.close(self)

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

    def __init__(self, base_dir, layout_name='automatic'):
        self.base_dir = os.path.normpath(base_dir) + os.path.sep
        self.temp_dir = os.path.join(base_dir, 'tmp')

        if layout_name == 'automatic':
            layout_name = auto_layout_select(base_dir)
        if layout_name == 'lawn':
            log('The `lawn` blob directory layout is deprecated due to '
                'scalability issues on some file systems, please consider '
                'migrating to the `bushy` layout.', level=logging.WARN)
        self.layout_name = layout_name 
        self.layout = LAYOUTS[layout_name]

    def create(self):
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir, 0700)
            log("Blob directory '%s' does not exist. "
                "Created new directory." % self.base_dir)
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir, 0700)
            log("Blob temporary directory '%s' does not exist. "
                "Created new directory." % self.temp_dir)

        if not os.path.exists(os.path.join(self.base_dir, LAYOUT_MARKER)):
            layout_marker = open(
                os.path.join(self.base_dir, LAYOUT_MARKER), 'wb')
            layout_marker.write(self.layout_name)
        else:
            layout = open(os.path.join(self.base_dir, LAYOUT_MARKER), 'rb'
                          ).read().strip()
            if layout != self.layout_name:
                raise ValueError(
                    "Directory layout `%s` selected for blob directory %s, but "
                    "marker found for layout `%s`" %
                    (self.layout_name, self.base_dir, layout))

    def isSecure(self, path):
        """Ensure that (POSIX) path mode bits are 0700."""
        return (os.stat(path).st_mode & 077) == 0

    def checkSecure(self):
        if not self.isSecure(self.base_dir):
            log('Blob dir %s has insecure mode setting' % self.base_dir,
                level=logging.WARNING)

    def getPathForOID(self, oid, create=False):
        """Given an OID, return the path on the filesystem where
        the blob data relating to that OID is stored.

        If the create flag is given, the path is also created if it didn't
        exist already.

        """
        # OIDs are numbers and sometimes passed around as integers. For our
        # computations we rely on the 64-bit packed string representation.
        if isinstance(oid, int):
            oid = utils.p64(oid)

        path = self.layout.oid_to_path(oid)
        path = os.path.join(self.base_dir, path)

        if create and not os.path.exists(path):
            try:
                os.makedirs(path, 0700)
            except OSError:
                # We might have lost a race.  If so, the directory
                # must exist now
                assert os.path.exists(targetpath)
        return path

    def getOIDForPath(self, path):
        """Given a path, return an OID, if the path is a valid path for an
        OID. The inverse function to `getPathForOID`.

        Raises ValueError if the path is not valid for an OID.

        """
        path = path[len(self.base_dir):]
        return self.layout.path_to_oid(path)

    def getBlobFilename(self, oid, tid):
        """Given an oid and a tid, return the full filename of the
        'committed' blob file related to that oid and tid.

        """
        oid_path = self.getPathForOID(oid)
        # TIDs are numbers and sometimes passed around as integers. For our
        # computations we rely on the 64-bit packed string representation
        if isinstance(tid, int):
            tid = utils.p64(tid)
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
        oid = self.getOIDForPath(path)

        serial = filename[:-len(BLOB_SUFFIX)]
        serial = utils.repr_to_oid(serial)
        return oid, serial 

    def getOIDsForSerial(self, search_serial):
        """Return all oids related to a particular tid that exist in
        blob data.

        """
        oids = []
        for oid, oidpath in self.listOIDs():
            for filename in os.listdir(oidpath):
                blob_path = os.path.join(oidpath, filename)
                oid, serial = self.splitBlobFilename(blob_path)
                if search_serial == serial:
                    oids.append(oid)
        return oids

    def listOIDs(self):
        """Iterates over all paths under the base directory that contain blob
        files.
        """
        for path, dirs, files in os.walk(self.base_dir):
            # Make sure we traverse in a stable order. This is mainly to make
            # testing predictable.
            dirs.sort()
            files.sort()
            try:
                oid = self.getOIDForPath(path)
            except ValueError:
                continue
            yield oid, path


class BlobStorageError(Exception):
    """The blob storage encountered an invalid state."""

def auto_layout_select(path):
    # A heuristic to look at a path and determine which directory layout to
    # use.
    layout_marker = os.path.join(path, LAYOUT_MARKER)
    if not os.path.exists(path):
        log('Blob directory %s does not exist. '
            'Selected `bushy` layout. ' % path)
        layout = 'bushy'
    elif len(os.listdir(path)) == 0:
        log('Blob directory `%s` is unused and has no layout marker set. '
            'Selected `bushy` layout. ' % path)
        layout = 'bushy'
    elif LAYOUT_MARKER not in os.listdir(path):
        log('Blob directory `%s` is used but has no layout marker set. '
            'Selected `lawn` layout. ' % path)
        layout = 'lawn'
    else:
        layout = open(layout_marker, 'rb').read()
        layout = layout.strip()
        log('Blob directory `%s` has layout marker set. '
            'Selected `%s` layout. ' % (path, layout))
    return layout


class BushyLayout(object):
    """A bushy directory layout for blob directories.

    Creates an 8-level directory structure (one level per byte) in
    big-endian order from the OID of an object.

    """

    blob_path_pattern = re.compile(
        r'(0x[0-9a-f]{1,2}\%s){7,7}0x[0-9a-f]{1,2}$' % os.path.sep)

    def oid_to_path(self, oid):
        directories = []
        # Create the bushy directory structure with the least significant byte
        # first
        for byte in str(oid):
            directories.append('0x%s' % binascii.hexlify(byte))
        return os.path.sep.join(directories)

    def path_to_oid(self, path):
        if self.blob_path_pattern.match(path) is None:
            raise ValueError("Not a valid OID path: `%s`" % path)
        path = path.split(os.path.sep)
        # Each path segment stores a byte in hex representation. Turn it into
        # an int and then get the character for our byte string.
        oid = ''.join(binascii.unhexlify(byte[2:]) for byte in path)
        return oid

LAYOUTS['bushy'] = BushyLayout()


class LawnLayout(object):
    """A shallow directory layout for blob directories.

    Creates a single level of directories (one for each oid).

    """

    def oid_to_path(self, oid):
        return utils.oid_repr(oid)

    def path_to_oid(self, path):
        try:
            if path == '':
                # This is a special case where repr_to_oid converts '' to the
                # OID z64.
                raise TypeError()
            return utils.repr_to_oid(path)
        except TypeError:
            raise ValueError('Not a valid OID path: `%s`' % path)

LAYOUTS['lawn'] = LawnLayout()

class BlobStorage(SpecificationDecoratorBase):
    """A storage to support blobs."""

    zope.interface.implements(ZODB.interfaces.IBlobStorage)

    # Proxies can't have a __dict__ so specifying __slots__ here allows
    # us to have instance attributes explicitly on the proxy.
    __slots__ = ('fshelper', 'dirty_oids', '_BlobStorage__supportsUndo',
                 '_blobs_pack_is_in_progress', )

    def __new__(self, base_directory, storage, layout='automatic'):
        return SpecificationDecoratorBase.__new__(self, storage)

    def __init__(self, base_directory, storage, layout='automatic'):
        # XXX Log warning if storage is ClientStorage
        SpecificationDecoratorBase.__init__(self, storage)
        self.fshelper = FilesystemHelper(base_directory, layout)
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
        self._blobs_pack_is_in_progress = False

    @non_overridable
    def temporaryDirectory(self):
        return self.fshelper.temp_dir

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

        self._lock_acquire()
        try:
            self.fshelper.getPathForOID(oid, create=True)
            targetname = self.fshelper.getBlobFilename(oid, serial)
            rename_or_copy_blob(blobfilename, targetname)

            # if oid already in there, something is really hosed.
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
            if os.path.exists(clean):
                remove_committed(clean) 

    @non_overridable
    def loadBlob(self, oid, serial):
        """Return the filename where the blob file can be found.
        """
        filename = self.fshelper.getBlobFilename(oid, serial)
        if not os.path.exists(filename):
            raise POSKeyError("No blob file", oid, serial)
        return filename

    @non_overridable
    def _packUndoing(self, packtime, referencesf):
        # Walk over all existing revisions of all blob files and check
        # if they are still needed by attempting to load the revision
        # of that object from the database.  This is maybe the slowest
        # possible way to do this, but it's safe.
        for oid, oid_path in self.fshelper.listOIDs():
            files = os.listdir(oid_path)

            for filename in files:
                filepath = os.path.join(oid_path, filename)
                whatever, serial = self.fshelper.splitBlobFilename(filepath)
                try:
                    self.loadSerial(oid, serial)
                except POSKeyError:
                    remove_committed(filepath)

            if not os.listdir(oid_path):
                shutil.rmtree(oid_path)

    @non_overridable
    def _packNonUndoing(self, packtime, referencesf):
        for oid, oid_path in self.fshelper.listOIDs():
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
                    remove_committed(os.path.join(oid_path, file))
            else:
                remove_committed_dir(oid_path)
                continue

            if not os.listdir(oid_path):
                shutil.rmtree(oid_path)

    @non_overridable
    def pack(self, packtime, referencesf):
        """Remove all unused OID/TID combinations."""
        self._lock_acquire()
        try:
            if self._blobs_pack_is_in_progress:
                raise BlobStorageError('Already packing')
            self._blobs_pack_is_in_progress = True
        finally:
            self._lock_release()

        try:
            # Pack the underlying storage, which will allow us to determine
            # which serials are current.
            unproxied = getProxiedObject(self)
            result = unproxied.pack(packtime, referencesf)

            # Perform a pack on the blob data.
            if self.__supportsUndo:
                self._packUndoing(packtime, referencesf)
            else:
                self._packNonUndoing(packtime, referencesf)
        finally:
            self._lock_acquire()
            self._blobs_pack_is_in_progress = False
            self._lock_release()

        return result

    @non_overridable
    def getSize(self):
        """Return the size of the database in bytes."""
        # XXX The old way of computing is way to resource hungry. We need to
        # do some kind of estimation instead.
        return getProxiedObject(self).getSize()

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
                    # undo. It would be nice if we had some way to
                    # link to old blobs.
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


copied = logging.getLogger('ZODB.blob.copied').debug
def rename_or_copy_blob(f1, f2, chmod=True):
    """Try to rename f1 to f2, fallback to copy.

    Under certain conditions a rename might not work, e.g. because the target
    directory is on a different partition. In this case we try to copy the
    data and remove the old file afterwards.

    """
    try:
        os.rename(f1, f2)
    except OSError:
        copied("Copied blob file %r to %r.", f1, f2)
        file1 = open(f1, 'rb')
        file2 = open(f2, 'wb')
        try:
            utils.cp(file1, file2)
        finally:
            file1.close()
            file2.close()
        remove_committed(f1)
    if chmod:
        os.chmod(f2, stat.S_IREAD)

if sys.platform == 'win32':
    # On Windows, you can't remove read-only files, so make the
    # file writable first.

    def remove_committed(filename):
        os.chmod(filename, stat.S_IWRITE)
        os.remove(filename)

    def remove_committed_dir(path):
        for (dirpath, dirnames, filenames) in os.walk(path):
            for filename in filenames:
                filename = os.path.join(dirpath, filename)
                remove_committed(filename)
        shutil.rmtree(path)
else:
    remove_committed = os.remove
    remove_committed_dir = shutil.rmtree
