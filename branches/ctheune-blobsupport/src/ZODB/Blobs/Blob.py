
import os

from zope.interface import implements

from ZODB.Blobs.interfaces import IBlob
from ZODB.Blobs.exceptions import BlobError
from ZODB import utils
from persistent import Persistent

class TempFileHandler(object):
    """Handles holding a tempfile around.

    The tempfile is unlinked when the tempfilehandler is GCed.
    """
    
    def __init__(self, directory, mode)
        self.handle, self.filename = tempfile.mkstemp(dir=directory,
                                                      text=mode)
        
    def __del__(self):
        self.handle
        os.unlink(self.filename)

class Blob(Persistent):
 
    implements(IBlob)

    def __init__(self):
        self._p_blob_readers = 0
        self._p_blob_writers = 0
        self._p_blob_uncommitted = None
        self._p_blob_data = None

    def open(self, mode):
        """Returns a file(-like) object for handling the blob data."""

        if mode == "r":
            if self._current_filename() is None:
                raise BlobError, "Blob does not exist."

            if self._p_blob_writers != 0:
                raise BlobError, "Already opened for writing."

            self._p_blob_readers += 1
            return BlobTempFile(self._current_filename(), "rb", self)

        if mode == "w":
            if self._p_blob_readers != 0:
                raise BlobError, "Already opened for reading."

            if self._p_blob_uncommitted is None:
                self._p_blob_uncommitted = self._get_uncommitted_filename()

            self._p_blob_writers += 1
            return BlobTempFile(self._p_blob_uncommitted, "wb", self)

        if mode =="a":
            if self._current_filename() is None:
                raise BlobError, "Blob does not exist."

            if self._p_blob_readers != 0:
                raise BlobError, "Already opened for reading."

            if not self._p_blob_uncommitted:
                # Create a new working copy
                self._p_blob_uncommitted = self._get_uncommitted_filename()
                uncommitted = BlobTempFile(self._p_blob_uncommitted, "wb", self)
                utils.cp(file(self._p_blob_data), uncommitted)
                uncommitted.seek(0)
            else:
                # Re-use existing working copy
                uncommitted = BlobTempFile(self._p_blob_uncommitted, "ab", self)
            
            self._p_blob_writers +=1
            return uncommitted

    # utility methods

    def _current_filename(self):
        return self._p_blob_uncommitted or self._p_blob_data

    def _get_uncommitted_filename(self):
        return os.tempnam()

class BlobFileBase:

    # XXX those files should be created in the same partition as
    # the storage later puts them to avoid copying them ...

    def __init__(self, name, mode, blob):
        file.__init__(self, name, mode)
        self.blob = blob

    def write(self, data):
        file.write(self, data)
        self.blob._p_changed = 1

    def writelines(self, lines):
        file.writelines(self, lines)
        self.blob._p_changed = 1

    def truncate(self, size):
        file.truncate(self, size)
        self.blob._p_changed = 1
        
    def close(self):
        if (self.mode.startswith("w") or
            self.mode.startswith("a")):
            self.blob._p_blob_writers -= 1
        else:
            self.blob._p_blob_readers -= 1
        file.close(self)

class BlobFile(BlobFileBase, file):
    pass

class BlobTempFile(BlobFileBase, NamedTempFile)
    pass

def copy_file(old, new):
    for chunk in old.read(4096):
        new.write(chunk)
    new.seek(0)
