
import os
import tempfile

from zope.interface import implements

from ZODB.Blobs.interfaces import IBlob
from ZODB.Blobs.exceptions import BlobError
from ZODB import utils
from persistent import Persistent

try:
    from ZPublisher.Iterators import IStreamIterator
except ImportError:
    IStreamIterator = None

class Blob(Persistent):
 
    implements(IBlob)

    _p_blob_readers = 0
    _p_blob_writers = 0
    _p_blob_uncommitted = None
    _p_blob_data = None

    def open(self, mode):
        """Returns a file(-like) object for handling the blob data."""
        if mode == "r":
            if self._current_filename() is None:
                raise BlobError, "Blob does not exist."

            if self._p_blob_writers != 0:
                raise BlobError, "Already opened for writing."

            self._p_blob_readers += 1
            return BlobFile(self._current_filename(), "rb", self)

        if mode == "w":
            if self._p_blob_readers != 0:
                raise BlobError, "Already opened for reading."

            if self._p_blob_uncommitted is None:
                self._p_blob_uncommitted = utils.mktemp()

            self._p_blob_writers += 1
            return BlobFile(self._p_blob_uncommitted, "wb", self)

        if mode =="a":
            if self._current_filename() is None:
                raise BlobError, "Blob does not exist."

            if self._p_blob_readers != 0:
                raise BlobError, "Already opened for reading."

            if self._p_blob_uncommitted is None:
                # Create a new working copy
                self._p_blob_uncommitted = utils.mktmp()
                uncommitted = BlobFile(self._p_blob_uncommitted, "wb", self)
                utils.cp(file(self._p_blob_data), uncommitted)
                uncommitted.seek(0)
            else:
                # Re-use existing working copy
                uncommitted = BlobFile(self._p_blob_uncommitted, "ab", self)
            
            self._p_blob_writers +=1
            return uncommitted

    # utility methods

    def _current_filename(self):
        return self._p_blob_uncommitted or self._p_blob_data

class BlobFile(file):

    # XXX those files should be created in the same partition as
    # the storage later puts them to avoid copying them ...

    if IStreamIterator is not None:
        __implements__ = (IStreamIterator,)

    def __init__(self, name, mode, blob):
        super(BlobFile, self).__init__(name, mode)
        self.blob = blob
        self.streamsize = 1<<16

    def write(self, data):
        super(BlobFile, self).write(data)
        self.blob._p_changed = 1

    def writelines(self, lines):
        super(BlobFile, self).writelines(lines)
        self.blob._p_changed = 1

    def truncate(self, size):
        super(BlobFile, self).truncate(size)
        self.blob._p_changed = 1
        
    def close(self):
        if (self.mode.startswith("w") or
            self.mode.startswith("a")):
            self.blob._p_blob_writers -= 1
        else:
            self.blob._p_blob_readers -= 1
        super(BlobFile, self).close()

    def next(self):
        data = self.read(self.streamsize)
        if not data:
            self.blob._p_blob_readers -= 1
            raise StopIteration
        return data

    def __len__(self):
        cur_pos = self.tell()
        self.seek(0, 2)
        size = self.tell()
        self.seek(cur_pos, 0)
        return size


