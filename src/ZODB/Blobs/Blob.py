
import os
import time
import tempfile

from zope.interface import implements

from ZODB.Blobs.interfaces import IBlob
from ZODB.Blobs.exceptions import BlobError
from ZODB import utils
import transaction
from transaction.interfaces import IDataManager
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
        result = None

        if mode == "r":
            if self._current_filename() is None:
                raise BlobError, "Blob does not exist."

            if self._p_blob_writers != 0:
                raise BlobError, "Already opened for writing."

            self._p_blob_readers += 1
            result = BlobFile(self._current_filename(), "rb", self)

        if mode == "w":
            if self._p_blob_readers != 0:
                raise BlobError, "Already opened for reading."

            if self._p_blob_uncommitted is None:
                self._p_blob_uncommitted = utils.mktemp()

            self._p_blob_writers += 1
            result = BlobFile(self._p_blob_uncommitted, "wb", self)

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
            result = uncommitted

        if result is not None:
            dm = BlobDataManager(self, result)
            transaction.get().register(dm)
            return result

    # utility methods

    def _current_filename(self):
        return self._p_blob_uncommitted or self._p_blob_data

class BlobDataManager:
    """Special data manager to handle transaction boundaries for blobs.

    Blobs need some special care taking on transaction boundaries. As 
    a) the ghost objects might get reused, the _p_ attributes must be
       set to a consistent state
    b) the file objects might get passed out of the thread/transaction
       and must deny any relationship to the original blob.
    """

    implements(IDataManager)

    def __init__(self, blob, filehandle):
        self.blob = blob
        self.filehandle = filehandle
        self.isSub = False
        self._sortkey = time.time()

    def _cleanUpBlob(self):
        self.blob._p_blob_readers = 0
        self.blob._p_blob_writers = 0
        self.filehandle.cleanTransaction()

    def abort_sub(self, transaction):
        pass

    def commit_sub(self, transaction):
        pass

    def tpc_begin(self, transaction, subtransaction=False):
        self.isSub = subtransaction

    def tpc_abort(self, transaction):
        self._cleanUpBlob()

    def tpc_finish(self, transaction):
        self.isSub = False

    def tpc_vote(self, transaction):
        if not self.isSub:
            self._cleanUpBlob()
                
    def commit(self, object, transaction):
        pass

    def abort(self, object, transaction):
        self._cleanUpBlob()

    def sortKey(self):
        return self._sortkey

    def beforeCompletion(self, transaction):
        pass

    def afterCompletion(self, transaction):
        pass

class BlobFile(file):

    # XXX those files should be created in the same partition as
    # the storage later puts them to avoid copying them ...

    if IStreamIterator is not None:
        __implements__ = (IStreamIterator,)

    def __init__(self, name, mode, blob):
        super(BlobFile, self).__init__(name, mode)
        self.blob = blob
        self.streamsize = 1<<16

    def _p_changed(self):
        if self.blob is not None:
            self.blob._p_changed = 1

    def write(self, data):
        super(BlobFile, self).write(data)
        self._p_changed()

    def writelines(self, lines):
        super(BlobFile, self).writelines(lines)
        self._p_changed()

    def truncate(self, size):
        super(BlobFile, self).truncate(size)
        self._p_changed()
        
    def close(self):
        if self.blob is not None:
            if (self.mode.startswith("w") or
                self.mode.startswith("a")):
                self.blob._p_blob_writers -= 1
            else:
                self.blob._p_blob_readers -= 1
        super(BlobFile, self).close()

    def cleanTransaction(self):
        self.blob = None

    def next(self):
        data = self.read(self.streamsize)
        if not data:
            if self.blob is not None:
                self.blob._p_blob_readers -= 1
            raise StopIteration
        return data

    def __len__(self):
        cur_pos = self.tell()
        self.seek(0, 2)
        size = self.tell()
        self.seek(cur_pos, 0)
        return size


