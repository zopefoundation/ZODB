
from zope.interface import Interface

class IBlob(Interface):
    """A BLOB supports efficient handling of large data within ZODB."""

    def open(mode):
        """Returns a file(-like) object for handling the blob data.

        mode: Mode to open the file with. Possible values: r,w,r+,a
        """

    # XXX need a method to initialize the blob from the storage
    # this means a) setting the _p_blob_data filename and b) putting
    # the current data in that file

class IBlobStorage(Interface):
    """A storage supporting BLOBs."""

    def storeBlob(oid, oldserial, data, blob, version, transaction):
        """Stores data that has a BLOB attached."""

    def loadBlob(oid, serial, version):
        """Return the filename of the Blob data responding to this OID and
        serial.

        Returns a filename or None if no Blob data is connected with this OID. 

        Raises POSKeyError if the blobfile cannot be found.
        """

