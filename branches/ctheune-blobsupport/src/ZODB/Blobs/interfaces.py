
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
