try:
    from Interface import Base
except ImportError:
    class Base:
        # a dummy interface for use when Zope's is unavailable
        pass

class ICache(Base):
    """ZEO client cache.

    __init__(storage, size, client, var)

    All arguments optional.

    storage -- name of storage
    size -- max size of cache in bytes
    client -- a string; if specified, cache is persistent.
    var -- var directory to store cache files in
    """
    
    def open():
        """Returns a sequence of object info tuples.

        An object info tuple is a pair containing an object id and a
        pair of serialnos, a non-version serialno and a version serialno:
        oid, (serial, ver_serial)

        This method builds an index of the cache and returns a
        sequence used for cache validation.
        """

    def close():
        """Closes the cache."""

    def verify(func):
        """Call func on every object in cache.

        func is called with three arguments
        func(oid, serial, ver_serial)
        """

    def invalidate(oid, version):
        """Remove object from cache."""

    def load(oid, version):
        """Load object from cache.

        Return None if object not in cache.
        Return data, serialno if object is in cache.
        """

    def store(oid, p, s, version, pv, sv):
        """Store a new object in the cache."""

    def update(oid, serial, version, data):
        """Update an object already in the cache.

        XXX This method is called to update objects that were modified by
        a transaction.  It's likely that it is already in the cache,
        and it may be possible for the implementation to operate more
        efficiently.
        """

    def modifiedInVersion(oid):
        """Return the version an object is modified in.

        '' signifies the trunk.
        Returns None if the object is not in the cache.
        """

    def checkSize(size):
        """Check if adding size bytes would exceed cache limit.

        This method is often called just before store or update.  The
        size is a hint about the amount of data that is about to be
        stored.  The cache may want to evict some data to make space.
        """

    
    
        

    
