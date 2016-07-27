Loose ends
==========

- BlobStorage no-longer works with FileStorage.  Since FileStorage
  supports blobs on its own, so there's no reason to wrap a storage
  with BlobStorage other than to test BlobStorage.

  Amongst the choices:

  - Make BlobStorage work with FileStorage.

  - Test BlobStorage with MappingStorage, but that makes it impossible
    to test some advanced features.

