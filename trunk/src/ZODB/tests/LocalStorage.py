class LocalStorage:
    """A single test that only make sense for local storages.

    A local storage is one that doens't use ZEO. The __len__()
    implementation for ZEO is inexact.
    """
    def checkLen(self):
        # The length of the database ought to grow by one each time
        assert len(self._storage) == 0
        self._dostore()
        assert len(self._storage) == 1
        self._dostore()
        assert len(self._storage) == 2

