"""Test that a storage's values persist across open and close."""

class PersistentStorage:

    def checkUpdatesPersist(self):
        oids = []

        def new_oid_wrapper(l=oids, new_oid=self._storage.new_oid):
            oid = new_oid()
            l.append(oid)
            return oid

        self._storage.new_oid = new_oid_wrapper

        self._dostore()
        self._dostore('a')
        oid = self._storage.new_oid()
        revid = self._dostore(oid)
        self._dostore(oid, revid, data=8, version='b')
        oid = self._storage.new_oid()
        revid = self._dostore(oid, data=1)
        revid = self._dostore(oid, revid, data=2)
        self._dostore(oid, revid, data=3)

        # keep copies of all the objects
        objects = []
        for oid in oids:
            p, s = self._storage.load(oid, '')
            objects.append((oid, '', p, s))
            ver = self._storage.modifiedInVersion(oid)
            if ver:
                p, s = self._storage.load(oid, ver)
                objects.append((oid, ver, p, s))

        self._storage.close()
        self.open()

        # keep copies of all the objects
        for oid, ver, p, s in objects:
            _p, _s = self._storage.load(oid, ver)
            self.assertEquals(p, _p)
            self.assertEquals(s, _s)
