import unittest

import ZODB
from ZODB.utils import u64
from ZODB.utils import z64

from .MVCCMappingStorage import MVCCMappingStorage


class PrefetchTests(unittest.TestCase):

    def test_prefetch(self):
        db = ZODB.DB(None)

        fetched = []

        def prefetch(oids, tid):
            fetched.append((list(map(u64, oids)), tid))

        db.storage.prefetch = prefetch

        with db.transaction() as conn:
            for i in range(10):
                conn.root()[i] = conn.root().__class__()

        conn = db.open()
        conn.prefetch(z64)
        conn.prefetch([z64])
        conn.prefetch(conn.root())

        conn.prefetch(z64, (conn.root()[i] for i in range(3)), conn.root()[3])

        self.assertEqual(fetched,
                         [([0], conn._storage._start),
                          ([0], conn._storage._start),
                          ([0], conn._storage._start),
                          ([0, 1, 2, 3, 4], conn._storage._start),
                          ])

        db.close()

    def test_prefetch_optional(self):
        conn = ZODB.connection(None)
        conn.prefetch(z64)
        conn.prefetch([z64])
        conn.prefetch(conn.root())
        conn.prefetch(z64, [z64])
        conn.prefetch(z64, [z64], conn.root())
        conn.close()

    def test_prefetch_optional_imvcc(self):
        conn = ZODB.connection(MVCCMappingStorage())
        conn.prefetch(z64)
        conn.prefetch([z64])
        conn.prefetch(conn.root())
        conn.prefetch(z64, [z64])
        conn.prefetch(z64, [z64], conn.root())
        conn.close()


def test_suite():
    return unittest.makeSuite(PrefetchTests)
