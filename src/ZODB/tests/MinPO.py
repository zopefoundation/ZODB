"""A minimal persistent object to use for tests"""

from Persistence import Persistent

class MinPO(Persistent):
    def __init__(self, value=None):
        self.value = value

    def __cmp__(self, aMinPO):
        return cmp(self.value, aMinPO.value)

    def __repr__(self):
        return "MinPO(%s)" % self.value
