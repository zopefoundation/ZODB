"""Install C-based replacements for BoboPOS components.
"""

import cPickleCache, cPersistence
import BoboPOS3, BoboPOS3.Persistence, BoboPOS3.Connection
BoboPOS3.Persistence.Persistent=cPersistence.Persistent
BoboPOS3.Persistent=cPersistence.Persistent
BoboPOS3.Connection.PickleCache=cPickleCache.PickleCache

