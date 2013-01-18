# this is a package

from ._impl import FileStorage
from ._impl import TransactionRecord
from ._impl import FileIterator
from ._impl import Record
from ._impl import packed_version


# BBB Alias for compatibility
RecordIterator = TransactionRecord


# More BBB in sys.modules
from sys import modules
modules['ZODB.FileStorage'] = modules['ZODB.filestorage']
modules['ZODB.FileStorage.FileStorage'] = modules['ZODB.filestorage._impl']
del modules
