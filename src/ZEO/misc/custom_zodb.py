# Sample custom_zodb.py
__version__ = "$Revision: 1.1 $"[11:-2]

# In situations where we switch between different storages, we've
# found it useful to use if-elif-else pattern.
import os

if 0: # Change the 0 to 1 to enable!
    
    # ZEO Unix Domain Socket

    # This import isn't strictly necessary but is helpful when
    # debugging and while Zope's and Python's asyncore are out of sync
    # to make sure we get the right version of asyncore.
    import ZServer

    import ZEO.ClientStorage
    Storage=ZEO.ClientStorage.ClientStorage(
        os.path.join(INSTANCE_HOME, 'var', 'zeo.soc'),
        # If no name is given, then connection info will be shown:
        name="ZEO Storage",
        # You can specify the storage name, which defaults to "1":
        storage="1",
        )

else:

    # Default FileStorage
    import ZODB.FileStorage
    Storage=ZODB.FileStorage.FileStorage(
        os.path.join(INSTANCE_HOME, 'var', 'Data.fs'),
        )

