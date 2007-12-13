===========================================
How to use NFS to make Blobs more efficient
===========================================

:Author: Christian Theune <ct@gocept.com>

Overview
========

When handling blobs, the biggest goal is to avoid writing operations that
require the blob data to be transferred using up IO resources.

When bringing a blob into the system, at least one O(N) operation has to
happen, e.g. when the blob is uploaded via a network server. The blob should
be extracted as a file on the final storage volume as early as possible,
avoiding further copies.

In a ZEO setup, all data is stored on a networked server and passed to it
using zrpc. This is a major problem for handling blobs, because it will lock
all transactions from committing when storing a single large blob. As a
default, this mechanism works but is not recommended for high-volume
installations.

Shared filesystem
=================

The solution for the transfer problem is to setup various storage parameters
so that blobs are always handled on a single volume that is shared via network
between ZEO servers and clients.

Step 1: Setup a writable shared filesystem for ZEO server and client
--------------------------------------------------------------------

On the ZEO server, create two directories on the volume that will be used by
this setup (assume the volume is accessible via $SERVER/):

    - $SERVER/blobs

    - $SERVER/tmp

Then export the $SERVER directory using a shared network filesystem like NFS.
Make sure it's writable by the ZEO clients.

Assume the exported directory is available on the client as $CLIENT.

Step 2: Application temporary directories
-----------------------------------------

Applications (i.e. Zope) will put uploaded data in a temporary directory
first. Adjust your TMPDIR, TMP or TEMP environment variable to point to the
shared filesystem:

    $ export TMPDIR=$CLIENT/tmp

Step 3: ZEO client caches
-------------------------

Edit the file `zope.conf` on the ZEO client and adjust the configuration of
the `zeoclient` storage with two new variables::

    blob-dir = $CLIENT/blobs
    blob-cache-writable = yes

Step 4: ZEO server
------------------

Edit the file `zeo.conf` on the ZEO server to configure the blob directory.
Assuming the published storage of the ZEO server is a file storage, then the
configuration should look like this::

    <blobstorage 1>
        <filestorage>
            path $INSTANCE/var/Data.fs
        <filestorage>
        blob-dir $SERVER/blobs
    </blobstorage>

(Remember to manually replace $SERVER and $CLIENT with the exported directory
as accessible by either the ZEO server or the ZEO client.)

Conclusion
----------

At this point, after restarting your ZEO server and clients, the blob
directory will be shared and a minimum amount of IO will occur when working
with blobs.
