##############################################################################
#
# Copyright (c) 2005 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

ZODB Blob support
=================

You create a blob like this:

    >>> from ZODB.Blobs.Blob import Blob
    >>> myblob = Blob()

Opening a new Blob for reading fails:

    >>> myblob.open("r")
    Traceback (most recent call last):
        ...
    BlobError: Blob does not exist.

But we can write data to a new Blob by opening it for writing:

    >>> f = myblob.open("w")
    >>> f.write("Hi, Blob!")

If we try to open a Blob again while it is open for writing, we get an error:

    >>> myblob.open("r")
    Traceback (most recent call last):
        ...
    BlobError: Already opened for writing.

We can close the file:

    >>> f.close()

Now we can open it for reading:

    >>> f2 = myblob.open("r")

And we get the data back:

    >>> f2.read()
    'Hi, Blob!'

If we want to, we can open it again:

    >>> f3 = myblob.open("r")
    >>> f3.read()
    'Hi, Blob!'

But we can't  open it for writing, while it is opened for reading:

    >>> myblob.open("a")
    Traceback (most recent call last):
        ...
    BlobError: Already opened for reading.

Before we can write, we have to close the readers:

    >>> f2.close()
    >>> f3.close()

Now we can open it for writing again and e.g. append data:

    >>> f4 = myblob.open("a")
    >>> f4.write("\nBlob is fine.")
    >>> f4.close()

Now we can read it:

    >>> myblob.open("r").read()
    'Hi, Blob!\nBlob is fine.'


