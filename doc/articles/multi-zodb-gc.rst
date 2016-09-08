Using zc.zodbdgc (fix PosKeyError's)
====================================

*This article was written by Hanno Schlichting*

The `zc.zodbdgc <http://pypi.python.org/pypi/zc.zodbdgc>`_ library contains two
useful features. On the one hand it supports advanced ZODB packing and garbage
collection approaches and on the other hand it includes the ability to create a
database of all persistent references.

The second feature allows us to debug and repair PosKeyErrors by finding the
persistent object(s) that point to the lost object.

Note: This documentation applies to ZODB 3.9 and later. Earlier versions of the
ZODB are not supported, as they lack the fast storage iteration API's required
by `zc.zodbdgc`.

This documentation does not apply to
`RelStorage <http://pypi.python.org/pypi/RelStorage>`_ which has the same
features built-in, but accessible in different ways. Look at the options for
the `zodbpack` script. The `--prepack` option creates a table containing the
same information as we are creating in the reference database.

Setup
-----

We'll assume you are familiar with a buildout setup. A typical config might
look like this::

  [buildout]
  parts =
    zeo
    zeopy
    zeo-conf
    zodbdgc
    refdb-conf

  [zeo]
  recipe = plone.recipe.zeoserver
  zeo-address = 127.0.0.1:8100
  blob-storage = ${buildout:directory}/var/blobstorage
  pack-gc = false
  pack-keep-old = false

  [zeopy]
  recipe = zc.recipe.egg
  eggs =
      ZODB3
      zc.zodbdgc
  interpreter = zeopy
  scripts = zeopy

  [zeo-conf]
  recipe = collective.recipe.template
  input = inline:
    <zodb main>
      <zeoclient>
        blob-dir ${buildout:directory}/var/blobstorage
        shared-blob-dir yes
        server ${zeo:zeo-address}
        storage 1
        name zeostorage
        var ${buildout:directory}/var
      </zeoclient>
    </zodb>
  output = ${buildout:directory}/etc/zeo.conf

  [zodbdgc]
  recipe = zc.recipe.egg
  eggs = zc.zodbdgc

  [refdb-conf]
  recipe = collective.recipe.template
  input = inline:
    <zodb main>
      <filestorage 1>
        path ${buildout:directory}/var/refdb.fs
      </filestorage>
    </zodb>
  output = ${buildout:directory}/etc/refdb.conf


Garbage collection
------------------

We configured the ZEO server to skip garbage collection as part of the normal
pack in the above config (`pack-gc = false`). Instead we use explicit garbage
collection via a different job::

  bin/multi-zodb-gc etc/zeo.conf

On larger databases garbage collection can take a couple hours. We can run this
only once a week or even less frequent. All explicitly deleted objects will
still be packed away by the normal pack, so the database doesn't grow
out-of-bound. We can also run the analysis against a database copy, taking away
load from the live database and only write the resulting deletions to the
production database.


Packing
-------

We can do regular packing every day while the ZEO server is running, via::

  bin/zeopack

Packing without garbage collection is much faster.


Reference analysis and POSKeyErrors
-----------------------------------

If our database has any POSKeyErrors, we can find and repair those.

Either we already have the oids of lost objects, or we can check the entire
database for any errors. To check everything we run the following command::

  $ bin/multi-zodb-check-refs etc/zeo.conf

This can take about 15 to 30 minutes on moderately sized databases of up to
10gb, dependent on disk speed. We'll write down the reported errors, as we'll
need them later on to analyze them.

If there are any lost objects, we can create a reference database to make it
easier to debug and find those lost objects::

  $ bin/multi-zodb-check-refs -r var/refdb.fs etc/zeo.conf

This is significantly slower and can take several hours to complete. Once this
is complete we can open the generated database via our interpreter::

  $ bin/zeopy

  >>> import ZODB.config
  >>> db = ZODB.config.databaseFromFile(open('./etc/refdb.conf'))
  >>> conn = db.open()
  >>> refs = conn.root()['references']

If we've gotten this error report::

  !!! main 13184375 ?
  POSKeyError: 0xc92d77

We can look up the persistent oid it was referenced from via::

  >>> parent = list(refs['main'][13184375])
  >>> parent
  [13178389]

We can also get the hex representation::

  >>> from ZODB.utils import p64
  >>> p64(parent[0])
  '\x00\x00\x00\x00\x00\xc9\x16\x15'

With this information, we should get back to our actual database and look
up this object. We'll leave the ref db open, as we might need to recursively
look up some more objects, until we get one we can identify and work on.

We could load the parent. In a debug prompt we could do something like::

  >>> app._p_jar.get('\x00\x00\x00\x00\x00\xc9\x16\x15')
  2010-04-28 14:28:28 ERROR ZODB.Connection Couldn't load state for 0xc91615
  Traceback (most recent call last):
  ...
  ZODB.POSException.POSKeyError: 0xc92d77

Gah, this gives us the POSKeyError of course. But we can load the actual data
of the parent, to get an idea of what this is::

  >>> app._p_jar.db()._storage.load('\x00\x00\x00\x00\x00\xc9\x16\x15', '')
  ('cBTrees.IOBTree
  IOBucket
  q\x01.((J$KT\x02ccopy_reg
  _reconstructor
  q\x02(cfive.intid.keyreference
  KeyReferenceToPersistent
  ...

Now we can be real evil and create a new fake object in place of the missing
one::

  >>> import transaction
  >>> transaction.begin()

The persistent oid that was reported missing was ``13184375``::

  >>> from ZODB.utils import p64
  >>> p64(13184375)
  '\x00\x00\x00\x00\x00\xc9-w'

  >>> from persistent import Persistent
  >>> a = Persistent()
  >>> a._p_oid = '\x00\x00\x00\x00\x00\xc9-w'

We cannot use the ``add`` method of the connection, as this would assign the
object a new persistent oid. So we replicate its internals here::

  >>> a._p_jar = app._p_jar
  >>> app._p_jar._register(a)
  >>> app._p_jar._added[a._p_oid] = a

  >>> transaction.commit()

Both getting the object as well as its parent will work now::

  >>> app._p_jar.get('\x00\x00\x00\x00\x00\xc9-w')
  <persistent.Persistent object at 0xa3e348c>

  >>> app._p_jar.get('\x00\x00\x00\x00\x00\xc9\x16\x15')
  BTrees.IOBTree.IOBucket([(39078692, <five.intid.keyreference...

Once we are finished we should be nice and close all databases::

  >>> conn.close()
  >>> db.close()

Depending on the class of object that went missing, we might need to use a
different persistent class, like a persistent mapping or a BTree bucket.

In general it's best to remove the parent object and thus our fake object from
the database and rebuild the data structure again via the proper application
level API's.
