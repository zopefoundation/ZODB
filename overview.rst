Overview
========

Python programs are written with the object-oriented paradigm. You use objects
that reference each other freely and can be of any form and shape: no object
has to adhere to a specific schema and can hold arbitrary information.

Storing those objects in relational databases requires you to give up on the
freedom of reference and schema. The constraints of the relational model
reduces your ability to write object-oriented code.

The ZODB is a native object database, that stores your objects while allowing
you to work with any paradigms that can be expressed in Python. Thereby your
code becomes simpler, more robust and easier to understand.

Also, there is no gap between the database and your program: no glue code to
write, no mappings to configure. Have a look at the tutorial to see, how easy
it is.

Some of the features that ZODB brings to you:

* Transparent persistence for Python objects
* Full ACID-compatible transaction support (including savepoints)
* History/undo ability
* Efficient support for binary large objects (BLOBs)
* Pluggable storages
* Scalable architecture
