A change in the way databases were initialized affected tests
=============================================================

Originally, databases added root objects by interacting directly with
storages, rather than using connections.  As storages transaction
interaction became more complex, interacting directly with storages
let to duplicated code (and buggy) code.

See: https://github.com/zopefoundation/ZODB/issues/84

Fixing this had some impacts that affected tests:

- New databases now have a connection with a single object in it's cache.
  This is a very slightly good thing, but it broke some tests expectations.

- Tests that manipulated time, had their clocks off because of new time calls.

This led to some test fixes, in many cases adding a mysterious
``cacheMinimize()`` call.
