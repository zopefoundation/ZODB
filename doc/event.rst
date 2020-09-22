=============
Event support
=============

Sometimes, you want to react when ZODB does certain things.  In the
past, ZODB provided ad hoc hook functions for this. Going forward,
ZODB will use an event mechanism.  ZODB.event.notify is called with
events of interest.

If zope.event is installed, then ZODB.event.notify is simply an alias
for zope.event.  If zope.event isn't installed, then ZODB.event is a
noop.
