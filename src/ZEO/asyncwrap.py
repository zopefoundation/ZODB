"""A wrapper for asyncore that provides robust exception handling.

The poll() and loop() calls exported by asyncore can raise exceptions.
asyncore uses either the select() or poll() system call.  It is
possible for those system calls to fail, returning, for example,
EINTR.  Python raises a select.error when an error occurs.  If the
program using asyncore doesn't catch the exception, it will die with
an uncaught exception.

This module exports safer versions of loop() and poll() that wrap the
asyncore calls in try/except handlers that catch the errors and do the
right thing.  In most cases, it is safe to catch the error and simply
retry the call.

XXX Operations on asyncore sockets can also fail with exceptions that
can safely be caught and ignored by user programs.  It's not clear if
it would be useful to extend this module with wrappers for those
errors.
"""

# XXX The current implementation requires Python 2.0.  Not sure if
# that's acceptable, depends on how many users want to combine ZEO 1.0
# and Zope 2.3.

import asyncore
import errno
import select

def loop(*args, **kwargs):
    while 1:
        try:
            apply(asyncore.loop, args, kwargs)
        except select.error, err:
            if err[0] != errno.EINTR:
                raise
        else:
            break
    
def poll(*args, **kwargs):
    try:
        apply(asyncore.poll, args, kwargs)
    except select.error, err:
        if err[0] != errno.EINTR:
            raise
