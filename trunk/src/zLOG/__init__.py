##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

"""General logging facility

Note:
  This module exists only for backward compatibility.  Any new code
  for Zope 2.8 and newer should use the logging module from Python's
  standard library directly.  zLOG is only an API shim to map existing
  use of zLOG onto the standard logging API.

This module attempts to provide a simple programming API for logging
with a pluggable API for defining where log messages should go.

Programmers call the LOG function to log information.

The LOG function, in turn, calls the log_write method to actually write
logging information somewhere.  This module provides a very simple
log_write implementation.  It is intended that applications main
programs will replace this method with a method more suited to their needs.

The module provides a register_subsystem method that does nothing, but
provides a hook that logging management systems could use to collect
information about subsystems being used.

The module defines several standard severities:

  TRACE=-300   -- Trace messages

  DEBUG=-200   -- Debugging messages

  BLATHER=-100 -- Somebody shut this app up.

  INFO=0       -- For things like startup and shutdown.

  PROBLEM=100  -- This isn't causing any immediate problems, but deserves
                  attention.

  WARNING=100  -- A wishy-washy alias for PROBLEM.

  ERROR=200    -- This is going to have adverse effects.

  PANIC=300    -- We're dead!

To plug in a log handler, simply replace the log_write function
with a callable object that takes 5 arguments:

      subsystem -- The subsystem generating the message (e.g. ZODB)

      severity -- The "severity" of the event.  This may be an integer or
                  a floating point number.  Logging back ends may
                  consider the int() of this valua to be significant.
                  For example, a backend may consider any severity
                  whos integer value is WARNING to be a warning.

      summary -- A short summary of the event

      detail -- A detailed description

      error -- A three-element tuple consisting of an error type, value, and
               traceback.  If provided, then a summary of the error
               is added to the detail.

The default logging facility uses Python's logging module as a
back-end; configuration of the logging module must be handled
somewhere else.

"""
from EventLogger import log_write, log_time, severity_string
from traceback import format_exception

# Standard severities
TRACE   = -300
DEBUG   = -200
BLATHER = -100
INFO    =    0
PROBLEM =  100
WARNING =  100
ERROR   =  200
PANIC   =  300


def initialize():
    pass

def set_initializer(func):
    """Set the function used to re-initialize the logs.

    This should be called when initialize_from_environment() is not
    appropiate.

    This does not ensure that the new function gets called; the caller
    should do that separately.
    """
    pass


def LOG(subsystem, severity, summary, detail='', error=None, reraise=None):
    """Log some information

    The required arguments are:

      subsystem -- The subsystem generating the message (e.g. ZODB)

      severity -- The "severity" of the event.  This may be an integer or
                  a floating point number.  Logging back ends may
                  consider the int() of this value to be significant.
                  For example, a backend may consider any severity
                  whos integer value is WARNING to be a warning.

      summary -- A short summary of the event

      detail -- A detailed description

      error -- A three-element tuple consisting of an error type, value, and
               traceback.  If provided, then a summary of the error
               is added to the detail.

      reraise -- If provided with a true value, then the error given by
                 error is reraised.

    """
    log_write(subsystem, severity, summary, detail, error)
    if reraise and error:
        raise error[0], error[1], error[2]

_subsystems = []
def register_subsystem(subsystem):
    """Register a subsystem name

    A logging facility might replace this function to collect information about
    subsystems used in an application.
    """
    _subsystems.append(subsystem)

# Most apps interested in logging only want the names below.
__all__ = ['LOG', 'TRACE', 'DEBUG', 'BLATHER', 'INFO', 'PROBLEM',
           'WARNING', 'ERROR', 'PANIC', 'log_time']
