##############################################################################
#
# Copyright Zope Foundation and Contributors.
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

def _noop(event):
    """
    Tests:

    >>> import sys, ZODB.event
    >>> notify = ZODB.event
    >>> zopemod = sys.modules.get('zope', False)
    >>> event = sys.modules.get('zope.event', False)
    >>> if event:
    ...    del zopemod.event
    ...    sys.modules['zope.event'] = None
    ...    _ = reload(ZODB.event)

    If zope.event isn't installed, then notify is _noop

    >>> ZODB.event.notify is ZODB.event._noop
    True

    If zope.event is installed, then notify is zope.event.notify:

    >>> zope = sys.modules['zope'] = type(sys)('zope')
    >>> zope.event = sys.modules['zope.event'] = type(sys)('zope.event')
    >>> zope.event.notify = lambda e: None
    >>> _ = reload(ZODB.event)
    >>> ZODB.event.notify is zope.event.notify
    True

    Cleanup:

    >>> if event is False:
    ...     del sys.modules['zope.event']
    ... else:
    ...     if event:
    ...         zopemod.event = event
    ...     sys.modules['zope.event'] = event

    >>> if zopemod is False:
    ...     del sys.modules['zope']
    ... else:
    ...     sys.modules['zope'] = zopemod

    >>> ZODB.event.notify = notify

    """
    pass

try:
    from zope.event import notify
except ImportError:
    notify = _noop

