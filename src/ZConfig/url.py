##############################################################################
#
# Copyright (c) 2002, 2003 Zope Corporation and Contributors.
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
"""urlparse-like helpers that normalize file: URLs.

ZConfig and urllib2 expect file: URLs to consistently use the '//'
hostpart seperator; the functions here enforce this constraint.
"""

import urlparse as _urlparse

try:
    from urlparse import urlsplit
except ImportError:
    def urlsplit(url):
        # Check for the fragment here, since Python 2.1.3 didn't get
        # it right for things like "http://www.python.org#frag".
        if '#' in url:
            url, fragment = url.split('#', 1)
        else:
            fragment = ''
        parts = list(_urlparse.urlparse(url))
        parts[-1] = fragment
        param = parts.pop(3)
        if param:
            parts[2] += ";" + param
        return tuple(parts)


def urlnormalize(url):
    lc = url.lower()
    if lc.startswith("file:/") and not lc.startswith("file:///"):
        url = "file://" + url[5:]
    return url


def urlunsplit(parts):
    parts = list(parts)
    parts.insert(3, '')
    url = _urlparse.urlunparse(tuple(parts))
    if (parts[0] == "file"
        and url.startswith("file:/")
        and not url.startswith("file:///")):
        url = "file://" + url[5:]
    return url


def urldefrag(url):
    url, fragment = _urlparse.urldefrag(url)
    return urlnormalize(url), fragment


def urljoin(base, relurl):
    url = _urlparse.urljoin(base, relurl)
    if url.startswith("file:/") and not url.startswith("file:///"):
        url = "file://" + url[5:]
    return url
