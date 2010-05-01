##############################################################################
#
# Copyright (c) 2008 Zope Corporation and Contributors.
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

"""In Python 2.6, the "sha" and "md5" modules have been deprecated
in favor of using hashlib for both. This class allows for compatibility
between versions."""

import sys

try:
    import hashlib
    sha1 = hashlib.sha1
    new = sha1
except ImportError:
    import sha
    sha1 = sha.new
    new = sha1
    digest_size = sha.digest_size
