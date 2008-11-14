##############################################################################
#
# This software is Copyright (c) Zope Corporation (tm) and
# Contributors. All rights reserved.
#
# This software consists of contributions made by Zope
# Corporation and many individuals on behalf of Zope
# Corporation.  Specific attributions are listed in the
# accompanying credits file.
#
##############################################################################
"""In Python 2.6, the "sha" and "md5" modules have been deprecated
in favor of using hashlib for both. This class allows for compatibility
between versions."""

import sys

if sys.version_info[:2] >= (2, 6):
    import hashlib
    sha1 = hashlib.sha1
else:
    import sha
    sha1 = sha.new
