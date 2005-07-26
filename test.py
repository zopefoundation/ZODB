#! /usr/bin/env python2.3
##############################################################################
#
# Copyright (c) 2001, 2002 Zope Corporation and Contributors.
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
"""
Test runner for ZODB.

See the docs for zope.testing; test.py is a small driver for
zope.testing.testrunner.
"""

import warnings
import sys
import os
from distutils.util import get_platform

# If ``setup.py build_ext -i`` was used, we want to get code from src/.
# Else (``setup.py build``) we have to look in a funky platform-specific
# subdirectory of build/.  We don't _know_ how it was built, so have to
# guess, and favor the latter (since build/lib.xyz doesn't exist unless
# ``setup.py build`` was done).
PLAT_SPEC = "%s-%s" % (get_platform(), sys.version[0:3])
LIB_DIR = os.path.join("build", "lib.%s" % PLAT_SPEC)
path = "src"
if os.path.isdir(LIB_DIR):
    path = LIB_DIR
print "Running tests from", path

sys.path.append(path)
from zope.testing import testrunner

# Persistence/__init__.py generates a long warning message about the
# the failure of
#     from _Persistence import Persistent
# for the benefit of people expecting that to work from previous (pre 3.3)
# ZODB3 releases.  We don't need to see that msg every time we run the
# test suite, though, and it's positively unhelpful to see it in this
# context.
# NOTE:  "(?s)" enables re.SINGLELINE, so that the ".*" can suck up
#        newlines.
warnings.filterwarnings("ignore",
    message="(?s)Couldn't import the ExtensionClass-based base class.*"
            "There are two possibilities:",
    category=UserWarning)


defaults = [
    "--path", path,
    ]

testrunner.run(defaults)
