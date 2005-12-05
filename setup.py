#############################################################################
#
# Copyright (c) 2005 Zope Corporation and Contributors.
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

import os
import site
import sys

here = os.path.dirname(os.path.abspath(__file__))
buildsupport = os.path.join(here, "buildsupport")

# Add 'buildsupport' to sys.path and process *.pth files from 'buildsupport':
last = len(sys.path)
site.addsitedir(buildsupport)
if len(sys.path) > last:
    # Move all appended directories to the start.
    # Make sure we use ZConfig shipped with the distribution
    new = sys.path[last:]
    del sys.path[last:]
    sys.path[:0] = new

import zpkgsetup.package
import zpkgsetup.publication
import zpkgsetup.setup

# Note that release.py must be able to recognize the VERSION line.
VERSION = "3.6.0b4"

context = zpkgsetup.setup.SetupContext(
    "ZODB", VERSION, __file__)

context.load_metadata(
    os.path.join(here,
                 zpkgsetup.publication.PUBLICATION_CONF))

context.walk_packages("src")
context.setup()
