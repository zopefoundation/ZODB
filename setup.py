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

import zpkgsetup.package
import zpkgsetup.publication
import zpkgsetup.setup

# Note that release.py must be able to recognize the VERSION line.
VERSION = "3.5.0a8"


here = os.path.dirname(os.path.abspath(__file__))

context = zpkgsetup.setup.SetupContext(
    "ZODB", VERSION, __file__)

context.load_metadata(
    os.path.join(here,
                 zpkgsetup.publication.PUBLICATION_CONF))

context.walk_packages("src")
context.setup()
