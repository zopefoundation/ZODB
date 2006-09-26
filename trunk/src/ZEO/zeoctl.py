#!/usr/bin/env python2.3
##############################################################################
#
# Copyright (c) 2005 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

"""Wrapper script for zdctl.py that causes it to use the ZEO schema."""

import os

import ZEO
import zdaemon.zdctl

# Main program
def main(args=None):
    options = zdaemon.zdctl.ZDCtlOptions()
    options.schemadir = os.path.dirname(ZEO.__file__)
    options.schemafile = "zeoctl.xml"
    zdaemon.zdctl.main(args, options)

if __name__ == "__main__":
    main()
