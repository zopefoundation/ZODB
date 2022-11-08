##############################################################################
#
# Copyright (c) Zope Foundation and Contributors.
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
import doctest
import os
import unittest
from os.path import join

import manuel.capture
import manuel.doctest
import manuel.testing
import zope.testing.module

import ZODB


def setUp(test):
    test.globs.update(
        ZODB=ZODB,
    )
    zope.testing.module.setUp(test)


def tearDown(test):
    zope.testing.module.tearDown(test)


def test_suite():
    base, src = os.path.split(os.path.dirname(os.path.dirname(ZODB.__file__)))
    assert src == 'src', src
    base = join(base, 'docs')
    guide = join(base, 'guide')
    reference = join(base, 'reference')

    return unittest.TestSuite((
        manuel.testing.TestSuite(
            manuel.doctest.Manuel(
                optionflags=doctest.IGNORE_EXCEPTION_DETAIL,
            ) + manuel.capture.Manuel(),
            join(guide, 'writing-persistent-objects.rst'),
            join(guide, 'install-and-run.rst'),
            join(guide, 'transactions-and-threading.rst'),
            join(reference, 'zodb.rst'),
            join(reference, 'storages.rst'),
            setUp=setUp, tearDown=tearDown,
        ),
    ))
