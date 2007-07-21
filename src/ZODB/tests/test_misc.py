##############################################################################
#
# Copyright (c) 2006 Zope Corporation and Contributors.
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
"""Misc tests :)
"""

import unittest
from zope.testing import doctest

def conflict_error_retains_data_passed():
    r"""
    
ConflictError can be passed a data record which it claims to retain as
an attribute.

    >>> import ZODB.POSException
    >>> 
    >>> ZODB.POSException.ConflictError(data='cM\nC\n').data
    'cM\nC\n'

    """

def test_suite():
    return unittest.TestSuite((
        doctest.DocTestSuite(),
        ))

