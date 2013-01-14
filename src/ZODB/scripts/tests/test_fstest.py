##############################################################################
#
# Copyright (c) 2010 Zope Foundation and Contributors.
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

def test_fstest_verbose():
    r"""
    >>> from ZODB.DB import DB
    >>> db = DB('data.fs')
    >>> db.close()
    >>> from ZODB.scripts.fstest import main
    >>> main(['data.fs'])

    >>> main(['data.fs'])

    >>> main(['-v', 'data.fs'])
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
             4: transaction tid ... #0
    no errors detected

    >>> main(['-vvv', 'data.fs'])
    ... # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
            52: object oid 0x0000000000000000 #0
             4: transaction tid ... #0
    no errors detected

    """


def test_suite():
    import doctest
    from zope.testing import setupstack
    return doctest.DocTestSuite(
        setUp=setupstack.setUpDirectory, tearDown=setupstack.tearDown)

