##############################################################################
# 
# Zope Public License (ZPL) Version 1.0
# -------------------------------------
# 
# Copyright (c) Digital Creations.  All rights reserved.
# 
# This license has been certified as Open Source(tm).
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
# 
# 1. Redistributions in source code must retain the above copyright
#    notice, this list of conditions, and the following disclaimer.
# 
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions, and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
# 
# 3. Digital Creations requests that attribution be given to Zope
#    in any manner possible. Zope includes a "Powered by Zope"
#    button that is installed by default. While it is not a license
#    violation to remove this button, it is requested that the
#    attribution remain. A significant investment has been put
#    into Zope, and this effort will continue if the Zope community
#    continues to grow. This is one way to assure that growth.
# 
# 4. All advertising materials and documentation mentioning
#    features derived from or use of this software must display
#    the following acknowledgement:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    In the event that the product being advertised includes an
#    intact Zope distribution (with copyright and license included)
#    then this clause is waived.
# 
# 5. Names associated with Zope or Digital Creations must not be used to
#    endorse or promote products derived from this software without
#    prior written permission from Digital Creations.
# 
# 6. Modified redistributions of any form whatsoever must retain
#    the following acknowledgment:
# 
#      "This product includes software developed by Digital Creations
#      for use in the Z Object Publishing Environment
#      (http://www.zope.org/)."
# 
#    Intact (re-)distributions of any official Zope release do not
#    require an external acknowledgement.
# 
# 7. Modifications are encouraged but must be packaged separately as
#    patches to official Zope releases.  Distributions that do not
#    clearly separate the patches from the original work must be clearly
#    labeled as unofficial distributions.  Modifications which do not
#    carry the name Zope may be packaged in any form, as long as they
#    conform to all of the clauses above.
# 
# 
# Disclaimer
# 
#   THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS ``AS IS'' AND ANY
#   EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
#   PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL CREATIONS OR ITS
#   CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
#   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
#   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
#   USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#   ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
#   OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
#   SUCH DAMAGE.
# 
# 
# This software consists of contributions made by Digital Creations and
# many individuals on behalf of Digital Creations.  Specific
# attributions are listed in the accompanying credits file.
# 
##############################################################################

__version__ = '$Id: testBTreesUnicode.py,v 1.4 2001/10/22 20:20:23 andreasjung Exp $'

import unittest,types
from BTrees.OOBTree import OOBTree


# When a OOBtree contains unicode strings as keys,
# it is neccessary accessing non-unicode strings are
# either ascii strings or encoded as unicoded using the
# corresponding encoding


encoding = 'ISO-8859-1'

class TestBTreesUnicode(unittest.TestCase):
    """ test unicode"""

    def setUp(self):
        """ setup an OOBTree with some unicode strings """

        self.s = unicode('dreit\xe4gigen','latin1')

        self.data = [('alien', 284708388), 
                ('k\xf6nnten', 284708389),
                ('fox', 284708387), 
                ('future', 284708388), 
                ('quick', 284708387), 
                ('zerst\xf6rt', 284708389), 
                (unicode('dreit\xe4gigen','latin1'), 284708391)
                ]

        self.tree = OOBTree()
        for k,v in self.data:
            if isinstance(k,types.StringType):
                self.tree[unicode(k,'latin1')]=v
            else:
                self.tree[k]=v



    def test1(self):
        """ check every item of the tree """

        for k, v in self.data:
            if isinstance(k,types.StringType):
                key = unicode(k,encoding)
            else:
                key = k

            if self.tree[key]!=v:
                print "fehler"


    def test2(self):
        """ try to access unicode keys in tree"""

        assert self.data[-1][0]== self.s
        assert self.tree[self.data[-1][0]]== self.data[-1][1]
        assert self.tree[self.s]== self.data[-1][1],''


def test_suite():
    return unittest.makeSuite(TestBTreesUnicode)

def main():
    unittest.TextTestRunner().run(test_suite())

if __name__ == '__main__':
    main()

