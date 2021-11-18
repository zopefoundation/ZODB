##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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

import persistent.mapping


class fixer(object):
    def __of__(self, parent):
        def __setstate__(state, self=parent):
            self._container = state
            del self.__setstate__
        return __setstate__


fixer = fixer()


class hack(object):
    pass


hack = hack()


def __basicnew__():
    r = persistent.mapping.PersistentMapping()
    r.__setstate__ = fixer
    return r


hack.__basicnew__ = __basicnew__
