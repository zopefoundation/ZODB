##############################################################################
#
# Copyright (c) 2002, 2003 Zope Corporation and Contributors.
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

_marker = []

def importer(name):
    components = name.split('.')
    start = components[0]
    g = globals()
    package = __import__(start, g, g)
    modulenames = [start]
    for component in components[1:]:
        modulenames.append(component)
        try:
            package = getattr(package, component)
        except AttributeError:
            name = '.'.join(modulenames)
            package = __import__(name, g, g, component)
    return package

class Factory:
    """
    A generic wrapper for instance construction and function calling used
    to delay construction/call until necessary.  The class path is the dotted
    name to the class or function, args are the positional args, kw are the
    keyword args. If it is specified, 'callback' is a function which will be
    called back after constructing an instance or calling a function.  It must
    take the instance (or the result of the function) as a single argument.
    """
    def __init__(self, class_path, callback, *args, **kw):
        self.class_path = class_path
        self.callback = callback
        self.setArgs(list(args), kw)
        self.resolved = _marker

    def __repr__(self):
        return ('<Factory instance for class "%s" with positional args "%s" '
                'and keword args "%s"' % (self.class_path, self.args, self.kw))

    __str__ = __repr__

    def __call__(self):
        if self.resolved is _marker:
            package = importer(self.class_path)
            inst = package(*self.args, **self.kw)
            if self.callback:
                self.callback(inst)
            self.resolved = inst
        return self.resolved

    def setArgs(self, args, kw):
        self.args = args
        self.kw = kw

    def getArgs(self):
        return (self.args, self.kw)
