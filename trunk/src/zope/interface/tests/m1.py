"""Test module that declares an interface
"""

from zope.interface import Interface, moduleProvides

class I1(Interface): pass
class I2(Interface): pass

moduleProvides(I1, I2)
