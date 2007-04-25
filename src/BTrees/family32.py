import sys
import zope.interface
import BTrees.Interfaces

from BTrees import IOBTree as IOModule
from BTrees import OIBTree as OIModule
from BTrees import IFBTree as IFModule
from BTrees import IIBTree as IIModule

maxint = int(2**31-1)
minint = -maxint - 1

zope.interface.moduleProvides(BTrees.Interfaces.IIntegerFamily)
