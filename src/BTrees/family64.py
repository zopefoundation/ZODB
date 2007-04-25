import sys
import zope.interface
import BTrees.Interfaces

from BTrees import LOBTree as IOModule
from BTrees import OLBTree as OIModule
from BTrees import LFBTree as IFModule
from BTrees import LLBTree as IIModule

maxint = 2**63-1
minint = -maxint - 1

zope.interface.moduleProvides(BTrees.Interfaces.IIntegerFamily)
