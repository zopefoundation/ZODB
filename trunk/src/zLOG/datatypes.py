##############################################################################
#
# Copyright (c) 2002 Zope Corporation and Contributors.
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

"""ZConfig datatypes for logging support."""

import zLOG

from zLOG import EventLogger
from ZConfig.components.logger import logger


class EventLogFactory(logger.EventLogFactory):
    """Factory used to create a logger to use with zLOG.

    This adds the getLowestHandlerLevel() method to make it suitable
    for Zope and replaces the startup() method to ensure zLOG is
    properly initialized.
    """

    def getLowestHandlerLevel(self):
        """ Return the lowest log level provided by any of our handlers
        (used by Zope startup logger code to decide what to send
        to stderr during startup) """
        lowest = self.level
        for factory in self.handler_factories:
            handler_level = factory.getLevel()
            if handler_level < lowest:
                lowest = factory.getLevel()
        return lowest

    def initialize(self):
        logger = self()
        for handler in logger.handlers:
            if hasattr(handler, "reopen"):
                handler.reopen()

    def startup(self):
        zLOG.set_initializer(self.initialize)
        zLOG.initialize()
