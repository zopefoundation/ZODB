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
from ZConfig.components.logger.factory import Factory


class EventLogFactory(Factory):
    """
    A wrapper used to create loggers while delaying actual logger
    instance construction.  We need to do this because we may
    want to reference a logger before actually instantiating it (for example,
    to allow the app time to set an effective user).
    An instance of this wrapper is a callable which, when called, returns a
    logger object.
    """
    def __init__(self, section):
        Factory.__init__(self)
        self.level = section.level
        self.handler_factories = section.handlers

    def create(self):
        # set the logger up
        import logging
        logger = logging.getLogger("event")
        logger.handlers = []
        logger.propagate = 0
        logger.setLevel(self.level)
        if self.handler_factories:
            for handler_factory in self.handler_factories:
                handler = handler_factory()
                logger.addHandler(handler)
        else:
            from ZConfig.components.logger.loghandler import NullHandler
            logger.addHandler(NullHandler())
        return logger

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
        EventLogger.event_logger.logger = self()

    def startup(self):
        zLOG.set_initializer(self.initialize)
        zLOG.initialize()
