##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
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
"""ZConfig factory datatypes for loggers."""

from ZConfig.components.logger.factory import Factory


class LoggerFactoryBase(Factory):
    """Base class for logger factories.

    Factory used to create loggers while delaying actual logger
    instance construction.  We need to do this because we may want to
    reference a logger before actually instantiating it (for example,
    to allow the app time to set an effective user).  An instance of
    this wrapper is a callable which, when called, returns a logger
    object.
    """

    def __init__(self, section):
        Factory.__init__(self)
        self.level = section.level
        self.handler_factories = section.handlers

    def create(self):
        # set the logger up
        import logging
        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)
        if self.handler_factories:
            for handler_factory in self.handler_factories:
                handler = handler_factory()
                logger.addHandler(handler)
        else:
            from ZConfig.components.logger import loghandler
            logger.addHandler(loghandler.NullHandler())
        return logger

    def startup(self):
        # make sure we've instantiated the logger
        self()

    def getLowestHandlerLevel(self):
        """Return the lowest log level provided by any configured handler.

        If all handlers and the logger itself have level==NOTSET, this
        returns NOTSET.
        """
        import logging
        lowest = self.level
        for factory in self.handler_factories:
            level = factory.getLevel()
            if level != logging.NOTSET:
                if lowest == logging.NOTSET:
                    lowest = level
                else:
                    lowest = min(lowest, level)
        return lowest

    def reopen(self):
        """Re-open any handlers for which this is a meaningful operation.

        This only works on handlers on the logger provided by this
        factory directly; handlers for child loggers are not affected.
        (This can be considered a bug, but is sufficient at the
        moment.)
        """
        logger = self()
        for handler in logger.handlers:
            reopen = getattr(handler, "reopen", None)
            if reopen is not None and callable(reopen):
                reopen()


class EventLogFactory(LoggerFactoryBase):
    """Logger factory that returns the root logger."""

    name = None


class LoggerFactory(LoggerFactoryBase):
    """Logger factory that returns the named logger."""

    def __init__(self, section):
        LoggerFactoryBase.__init__(self, section)
        self.name = section.name
        self.propagate = section.propagate

    def create(self):
        logger = LoggerFactoryBase.create(self)
        logger.propagate = self.propagate
        return logger
