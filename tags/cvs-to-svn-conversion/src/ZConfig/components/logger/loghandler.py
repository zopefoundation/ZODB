##############################################################################
#
# Copyright (c) 2001 Zope Corporation and Contributors. All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

"""Handlers which can plug into a PEP 282 logger."""

import os.path
import sys

from logging import Handler, StreamHandler
from logging.handlers import SysLogHandler, BufferingHandler
from logging.handlers import HTTPHandler, SMTPHandler
from logging.handlers import NTEventLogHandler as Win32EventLogHandler


class FileHandler(StreamHandler):
    """File handler which supports reopening of logs.

    Re-opening should be used instead of the 'rollover' feature of
    the FileHandler from the standard library's logging package.
    """

    def __init__(self, filename, mode="a"):
        filename = os.path.abspath(filename)
        StreamHandler.__init__(self, open(filename, mode))
        self.baseFilename = filename
        self.mode = mode

    def close(self):
        self.stream.close()

    def reopen(self):
        self.close()
        self.stream = open(self.baseFilename, self.mode)


class NullHandler(Handler):
    """Handler that does nothing."""

    def emit(self, record):
        pass

    def handle(self, record):
        pass


class StartupHandler(BufferingHandler):
    """Handler which stores messages in a buffer until later.

    This is useful at startup before we can know that we can safely
    write to a configuration-specified handler.
    """

    def __init__(self):
        BufferingHandler.__init__(self, sys.maxint)

    def shouldFlush(self, record):
        return False

    def flushBufferTo(self, target):
        while self.buffer:
            target.handle(self.buffer.pop(0))
