import string
from types import StringType
from zLOG import *

__all__ = ["zLogger", "format_msg"]

_MAX_MSG_SIZE = 120

def format_msg(*args):
    accum = []
    total_len = 0
    for arg in args:
        if not isinstance(arg, StringType):
            arg = str(arg)
        accum.append(arg)
        total_len = total_len + len(arg)
        if total_len >= _MAX_MSG_SIZE:
            break
    m = string.join(accum)
    if len(m) > _MAX_MSG_SIZE:
        m = m[:_MAX_MSG_SIZE] + ' ...'
    return m

class zLogger:

    def __init__(self, channel):
        self.channel = channel

    def __str__(self):
        raise RuntimeError, "don't print me"

    def trace(self, msg):
        LOG(self.channel, TRACE, msg)

    def debug(self, msg):
        LOG(self.channel, DEBUG, msg)

    def blather(self, msg):
        LOG(self.channel, BLATHER, msg)

    def info(self, msg):
        LOG(self.channel, INFO, msg)

    def problem(self, msg):
        LOG(self.channel, PROBLEM, msg)

    def warning(self, msg):
        LOG(self.channel, WARNING, msg)

    def error(self, msg, error=None):
        LOG(self.channel, ERROR, msg, error=error)

    def panic(self, msg):
        LOG(self.channel, PANIC, msg)
