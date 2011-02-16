##############################################################################
#
# Copyright (c) 2011 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
import unittest

class TimeStampTests(unittest.TestCase):

    def _getTargetClass(self):
        from persistent.timestamp import TimeStamp
        return TimeStamp

    def _makeOne(self, *args, **kw):
        return self._getTargetClass()(*args, **kw)

    def test_ctor_invalid_arglist(self):
        BAD_ARGS = [(),
                    (1,),
                    (1, 2),
                    (1, 2, 3),
                    (1, 2, 3, 4),
                    (1, 2, 3, 4, 5),
                    ('1', '2', '3', '4', '5', '6'),
                    (1, 2, 3, 4, 5, 6, 7),
                   ]
        for args in BAD_ARGS:
            self.assertRaises((TypeError, ValueError), self._makeOne, *args)

    def test_ctor_from_string(self):
        from persistent.timestamp import _makeOctets
        from persistent.timestamp import _makeUTC
        ZERO = _makeUTC(1900, 1, 1, 0, 0, 0)
        EPOCH = _makeUTC(1970, 1, 1, 0, 0, 0)
        DELTA = ZERO - EPOCH
        DELTA_SECS = DELTA.days * 86400 + DELTA.seconds
        SERIAL = _makeOctets('\x00' * 8)
        ts = self._makeOne(SERIAL)
        self.assertEqual(ts.raw(), SERIAL)
        self.assertEqual(ts.year(), 1900)
        self.assertEqual(ts.month(), 1)
        self.assertEqual(ts.day(), 1)
        self.assertEqual(ts.hour(), 0)
        self.assertEqual(ts.minute(), 0)
        self.assertEqual(ts.second(), 0.0)
        self.assertEqual(ts.timeTime(), DELTA_SECS)

    def test_ctor_from_elements(self):
        from persistent.timestamp import _makeOctets
        from persistent.timestamp import _makeUTC
        ZERO = _makeUTC(1900, 1, 1, 0, 0, 0)
        EPOCH = _makeUTC(1970, 1, 1, 0, 0, 0)
        DELTA = ZERO - EPOCH
        DELTA_SECS = DELTA.days * 86400 + DELTA.seconds
        SERIAL = _makeOctets('\x00' * 8)
        ts = self._makeOne(1900, 1, 1, 0, 0, 0.0)
        self.assertEqual(ts.raw(), SERIAL)
        self.assertEqual(ts.year(), 1900)
        self.assertEqual(ts.month(), 1)
        self.assertEqual(ts.day(), 1)
        self.assertEqual(ts.hour(), 0)
        self.assertEqual(ts.minute(), 0)
        self.assertEqual(ts.second(), 0.0)
        self.assertEqual(ts.timeTime(), DELTA_SECS)

    def test_laterThan_self_is_earlier(self):
        from persistent.timestamp import _makeOctets
        SERIAL1 = _makeOctets('\x01' * 8)
        SERIAL2 = _makeOctets('\x02' * 8)
        ts1 = self._makeOne(SERIAL1)
        ts2 = self._makeOne(SERIAL2)
        later = ts1.laterThan(ts2)
        self.assertEqual(later.raw(), _makeOctets('\x02' * 7 + '\x03'))

    def test_laterThan_self_is_later(self):
        from persistent.timestamp import _makeOctets
        SERIAL1 = _makeOctets('\x01' * 8)
        SERIAL2 = _makeOctets('\x02' * 8)
        ts1 = self._makeOne(SERIAL1)
        ts2 = self._makeOne(SERIAL2)
        later = ts2.laterThan(ts1)
        self.failUnless(later is ts2)
