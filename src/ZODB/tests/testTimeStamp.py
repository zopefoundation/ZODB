"""Test the TimeStamp utility type"""

import time
import unittest

from ZODB.TimeStamp import TimeStamp

EPSILON = 0.000001

class TimeStampTests(unittest.TestCase):

    def checkYMDTimeStamp(self):
        self._check_ymd(2001, 6, 3)

    def _check_ymd(self, yr, mo, dy):
        ts = TimeStamp(yr, mo, dy)
        self.assertEqual(ts.year(), yr)
        self.assertEqual(ts.month(), mo)
        self.assertEqual(ts.day(), dy)

        self.assertEquals(ts.hour(), 0)
        self.assertEquals(ts.minute(), 0)
        self.assertEquals(ts.second(), 0)

        t = time.gmtime(ts.timeTime())
        self.assertEquals(yr, t[0])
        self.assertEquals(mo, t[1])
        self.assertEquals(dy, t[2])

    def checkFullTimeStamp(self):
        t = time.gmtime(time.time())
        ts = TimeStamp(*t[:6])

        # XXX floating point comparison
        self.assertEquals(ts.timeTime() + time.timezone, time.mktime(t))

        self.assertEqual(ts.year(), t[0])
        self.assertEqual(ts.month(), t[1])
        self.assertEqual(ts.day(), t[2])

        self.assertEquals(ts.hour(), t[3])
        self.assertEquals(ts.minute(), t[4])
        self.assert_(abs(ts.second() - t[5]) < EPSILON)

    def checkRawTimestamp(self):
        t = time.gmtime(time.time())
        ts1 = TimeStamp(*t[:6])
        ts2 = TimeStamp(`ts1`)

        self.assertEquals(ts1, ts2)
        self.assertEquals(ts1.timeTime(), ts2.timeTime())
        self.assertEqual(ts1.year(), ts2.year())
        self.assertEqual(ts1.month(), ts2.month())
        self.assertEqual(ts1.day(), ts2.day())
        self.assertEquals(ts1.hour(), ts2.hour())
        self.assertEquals(ts1.minute(), ts2.minute())
        self.assert_(abs(ts1.second() - ts2.second()) < EPSILON)

    def checkDictKey(self):
        t = time.gmtime(time.time())
        ts1 = TimeStamp(*t[:6])
        ts2 = TimeStamp(2000, *t[1:6])

        d = {}
        d[ts1] = 1
        d[ts2] = 2

        self.assertEquals(len(d), 2)

    def checkCompare(self):
        ts1 = TimeStamp(1972, 6, 27)
        ts2 = TimeStamp(1971, 12, 12)
        self.assert_(ts1 > ts2)
        self.assert_(ts2 <= ts1)

    def checkLaterThan(self):
        # XXX what does laterThan() do?
        t = time.gmtime(time.time())
        ts = TimeStamp(*t[:6])
        ts2 = ts.laterThan(ts)
        self.assert_(ts2 > ts)

    # XXX should test for bogus inputs to TimeStamp constructor

    def checkTimeStamp(self):
        # Alternate test suite
        t = TimeStamp(2002, 1, 23, 10, 48, 5) # GMT
        self.assertEquals(str(t), '2002-01-23 10:48:05.000000')
        self.assertEquals(repr(t), '\x03B9H\x15UUU')
        self.assertEquals(TimeStamp('\x03B9H\x15UUU'), t)
        self.assertEquals(t.year(), 2002)
        self.assertEquals(t.month(), 1)
        self.assertEquals(t.day(), 23)
        self.assertEquals(t.hour(), 10)
        self.assertEquals(t.minute(), 48)
        self.assertEquals(round(t.second()), 5)
        self.assertEquals(t.second(), t.seconds()) # Alias
        self.assertEquals(t.timeTime(), 1011782885)
        t1 = TimeStamp(2002, 1, 23, 10, 48, 10)
        self.assertEquals(str(t1), '2002-01-23 10:48:10.000000')
        self.assert_(t == t)
        self.assert_(t != t1)
        self.assert_(t < t1)
        self.assert_(t <= t1)
        self.assert_(t1 >= t)
        self.assert_(t1 > t)
        self.failIf(t == t1)
        self.failIf(t != t)
        self.failIf(t > t1)
        self.failIf(t >= t1)
        self.failIf(t1 < t)
        self.failIf(t1 <= t)
        self.assertEquals(cmp(t, t), 0)
        self.assertEquals(cmp(t, t1), -1)
        self.assertEquals(cmp(t1, t), 1)
        self.assertEquals(t1.laterThan(t), t1)
        self.assert_(t.laterThan(t1) > t1)
        self.assertEquals(TimeStamp(2002,1,23), TimeStamp(2002,1,23,0,0,0))

def test_suite():
    return unittest.makeSuite(TimeStampTests, 'check')
