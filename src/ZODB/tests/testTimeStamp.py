"""Test the TimeStamp module."""

import time
import unittest

from ZODB.TimeStamp import TimeStamp

class TestTimeStamp(unittest.TestCase):

    def checkTimeStamp(self):
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
    return unittest.makeSuite(TestTimeStamp, 'check')
            
if __name__ == "__main__":
    loader = unittest.TestLoader()
    loader.testMethodPrefix = "check"
    unittest.main(testLoader=loader)
